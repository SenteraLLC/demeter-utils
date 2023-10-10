import logging
from datetime import datetime
from typing import Tuple
from urllib.error import URLError

from geopandas import GeoDataFrame, read_file
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import melt
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from demeter_utils.data_ingest.cloudvault._connect import get_cv_connection
from demeter_utils.data_ingest.cloudvault._gql import get_asset_analytics


def _parse_stat_name(x: str) -> str:
    """Parse statistic name from variable name string and return statistic name."""
    stat_list = ["Mean", "Median", "Mode", "Std. Dev.", "Min", "Max", "Range"]
    for stat in stat_list:
        if stat.casefold() in x.casefold():
            return stat
    return "Mean"  # https://catalog.sentera.com/products/plot_crop_health_multispectral_uniformity doesn't specify "Mean"


def _wide_to_long(
    df: DataFrame, primary_keys: list, value_subset: list, crop_season_year: int
) -> DataFrame:
    """Convert wide-format dataframe to long-format dataframe, parsing identifier columns."""
    df_long = melt(
        df,
        id_vars=primary_keys,
        value_vars=value_subset,
    )
    idx_initial_insert = 3  # column index of first column insertion
    df_long.insert(
        idx_initial_insert,
        "date",
        df_long["variable"].apply(
            lambda x: datetime.strptime(
                x.split(" ")[-1] + f"-{crop_season_year}", "%d-%b-%Y"
            ).date()
        ),
    )
    df_long.insert(
        idx_initial_insert + 1,
        "observation_type",
        df_long["variable"].apply(lambda x: " ".join(x.split(" ")[:2])),
    )
    df_long.insert(
        idx_initial_insert + 2,
        "statistic_type",
        df_long["variable"].apply(lambda x: _parse_stat_name(x)),
    )
    df_long.insert(
        idx_initial_insert + 3,
        "subplot",
        df_long["variable"].apply(lambda x: True if "subplot" in x.lower() else False),
    )
    return df_long


def load_field_insights_data(
    asset_name: str,
    asset_sentera_id: str,
    date_on_or_after: datetime = datetime(2023, 5, 1),
    analytic_name: str = "Plot Multispectral Indices and Uniformity",
    primary_keys: list[str] = ["site_name", "plot_id"],
    cols_ignore: list[str] = [
        "num_rows",
        "stroke",
        "stroke-opacity",
        "fill",
        "fill-opacity",
    ],
    col_starts_with_allowable: list[str] = ["Band", "Index", "Canopy Cover"],
) -> Tuple[GeoDataFrame, dict]:
    """
    Loads FieldInsights-formatted data for all sites, surveys, and plots, converting from wide to long format.

    Args:
        asset_name (str): Sentera asset name.
        asset_sentera_id (str): Sentera asset id to query.
        date_on_or_after (datetime): Earliest date to load data from; defaults to May 1, 2023.

        analytic_name (str): Name of analytic to load from CloudVault; could be `None` (?), in which case all available
            geojson analytics will be loaded. Defaults to "Plot Multispectral Indices and Uniformity".

        primary_keys (list[str]): List of column names to use as primary keys for merging dataframes. Defaults to
            ["site_name", "plot_id"].

        cols_ignore (list[str]): List of column names to ignore when converting from wide to long. This argument is
            necessary as long as column names vary across files of the same Field Insights analtyics/deliverables. For
            example, the "Plot Multispectral Indices and Uniformity" analytic generated for one date or location of an
            experment may includee a column (e.g., "id") whereas another date or location might be missing that column.
            Without explicitly ignoring inconsistencies in column names at this step, the inner join results in a sparse
            `gdf_plots` DataFrame.

        col_starts_with_allowable (list[str]): Column names that start with these strings will be included in the
            `value_vars` list of the `pandas.melt` function.

    From CloudVault, all available "Plot Multispectral Indices and Uniformity" GeoJSONs for an asset are used.

    Returns:
        gdf_plots (GeoDataFrame): Plot boundaries and metadata for all sites, including plot_id, site_name, row, range,
            and geometry.

        df_long (DataFrame): long-format dataframe containing plot-level FieldInsights data for all sites, plots, and
            observations.
    """
    logging.info(
        'Checking asset "%s" for valid "%s" layers from CloudVault; sentera_id: %s',
        asset_name,
        analytic_name,
        asset_sentera_id,
    )
    client, ds = get_cv_connection()

    df_asset_analytics = get_asset_analytics(
        client,
        ds,
        asset_sentera_id=asset_sentera_id,
        date_on_or_after=date_on_or_after,
        analytic_name=analytic_name,
        file_type="geo_json",
    )

    if df_asset_analytics is None:
        return GeoDataFrame(), DataFrame()

    df_asset_analytics.insert(0, "site_name", asset_name)

    df_asset_analytics.drop_duplicates(inplace=True)

    @retry(
        retry=retry_if_exception_type(URLError),
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
    )
    def _read_file_retry(url: str) -> GeoDataFrame:
        return read_file(url)

    # For wide to long conversion
    gdf_plots = GeoDataFrame()  # Load field boundary data
    df_long = DataFrame()
    # Wide to long format for all sites, surveys, and plots
    for i, row in df_asset_analytics.iterrows():
        logging.info(
            '  %s: Loading data from CloudVault. site_id: "%s" date: %s',
            i,
            row["site_name"],
            row["date"].date(),
        )
        try:
            gdf_temp = _read_file_retry(row["url"])
        except URLError:
            logging.warning(
                '  %s: Unable to load data from CloudVault. site_id: "%s" date: %s',
                i,
                row["site_name"],
                row["date"].date(),
            )
            continue
        # TODO: Is "site_name" always present?
        gdf_temp.insert(0, "site_name", row["site_name"])

        # Filters columns; any columns that do not start with `col_starts_with_allowable` are kept.
        plots_subset = list(
            filter(
                lambda x: (
                    not any([x.startswith(s) for s in col_starts_with_allowable])
                    and x not in primary_keys + cols_ignore
                ),
                list(gdf_temp.columns),
            )
        )

        # Filters columns; only those that start with `col_starts_with_allowable` and are not `primary_keys` are kept.
        value_subset = list(
            filter(
                lambda x: (
                    any([x.startswith(s) for s in col_starts_with_allowable])
                    and x not in primary_keys + cols_ignore + [gdf_temp.geometry.name]
                ),
                list(gdf_temp.columns),
            )
        )

        df_long_temp = _wide_to_long(
            gdf_temp[primary_keys + value_subset],
            primary_keys,
            value_subset,
            crop_season_year=row["date"].year,
        )

        gdf_plots = pd_concat(
            [gdf_plots, gdf_temp[primary_keys + plots_subset]],
            axis=0,
            ignore_index=True,
        )
        df_long = pd_concat([df_long, df_long_temp], axis=0, ignore_index=True)
    gdf_plots.drop_duplicates(subset=primary_keys, inplace=True)
    return gdf_plots, df_long
