import logging
from datetime import datetime
from typing import Tuple
from urllib.error import URLError

from geopandas import GeoDataFrame, read_file
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import melt
from retrying import retry

from demeter_utils.data_ingest._utils import get_asset_analytic_info, get_cv_connection


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


# def load_trial_info() -> DataFrame:
#     """Load and organize treatment information.

#     Gathered data and data sources:
#     1. Field trial information for each site/location (CSV)

#     Returns:
#         df_exp_design (DataFrame): Trial information for all sites, including `plot_id` and `seed_id`. Does not include
#             plot geometry.
#     """
#     logging.info("   Collecting and cleaning field notes data from CSV files")

#     demeter_dir = str(getenv("DEMETER_DIR"))
#     data_dir = join(demeter_dir, "projects/mosaic_co_stats/data")

#     df_exp_design = None
#     for asset_name in ASSET_SENTERA_ID.keys():
#         # Load plot data
#         fname_seed_id = join(data_dir, f"seed_id - {asset_name}.csv")
#         df_temp = read_csv(fname_seed_id).rename(columns={"ms": "plot_id"})

#         # clean up and standardize
#         df_temp.insert(0, "site_name", asset_name)
#         df_exp_design = (
#             pd_concat([df_exp_design, df_temp], axis=0, ignore_index=True)
#             if df_exp_design is not None
#             else df_temp.copy()
#         )
#     return df_exp_design


def load_field_insights_data(
    asset_sentera_id_dict: dict,
    date_on_or_after: datetime = datetime(2023, 5, 1),
    analytic_name: str = "Plot Multispectral Indices and Uniformity",
    primary_keys: list[str] = ["site_name", "plot_id"],
    cols_ignore: list[str] = [
        "cust_id",
        "treatment",
        "split_idx",
        "split_idx_",
        "split_idx_1",
        "split_idx_2",
        "trial_id",
        "range",
        "row",
        "num_rows",
        "stroke",
        "stroke-opa",
        "fill",
        "fill-opaci",
        "geometry",
    ],
    col_starts_with_allowable: list[str] = ["Band", "Index", "Canopy Cover"],
) -> Tuple[GeoDataFrame, dict]:
    """
    Loads FieldInsights-formatted data for all sites, surveys, and plots, converting from wide to long format.

    Args:
        asset_sentera_id_dict (dict): Sentera asset names and corresponding sentera_ids.
        date_on_or_after (datetime): Earliest date to load data from; defaults to May 1, 2023.

        analytic_name (str): Name of analytic to load from CloudVault; could be `None` (?), in which case all available
            geojson analytics will be loaded. Defaults to "Plot Multispectral Indices and Uniformity".

        primary_keys (list[str]): List of column names to use as primary keys for merging dataframes. Defaults to
            ["site_name", "plot_id"].

        cols_ignore (list[str]): List of column names to ignore when converting from wide to long.

        col_starts_with_allowable (list[str]): Column names that start with these strings will be included in the
            `value_vars` list of the `pandas.melt` function.

    From CloudVault, all available "Plot Multispectral Indices and Uniformity" GeoJSONs for an asset are used.

    Returns:
        gdf_plots (GeoDataFrame): Plot boundaries and metadata for all sites, including plot_id, site_name, row, range,
            and geometry.

        df_long (DataFrame): long-format dataframe containing plot-level FieldInsights data for all sites, plots, and
            observations.
    """
    logging.info('   Checking for valid "%s" layers from CloudVault', analytic_name)
    client, ds = get_cv_connection()

    df_analytic_list = DataFrame()
    for asset_name in asset_sentera_id_dict.keys():
        df_analytic_asset = get_asset_analytic_info(
            client,
            ds,
            asset_sentera_id=asset_sentera_id_dict[asset_name],
            date_on_or_after=date_on_or_after,
            analytic_name=analytic_name,
            file_type="geo_json",
        )

        if df_analytic_asset is not None:
            df_analytic_asset.insert(0, "site_name", asset_name)
            df_analytic_list = (
                pd_concat(
                    [df_analytic_list, df_analytic_asset], axis=0, ignore_index=True
                )
                if len(df_analytic_list.columns) != 0
                else df_analytic_asset.copy()
            )
    df_analytic_list.drop_duplicates(inplace=True)
    logging.info(
        "   Found %s analytics from %s surveys",
        len(df_analytic_list),
        len(df_analytic_list["survey_sentera_id"].unique()),
    )

    @retry(retry_on_exception=URLError, stop_max_attempt_number=3, wait_fixed=2)
    def _read_file_retry(url: str) -> GeoDataFrame:
        return read_file(url)

    # For wide to long conversion
    gdf_plots = GeoDataFrame()  # Load field boundary data
    df_long = DataFrame()
    # Wide to long format for all sites, surveys, and plots
    for i, row in df_analytic_list.iterrows():
        logging.info(
            '   %s: Loading data from CloudVault: %s "%s"',
            i,
            row["date"].date(),
            row["site_name"],
        )
        gdf_temp = _read_file_retry(row["url"])

        # TODO: Is "site_name" always present?
        gdf_temp.insert(0, "site_name", row["site_name"])
        gdf_plots_temp = gdf_temp[["site_name", "plot_id", "geometry"]].copy()

        # Filters columns; only those that start with `cols_starts_with_allowable` and are not `primary_keys` are kept.
        value_subset = list(
            filter(
                lambda x: (
                    any([x.startswith(s) for s in col_starts_with_allowable])
                    and x not in primary_keys + cols_ignore
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

        gdf_plots = pd_concat([gdf_plots, gdf_plots_temp], axis=0, ignore_index=True)
        df_long = pd_concat([df_long, df_long_temp], axis=0, ignore_index=True)
    gdf_plots.drop_duplicates(subset=primary_keys, inplace=True)
    return gdf_plots, df_long
