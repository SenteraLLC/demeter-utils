from datetime import datetime

from geopandas import read_file
from gql.client import Client
from gql.dsl import DSLSchema
from pandas import DataFrame
from pandas import concat as pd_concat
from pytz import UTC
from pytz import timezone as pytz_timezone
from shapely import Polygon
from timezonefinder import TimezoneFinder

from demeter_utils.data_ingest._gql import (
    _get_image_date_for_survey,
    _get_surveys_after_date,
    _maybe_find_survey_analytic_files,
)

tzf = TimezoneFinder()


def _load_and_format_ndvi_plot_ratings_from_url(url: str) -> DataFrame:
    """Load and format "NDVI Plot Ratings" GeoJSON file from CloudVault download URL."""
    gdf = read_file(url)

    # ensure consistent column names
    col_rename = {"SenteraID": "sentera_id"}
    gdf.rename(columns=col_rename, inplace=True)

    if "sentera_id" not in gdf.columns.values:
        gdf["sentera_id"] = [-999] * len(gdf)  # these will be fixed later

    # ensure consistent dtypes
    for col in ["range", "column", "sentera_id"]:
        gdf[col] = gdf[col].astype(int)

    cols_ndvi = [col for col in gdf.columns if "NDVI" in col]
    assert len(cols_ndvi) == 1, f"More than one NDVI column present: {cols_ndvi}"
    col_ndvi = cols_ndvi[0]

    df_temp = gdf[["sentera_id", "range", "column", col_ndvi]].rename(
        columns={col_ndvi: "ndvi_mean"}
    )

    df_temp = df_temp.loc[df_temp["ndvi_mean"].notna()]

    return df_temp.reset_index(drop=True)


def get_ndvi_plot_ratings_for_asset(
    client: Client,
    ds: DSLSchema,
    asset_sentera_id: str,
    date_on_or_after: datetime,
) -> DataFrame:
    """Get all plot-level mean NDVI values from CloudVault for a given `asset_sentera_id`.

    Parses through all surveys dated after `date_on_or_after` and, for all available "NDVI Plot
    Ratings" feature sets, loads the data and adds to `df_ndvi`.

    Args:
        client, ds: Connections to CloudVault as set up by `get_cv_connection()`
        asset_sentera_id (str): Sentera ID of the asset
        date_on_or_after (datetime): Earliest planting date for that asset

    Returns:
        `df_ndvi` (DataFrame) contains "date_observed", "sentera_id", "range", "column",
            and plot-level mean NDVI ("ndvi_mean").
    """
    # get all surveys for this field after `date_on_or_after`
    df_survey = _get_surveys_after_date(
        client, ds, asset_sentera_id=asset_sentera_id, date_on_or_after=date_on_or_after
    )

    # get information for all NDVI plot ratings GeoJSON files
    df_files = None
    for _, row in df_survey.iterrows():
        survey_sentera_id = row["survey_sentera_id"]
        df_temp = _maybe_find_survey_analytic_files(
            client,
            ds,
            survey_sentera_id=survey_sentera_id,
            analytic_name="NDVI Plot Ratings",
        )
        if df_temp is not None:
            datetime_flight = _get_image_date_for_survey(
                client, ds, survey_sentera_id=survey_sentera_id
            )
            df_temp.insert(0, "survey", row["survey"].strftime("%m/%d/%Y"))
            df_temp.insert(0, "datetime_flight", datetime_flight)

            if df_files is None:
                df_files = df_temp.copy()
            else:
                df_files = pd_concat([df_files, df_temp], axis=0, ignore_index=True)

    # load all of the NDVI plot ratings files and extract values
    df_ndvi = None
    for _, file_info in df_files.iterrows():
        df_temp = _load_and_format_ndvi_plot_ratings_from_url(file_info["url"])
        df_temp.insert(0, "date_observed", file_info["datetime_flight"])

        if df_ndvi is None:
            df_ndvi = df_temp.copy()
        else:
            df_ndvi = pd_concat([df_ndvi, df_temp], axis=0, ignore_index=True)

    return df_ndvi


def get_date_planted_for_plot(
    site: str, sentera_id: int, geom: Polygon, df: DataFrame
) -> datetime:
    """Find plot-level planting date from `df`, localize datetime, and convert to UTC.

    Args:
        site (str): Name of Phase 1 site
        sentera_id (int): Assigned Sentera ID to plot (CSV and GeoJSON)
        geom (Polygon): Plot boundary geometry
        df (DataFrame): Dataframe from `collect_data()`
    """
    stmt = f"site == '{site}' & sentera_id == {sentera_id}"
    res = df.query(stmt)["date_planted"].item()

    point = geom.centroid

    # set planting to 12 noon local time
    tz_str = tzf.timezone_at(lat=point.y, lng=point.x)
    tz_local = pytz_timezone(tz_str)
    date_planted_local = datetime.strptime(res, "%m/%d/%Y").replace(
        hour=12, tzinfo=tz_local
    )

    # change to UTC
    date_planted_utc = date_planted_local.astimezone(UTC)

    return date_planted_utc
