"""Helper functions for getting data from CloudVault and inserting customer data into demeter."""
import logging
from datetime import datetime, timedelta
from os import getenv
from typing import Tuple

from geopandas import read_file
from gql.client import Client
from gql.dsl import DSLSchema
from gql_utils.api import (
    get_client,
    get_files_for_feature_set,
    get_fs_by_survey_df,
    get_images_by_survey_df,
    get_survey_by_field_df,
    graphql_token,
)
from pandas import DataFrame, Timedelta, Timestamp
from pandas import concat as pd_concat
from pandas import to_datetime
from pytz import UTC
from pytz import timezone as pytz_timezone
from shapely.geometry import Polygon
from timezonefinder import TimezoneFinder

from demeter_utils.data_ingest.constants import IMAGE_DATE_HACK, TIMEDELTA_HACK

tzf = TimezoneFinder()


def get_unix_from_datetime(dt: datetime) -> int:
    """Convert dt `datetime` to UNIX timestamp following pandas recommended method.

    See https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#from-timestamps-to-epoch.
    """
    date = to_datetime(dt)
    start_epoch = Timestamp("1970-01-01")
    unix = (date - start_epoch) // Timedelta("1s")
    return unix


def get_cv_connection(env: str = "prod") -> Tuple[Client, DSLSchema]:
    """Create a session with CloudVault's API using credentials in .env file.

    Args:
        env (str): CloudVault enviroment to connect to; can be "prod" or "staging"
    """
    # load environment variables
    email = getenv("SENTERA_EMAIL")
    if not email:
        raise Exception(
            "You must provide your CloudVault email as environment variable 'SENTERA_EMAIL'"
        )
    password = getenv(f"SENTERA_{env.upper()}_PW")
    if not password:
        raise Exception(
            "You must provide your CloudVault password as environment variable 'SENTERA_<ENV>_PW'"
        )

    sentera_api_url = getenv(f"SENTERA_API_{env.upper()}_URL")
    if not sentera_api_url:
        raise Exception(
            "You must provide the API url as environment variable 'SENTERA_API_<ENV>_URL'"
        )

    token = graphql_token(
        email,
        password,
        url_auth=f"{sentera_api_url}/v1/sessions",
    )
    client, ds = get_client(token, sentera_api_url)

    return client, ds


def _get_surveys_after_date(
    client: Client, ds: DSLSchema, asset_sentera_id: str, date_on_or_after: datetime
) -> DataFrame:
    """Get all CloudVault surveys available for a given asset after a given planting date."""
    # get all surveys for this asset after `date_on_or_after`
    df_survey = get_survey_by_field_df(client, ds, asset_sentera_id).rename(
        columns={"sentera_id": "survey_sentera_id", "name": "survey"}
    )
    df_survey["survey"] = df_survey["survey"].map(
        lambda d: datetime.strptime(d, "%m-%d-%Y")
    )
    df_survey = df_survey.loc[df_survey["survey"] > date_on_or_after]

    # get earliest plot ratings and pull plot boundaries from GeoJSON
    df_survey.sort_values("survey", inplace=True)

    return df_survey


def _maybe_find_survey_analytic_files(
    client: Client,
    ds: DSLSchema,
    survey_sentera_id: str,
    analytic_name: str = None,
    file_type: str = "geo_json",
) -> DataFrame:
    """
    Query for `analytic_name` FeatureSet in a CloudVault survey, returning the file info.

    Args:
        analytic_name (str, Optional): Filter on a particular analytic name (e.g., "NDVI Plot Ratings", "Plot
            Multispectral Indices and Uniformity", etc.). If None, all available analytics are returned.

        file_type (str, Optional): Filter on a particular file time (e.g., "geo_json", "document", etc.). Refer to
            GraphQL API for valid file types. If None, all available files are returned.
    """
    # get all files for survey
    df_fs = get_fs_by_survey_df(client, ds, survey_sentera_id)
    if len(df_fs) == 0:
        return None
    if analytic_name is not None:
        df_fs = df_fs.loc[df_fs["name"] == analytic_name]

    df_files = None
    for _, row in df_fs.iterrows():
        df_temp = get_files_for_feature_set(client, ds, row["sentera_id"])
        if len(df_temp) == 0:
            logging.info("No files available for feature set %s", row["sentera_id"])
            continue
        df_temp.insert(0, "survey_sentera_id", survey_sentera_id)
        df_temp.insert(1, "fs_sentera_id", row["sentera_id"])
        df_temp.insert(2, "analytic_name", row["name"])
        df_files = (
            pd_concat([df_files, df_temp], axis=0, ignore_index=True)
            if df_files is not None
            else df_temp.copy()
        )
    df_files = df_files.loc[df_files["file_type"] == file_type]

    if len(df_files) == 0:
        logging.warning(
            "No %sfiles available for survey %s",
            str(file_type + " " or ""),
            survey_sentera_id,
        )
    else:
        logging.info(
            "%s %sfiles available for survey %s",
            len(df_files),
            str(file_type + " " or ""),
            survey_sentera_id,
        )
    return df_files


def get_asset_analytic_info(
    client: Client,
    ds: DSLSchema,
    asset_sentera_id: str,
    date_on_or_after: datetime,
    analytic_name: str = None,
    file_type: str = "geo_json",
) -> DataFrame:
    """Get survey and analytic information for all analytics under a given Sentera `asset`.

    Considering only those surveys that were created after `date_on_or_after`, this function identifies
    the first available plot ratings file after planting and extracts the plot geometries.

    Args:
        client, ds: Connections to CloudVault as set up by `get_cv_connection()`
        asset_sentera_id (str): Sentera ID of the asset to query.
        date_on_or_after (datetime): Filter surveys/analytics so only those captured on or after this date are returned.

        analytic_name (str, Optional): Filter on a particular analytic name (e.g., "NDVI Plot Ratings", "Plot
            Multispectral Indices and Uniformity", etc.). If None, all available analytics are returned.

    Returns:
        `gdf` (GeoDataFrame) contains "sentera_id", "range", "column", and geometry of plots from file
        `file_metadata` (dict) dictionary containing relevant metadata for source file
    """
    df_survey = _get_surveys_after_date(
        client, ds, asset_sentera_id=asset_sentera_id, date_on_or_after=date_on_or_after
    )

    # get earliest plot ratings and pull plot boundaries from GeoJSON
    df_analytic_list = None
    for _, row in df_survey.iterrows():
        survey_sentera_id = row["survey_sentera_id"]
        df_files = _maybe_find_survey_analytic_files(
            client,
            ds,
            survey_sentera_id=survey_sentera_id,
            analytic_name=analytic_name,
            file_type=file_type,
        )
        if df_files is not None:
            df_files.insert(0, "date", row["survey"].strftime("%Y-%m-%d"))
            df_analytic_list = (
                pd_concat([df_analytic_list, df_files], axis=0, ignore_index=True)
                if df_analytic_list is not None
                else df_files.copy()
            )

    if len(df_analytic_list) == 0:
        logging.warning(
            "No %sfiles available for survey %s",
            str(file_type + " " or ""),
            survey_sentera_id,
        )
    else:
        logging.info(
            "%s %sfiles available for survey %s",
            len(df_analytic_list),
            str(file_type + " " or ""),
            survey_sentera_id,
        )
    return df_analytic_list


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


def _get_image_date_for_survey(
    client: Client, ds: DSLSchema, survey_sentera_id: str
) -> datetime:
    """Get midpoint timestamp of all "captured_at" attributes for images in a given survey."""
    df_images = get_images_by_survey_df(client, ds, survey_sentera_id=survey_sentera_id)

    if len(df_images) == 0:
        if survey_sentera_id not in IMAGE_DATE_HACK.keys():
            print(survey_sentera_id)
            print(df_images)
            raise AssertionError
        else:
            return IMAGE_DATE_HACK[survey_sentera_id]
    else:
        min_date = datetime.strptime(
            df_images["captured_at"].min(), "%Y-%m-%dT%H:%M:%SZ"
        )
        max_date = datetime.strptime(
            df_images["captured_at"].max(), "%Y-%m-%dT%H:%M:%SZ"
        )
        date_range = max_date - min_date

        assert date_range <= timedelta(
            days=1
        ), f"Image dates in this collection {survey_sentera_id} differ by more than one day."

        return (min_date + date_range / 2) + TIMEDELTA_HACK


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


# def get_mosaic_for_survey(
#     client: Client, ds: DSLSchema, survey_sentera_id: str, mosaic_name: str
# ) -> Union[None, DataFrame]:
#     df_mosaic = get_mosaic_by_survey_df(client, ds, survey_sentera_id)
#     if len(df_mosaic) == 0:
#         return None

#     df_mosaic = df_mosaic.loc[df_mosaic["name"] == mosaic_name]
#     if len(df_mosaic) == 0:
#         return None

#     df_final = None

#     for mosaic_sentera_id in df_mosaic["sentera_id"].to_list():
#         df_temp = get_files_for_mosaic(client, ds, mosaic_sentera_id)
#         df_temp.insert(0, "fs_sentera_id", mosaic_sentera_id)
#         df_temp.insert(0, "survey_sentera_id", survey_sentera_id)

#         if df_final is None:
#             df_final = df_temp.copy()
#         else:
#             df_final = pd_concat([df_final, df_temp], axis=0)

#     return df_final
