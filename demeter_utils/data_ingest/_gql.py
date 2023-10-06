"""Helper functions for getting data from CloudVault and inserting customer data into demeter."""
import logging
from datetime import datetime, timedelta

from gql.client import Client
from gql.dsl import DSLSchema
from gql_utils.api import (
    get_files_for_feature_set,
    get_fs_by_survey_df,
    get_images_by_survey_df,
    get_survey_by_field_df,
)
from pandas import DataFrame
from pandas import concat as pd_concat


def _get_surveys_after_date(
    client: Client, ds: DSLSchema, asset_sentera_id: str, date_on_or_after: datetime
) -> DataFrame:
    """Get all CloudVault surveys available for a given asset after a given planting date."""
    # get all surveys for this asset after `date_on_or_after`
    df_survey = get_survey_by_field_df(client, ds, asset_sentera_id).rename(
        columns={"sentera_id": "survey_sentera_id", "name": "survey"}
    )
    df_survey["date"] = df_survey["survey"].map(
        lambda d: datetime.strptime(d, "%m-%d-%Y")
    )
    df_survey = df_survey.loc[df_survey["date"] >= date_on_or_after]
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
        return DataFrame()
    if analytic_name is not None:
        df_fs = df_fs.loc[df_fs["name"] == analytic_name]

    df_files = DataFrame()
    for _, row in df_fs.iterrows():
        df_temp = get_files_for_feature_set(client, ds, row["sentera_id"])
        if len(df_temp) == 0:
            logging.info("No files available for feature set %s", row["sentera_id"])
            continue
        df_temp.insert(0, "analytic_name", row["name"])
        df_temp.insert(1, "survey_sentera_id", survey_sentera_id)
        df_temp.insert(2, "fs_sentera_id", row["sentera_id"])
        df_temp.rename(columns={"sentera_id": "file_sentera_id"}, inplace=True)
        df_files = (
            pd_concat([df_files, df_temp], axis=0, ignore_index=True)
            if len(df_files.columns) != 0
            else df_temp.copy()
        )
    df_files = (
        df_files.loc[df_files["file_type"] == file_type]
        if (len(df_files.columns) != 0 and "file_type" in df_files.columns)
        else df_files
    )

    if len(df_files.columns) == 0:  # Most efficient
        logging.warning(
            "No %sfiles available for survey %s",
            str(file_type + " " or ""),
            survey_sentera_id,
        )
    else:
        # TODO: It would be nice to improve the information in this log, but this query only returns sentera_ids and urls
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
    df_analytic_list = DataFrame()
    for _, row in df_survey.iterrows():
        survey_sentera_id = row["survey_sentera_id"]
        df_files = _maybe_find_survey_analytic_files(
            client,
            ds,
            survey_sentera_id=survey_sentera_id,
            analytic_name=analytic_name,
            file_type=file_type,
        )
        if len(df_files.columns) != 0:  # Most efficient
            df_files.insert(1, "date", row["date"])
            df_files.insert(2, "asset_sentera_id", asset_sentera_id)
            df_analytic_list = (
                pd_concat([df_analytic_list, df_files], axis=0, ignore_index=True)
                if len(df_analytic_list.columns) != 0
                else df_files.copy()
            )

    if len(df_analytic_list.columns) == 0:  # Most efficient
        logging.warning(
            "No %sfiles available for asset %s",
            str(file_type + " " or ""),
            asset_sentera_id,
        )
    else:
        logging.info(
            "%s %sfiles available for asset %s",
            len(df_analytic_list),
            str(file_type + " " or ""),
            asset_sentera_id,
        )
    return df_analytic_list


def _get_image_date_for_survey(
    client: Client,
    ds: DSLSchema,
    survey_sentera_id: str,
) -> datetime:
    """
    Get midpoint timestamp of all "captured_at" attributes for images in a given survey.

    Warning:
        CloudVault is known to have incorrect timestamps on images. This function may return an incorrect time.

    Args:
        client, ds: Connections to CloudVault as set up by `get_cv_connection()`
        survey_sentera_id (str): Sentera ID of the survey to query.
    """
    df_images = get_images_by_survey_df(client, ds, survey_sentera_id=survey_sentera_id)

    if len(df_images) == 0:
        raise RuntimeError(
            f"Unable to find images for survey_sentera_id f{survey_sentera_id}"
        )

    min_date = datetime.strptime(df_images["captured_at"].min(), "%Y-%m-%dT%H:%M:%SZ")
    max_date = datetime.strptime(df_images["captured_at"].max(), "%Y-%m-%dT%H:%M:%SZ")
    date_range = max_date - min_date

    if date_range > timedelta(days=1):
        raise RuntimeError(
            f'Image dates in collection "{survey_sentera_id}" differ by more than one day.'
        )

    return min_date + date_range / 2
