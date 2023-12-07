"""Util functions for querying and translating Demeter data."""
from typing import Any, List, Union

from pandas import DataFrame, json_normalize

from demeter_utils.query._demeter._core import basic_demeter_query


def get_as_applied(
    cursor: Any,
    field_id: Union[int, List[int]],
    colname_date: str = "date_applied",
) -> DataFrame:
    """Get all as-applied information for a field ID or a list of field IDs.

    Includes information on method, nutrient, source, and rate are included as separate
    columns.

    Args:
        cursor: Connection to demeter
        field_id (int or list[int]): Field id[s] to extract harvest data for.

        colname_date (str): Column name to change "date_performed"; defaults to
            "date_applied".
    """
    # get all fertilizer information for this field
    df_applied = basic_demeter_query(
        cursor,
        table="act",
        cols=["field_id", "date_performed", "details"],
        conditions={"act_type": "fertilize", "field_id": field_id},
    ).rename(columns={"date_performed": colname_date})

    df_applied["applications"] = df_applied["details"].map(
        lambda val: val["applications"]
    )
    df_expanded = df_applied.explode("applications")

    # expand application information
    df_apps = json_normalize(df_expanded["applications"].to_list())
    df_apps["field_id"] = df_expanded["field_id"].to_list()
    df_apps[colname_date] = df_expanded[colname_date].to_list()

    return df_apps


def get_planting(
    cursor: Any,
    field_id: list[int],
    cols: Union[None, list[str]] = ["field_id", "date_performed"],
    colname_date: str = "date_planted",
) -> DataFrame:
    """Get planting information for a field ID or a list of field IDs.

    Args:
        cursor: Connection to demeter
        field_id (int or list[int]): Field id[s] to extract planting data for.

        cols (None or list[str]): List of column names to extract from act table
            for planting info. If None, returns all columns.

        colname_date (str): Column name to change "date_performed"; defaults to
            "date_planted".
    """
    df_planted = basic_demeter_query(
        cursor,
        table="act",
        cols=cols,
        conditions={"act_type": "plant", "field_id": field_id},
    ).rename(columns={"date_performed": colname_date})

    return df_planted


def get_harvest(
    cursor: Any,
    field_id: list[int],
    cols: Union[None, list[str]] = ["field_id", "date_performed"],
    colname_date: str = "date_harvested",
) -> DataFrame:
    """Get harvest information for a field ID or a list of field IDs.

    Args:
        cursor: Connection to demeter
        field_id (int or list[int]): Field id[s] to extract harvest data for.

        cols (None or list[str]): List of column names to extract from act table
            for harvest info. If None, returns all columns.

        colname_date (str): Column name to change "date_performed"; defaults to
            "date_harvested".
    """
    df_harvested = basic_demeter_query(
        cursor,
        table="act",
        cols=cols,
        conditions={"act_type": "harvest", "field_id": field_id},
    ).rename(columns={"date_performed": colname_date})

    return df_harvested
