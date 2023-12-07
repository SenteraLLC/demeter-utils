"""Util functions for querying and translating Demeter data."""
from typing import Any, List, Union

from demeter.data import getGrouperFields  # type: ignore
from demeter.db._postgres.tools import doPgFormat, doPgJoin  # type: ignore
from pandas import DataFrame, json_normalize
from psycopg2.sql import Identifier

from ._query_format import format_conditions_dict, format_select_cols


def basic_demeter_query(
    cursor: Any,
    table: str,
    cols: Union[None, str, List[str]] = None,
    conditions: Union[None, dict[str, Any]] = None,
) -> DataFrame:
    """Generalized SQL query to pandas DataFrame which searches within one table (`table`) based on `conditions` and selects `cols`.

    If `conditions` are not provided, full table is returned.
    If `cols` is not provided, all columns are returned.

    Args:
        cursor (Any): Connection to Demeter database
        table (str): Name of SQL table to query
        cols (str or list[str]): Column names to extract from `table` and return in dataframe

        conditions (dict): Dictionary containing key-value pairs of query constraints, where the key
            is the column name and the value is the value (or list of values) of the key to filter on.
    """

    # format conditions into SQL
    if conditions is not None:
        formatted_conditions = format_conditions_dict(conditions)

    # format column names into SQL Composable objects
    formatted_cols = format_select_cols(cols)

    # prepare query and execute
    if conditions is None:
        stmt = doPgFormat(
            "SELECT {0} FROM {1}",
            formatted_cols,
            Identifier(table),
        )
    else:
        stmt = doPgFormat(
            "SELECT {0} FROM {1} WHERE {2}",
            formatted_cols,
            Identifier(table),
            doPgJoin(" AND ", formatted_conditions),
        )

    cursor.execute(stmt, conditions)
    result = cursor.fetchall()

    df_result = DataFrame(result)

    return df_result


def get_obs_type_and_unit_colname(
    cursor: Any, observation_type_id: int, unit_type_id: int
) -> str:
    """Formats a feature column name for a given observation type and unit type in demeter.

    Takes observation type ID and unit type ID and returns a column name
    for that observation, that includes both observation type name and unit name
    separated with underscores.

    Args:
        cursor: Connection to Demeter database

        observation_type_id (int): Observation type ID to look at in Demeter where `type_name` is
            used to create column name.

        unit_type_id (int): Unit type ID to look at in Demeter where name is appended to column
            name.
    """
    df_type = basic_demeter_query(
        cursor=cursor,
        table="observation_type",
        cols="type_name",
        conditions={"observation_type_id": observation_type_id},
    )

    type_name = str(df_type.iloc[0, 0])

    if " - " in type_name:
        type_name = type_name.replace(" - ", "_")
    # some names have hyphens and we should replace those with underscores, too

    joined_type_name = type_name.replace(" ", "_")

    df_unit = basic_demeter_query(
        cursor=cursor,
        table="unit_type",
        cols="unit_name",
        conditions={"unit_type_id": unit_type_id},
    )

    unit_name = str(df_unit.iloc[0, 0])
    if "/" in unit_name:
        unit_name = unit_name.replace("/", "_")
    # if there is a forward slash in units, replace with underscore

    if unit_name == "unitless":
        return joined_type_name
    else:
        return f"{joined_type_name}_{unit_name}"


def get_df_fields_for_field_group(
    cursor: Any, field_group_id: int, cols: Union[None, List[str]] = None
) -> DataFrame:
    """SQL query of demeter.field table that returns a dataframe of the fields belonging to a given field group.

    Args:
        cursor: Connection to Demeter
        field_group_id (int): ID of field group in Demeter.FieldGroup

        cols (list[str]): Names of Demeter.field columns to return. If None,
            all columns are returned.
    """
    df_field_summaries = getGrouperFields(cursor, field_group_id)

    df_fields = basic_demeter_query(
        cursor,
        table="field",
        cols=cols,
        conditions={"field_id": df_field_summaries["field_id"].to_list()},
    )

    return df_fields


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
