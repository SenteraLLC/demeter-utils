"""Util functions for querying and translating Demeter data."""
from typing import Any, List, Union

from demeter.db._postgres.tools import doPgFormat, doPgJoin  # type: ignore
from pandas import DataFrame
from psycopg2.sql import Identifier

from demeter_utils.query._translate import (
    explode_details as demeter_utils_explode_details,
)

from ._format import format_conditions_dict, format_select_cols


def basic_demeter_query(
    cursor: Any,
    table: str,
    cols: Union[None, str, List[str]] = None,
    conditions: Union[None, dict[str, Any]] = None,
    explode_details: bool = False,
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

    df_result = (
        demeter_utils_explode_details(DataFrame(result), col_details="details")
        if explode_details
        else DataFrame(result)
    )

    return df_result
