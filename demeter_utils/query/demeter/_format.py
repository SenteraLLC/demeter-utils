"""Functions to translate Python to SQL "WHERE" statements for Composable SQL statements."""
from typing import Any, List, Union, cast

from demeter.db._postgres.tools import doPgFormat, doPgJoin  # type: ignore
from psycopg2.sql import SQL, Composed, Identifier, Placeholder


def where_col_is_null(col: str):
    """Equivalent to `where col is NULL`"""
    return doPgFormat("{0} IS NULL", Identifier(col))


def where_col_value_equals_value(col: str):
    """Equivalent to `where col = value`."""
    return doPgJoin(" = ", [Identifier(col), Placeholder(col)])


def where_col_value_in_list(col: str, values: Union[List[str], List[int], List[float]]):
    """Equivalent to `where col in values`."""
    assert type(values[0]) in [
        str,
        float,
        int,
    ], "List types must be numeric or strings."

    if isinstance(values[0], str):
        sql_safe_list_query = "{0} IN ('" + "', '".join(values) + "')"
    else:
        numeric_list_to_str_list = [str(val) for val in values]
        sql_safe_list_query = "{0} IN (" + ", ".join(numeric_list_to_str_list) + ")"

    return doPgFormat(sql_safe_list_query, Identifier(col))


def format_conditions_dict(conditions: dict[str, Any]):
    """Parses through query conditions and translates to SQL WHERE statement using SQL Composable objects."""

    assert conditions is not None, "`conditions` was passed as `None`."

    formatted_conditions: List[Any] = []
    for key in conditions.keys():
        if conditions[key] is None:
            formatted_conditions += [where_col_is_null(key)]

        elif isinstance(conditions[key], list):
            formatted_conditions += [where_col_value_in_list(key, conditions[key])]

        else:
            formatted_conditions += [where_col_value_equals_value(key)]
    return formatted_conditions


def format_select_cols(cols: Union[List[str], str, None]):
    """Format `cols` for SQL SELECT statement.

    If `cols` is None, return `*`.
    """

    # format column names into SQL Composable objects
    if cols is None:
        formatted_cols = cast(Composed, SQL("*"))
    else:
        if not isinstance(cols, list):
            cols = [cols]
        formatted_cols = SQL(", ").join([Identifier(c) for c in cols])

    return formatted_cols
