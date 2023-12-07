"""Util functions for querying and translating Demeter data."""
from typing import Any, List, Union

from demeter.data import Field, FieldTrial, Plot
from demeter.db import TableId
from pandas import DataFrame

from demeter_utils.query._demeter._core import basic_demeter_query
from demeter_utils.query._demeter._grouper import get_grouper_object_by_id


def get_fields_by_grouper(
    cursor: Any,
    table: TableId,
    grouper_id: int,
    cols: Union[None, List[str]] = None,
) -> DataFrame:
    """SQL query of demeter.field table that returns a dataframe of the fields belonging to a given field group.

    Args:
        cursor: Connection to Demeter
        grouper_id (int): ID of field group in Demeter.FieldGroup
        table (str): Name of database table to query (should be one of ["field", "field_trial", "plot"]).
        cols (list[str]): Names of Demeter.field columns to return. If None, all columns are returned.
    """
    table_name = table.__name__.lower()
    if table not in [Field, FieldTrial, Plot]:
        raise ValueError(f'Groupers are not supported by table "{table_name}".')

    if table not in ["field", "field_trial", "plot"]:
        raise ValueError(f'Groupers are not supported by table "{table}".')

    df_field_summaries = get_grouper_object_by_id(
        cursor, table, grouper_id, include_descendants=True
    )
    # df_field_summaries = getGrouperFields(cursor, grouper_id)

    df_fields = basic_demeter_query(
        cursor,
        table="field",
        cols=cols,
        conditions={"field_id": df_field_summaries["field_id"].to_list()},
    )

    return df_fields
