"""Util functions for querying and translating Demeter data."""
from typing import Any, List, Union

from demeter.data import Field, FieldTrial, Plot
from demeter.db import TableId
from pandas import DataFrame

from demeter_utils.query._demeter._core import basic_demeter_query
from demeter_utils.query._demeter._grouper import get_grouper_object_by_id


def get_fields_by_grouper(
    cursor: Any,
    demeter_table: TableId,
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
    table_name = demeter_table.__name__.lower()
    if demeter_table not in [Field, FieldTrial, Plot]:
        raise ValueError(f'Groupers are not supported by table "{table_name}".')

    df_field_summaries = get_grouper_object_by_id(
        cursor, demeter_table, grouper_id, include_descendants=True
    )
    df_fields = basic_demeter_query(
        cursor,
        table=table_name,
        cols=cols,
        conditions={table_name + "_id": df_field_summaries["table_id"].to_list()},
    )
    return df_fields
