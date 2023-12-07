"""Util functions for querying and translating Demeter data."""
from typing import Any

from demeter.data import Field, FieldTrial, Plot
from demeter.db import TableId

from demeter_utils.query._demeter._core import basic_demeter_query
from demeter_utils.query._demeter._grouper import get_grouper_object_by_id
from demeter_utils.query._translate import camel_to_snake


def get_fields_by_grouper(
    cursor: Any,
    grouper_id: int,
    include_descendants: bool = True,
):
    return _get_field_fieldtrial_plot_by_grouper(
        cursor, grouper_id, Field, include_descendants=include_descendants
    )


def get_field_trials_by_grouper(
    cursor: Any,
    grouper_id: int,
    include_descendants: bool = True,
):
    return _get_field_fieldtrial_plot_by_grouper(
        cursor, grouper_id, FieldTrial, include_descendants=include_descendants
    )


def get_plots_by_grouper(
    cursor: Any,
    grouper_id: int,
    include_descendants: bool = True,
):
    return _get_field_fieldtrial_plot_by_grouper(
        cursor, grouper_id, Plot, include_descendants=include_descendants
    )


def _get_field_fieldtrial_plot_by_grouper(
    cursor: Any,
    grouper_id: int,
    demeter_table: TableId,
    include_descendants: bool = True,
):
    table_name = camel_to_snake(demeter_table.__name__)
    if demeter_table not in [Field, FieldTrial, Plot]:
        raise ValueError(f'Groupers are not supported by table "{table_name}".')

    # Step 1: Get all descendants of Table Type for `grouper_id`
    if demeter_table in [Field, FieldTrial]:
        # Business as usual
        df_descendants = get_grouper_object_by_id(
            cursor, demeter_table, grouper_id, include_descendants=include_descendants
        )
    if demeter_table == Plot:
        # Get all Field Trials first
        df_descendants = get_grouper_object_by_id(
            cursor, FieldTrial, grouper_id, include_descendants=include_descendants
        )

    # Step 2: Get all plots for field trials
    df_result = basic_demeter_query(
        cursor,
        table=demeter_table,
        cols=None,
        conditions={table_name + "_id": df_descendants["table_id"].to_list()},
    )
    return df_result
