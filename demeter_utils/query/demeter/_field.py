"""Util functions for querying and translating Demeter data."""
from typing import Any

from demeter.data import Field, FieldTrial, Plot
from demeter.db import Table, TableId

from demeter_utils.query._translate import camel_to_snake
from demeter_utils.query.demeter._core import basic_demeter_query
from demeter_utils.query.demeter._grouper import get_demeter_object_by_grouper


def get_fields_by_grouper(
    cursor: Any,
    organization_id: TableId,
    grouper_id: TableId,
    include_descendants: bool = True,
):
    return _get_field_fieldtrial_plot_by_grouper(
        cursor,
        Field,
        organization_id,
        grouper_id,
        include_descendants=include_descendants,
    )


def get_field_trials_by_grouper(
    cursor: Any,
    organization_id: TableId,
    grouper_id: TableId,
    include_descendants: bool = True,
):
    return _get_field_fieldtrial_plot_by_grouper(
        cursor,
        FieldTrial,
        organization_id,
        grouper_id,
        include_descendants=include_descendants,
    )


def get_plots_by_grouper(
    cursor: Any,
    organization_id: TableId,
    grouper_id: TableId,
    include_descendants: bool = True,
):
    return _get_field_fieldtrial_plot_by_grouper(
        cursor,
        Plot,
        organization_id,
        grouper_id,
        include_descendants=include_descendants,
    )


def get_fields_by_organization(
    cursor: Any,
    organization_id: TableId,
):
    return _get_field_fieldtrial_plot_by_organization(
        cursor,
        Field,
        organization_id,
    )


def get_field_trials_by_organization(
    cursor: Any,
    organization_id: TableId,
):
    return _get_field_fieldtrial_plot_by_organization(
        cursor,
        FieldTrial,
        organization_id,
    )


def get_plots_by_organization(
    cursor: Any,
    organization_id: TableId,
):
    return _get_field_fieldtrial_plot_by_organization(
        cursor,
        Plot,
        organization_id,
    )


def _get_field_fieldtrial_plot_by_grouper(
    cursor: Any,
    demeter_table: Table,
    organization_id: TableId,
    grouper_id: TableId,
    include_descendants: bool = True,
):
    table_name = camel_to_snake(demeter_table.__name__)
    if demeter_table not in [Field, FieldTrial, Plot]:
        raise ValueError(f'Groupers are not supported by table "{table_name}".')

    # Step 1: Get all descendants of Table Type for `grouper_id`
    if demeter_table in [Field, FieldTrial]:
        # Business as usual
        df_descendants = get_demeter_object_by_grouper(
            cursor,
            demeter_table,
            organization_id,
            grouper_id,
            include_descendants=include_descendants,
        )
        table_id = table_name + "_id"
    if demeter_table == Plot:
        # Get all Field Trials
        df_descendants = get_demeter_object_by_grouper(
            cursor,
            FieldTrial,
            organization_id,
            grouper_id,
            include_descendants=include_descendants,
        )
        table_id = "field_trial_id"

    # Step 2: Get all plots for field trials
    df_result = basic_demeter_query(
        cursor,
        table=table_name,
        cols=None,
        conditions={
            "organization_id": organization_id,
            table_id: df_descendants["table_id"].to_list(),
        },
    )
    return df_result


def _get_field_fieldtrial_plot_by_organization(
    cursor: Any,
    organization_id: int,
    demeter_table: TableId,
):
    # TODO: support date_start and date_end
    table_name = camel_to_snake(demeter_table.__name__)
    if demeter_table not in [Field, FieldTrial, Plot]:
        raise ValueError("`demeter_table` must be one of [Field, FieldTrial, Plot].")

    # Get all ojbects of TableType for `organization_id`
    df_result = basic_demeter_query(
        cursor,
        table=table_name,
        cols=None,
        conditions={
            "organization_id": organization_id,
        },
    )
    return df_result
