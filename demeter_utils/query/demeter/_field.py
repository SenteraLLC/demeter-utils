"""Util functions for querying and translating Demeter data."""
from typing import Any

from demeter.data import Field, FieldTrial, Plot
from demeter.db import Table, TableId
from pandas import DataFrame
from psycopg2.extensions import AsIs

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
    return get_demeter_object_by_organization(
        cursor,
        Field,
        organization_id,
    )


def get_field_trials_by_organization(
    cursor: Any,
    organization_id: TableId,
):
    return get_demeter_object_by_organization(
        cursor,
        FieldTrial,
        organization_id,
    )


def get_plots_by_organization(
    cursor: Any,
    organization_id: TableId,
):
    return get_demeter_object_by_organization(
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


# def _get_field_fieldtrial_plot_by_organization(
#     cursor: Any,
#     demeter_table: TableId,
#     organization_id: int,
# ):
#     # TODO: support date_start and date_end
#     table_name = camel_to_snake(demeter_table.__name__)
#     if demeter_table not in [Field, FieldTrial, Plot]:
#         raise ValueError("`demeter_table` must be one of [Field, FieldTrial, Plot].")

#     # Get all ojbects of TableType for `organization_id`
#     df_result = basic_demeter_query(
#         cursor,
#         table=table_name,
#         cols=None,
#         conditions={
#             "organization_id": organization_id,
#         },
#     )
#     return df_result


def get_demeter_object_by_organization(
    cursor: Any,
    demeter_table: Table,
    organization_id: TableId,
) -> DataFrame:
    """Gets all Organization descendants for a given Organization ID (sorted by ancestrial distance)."""
    table_name = camel_to_snake(demeter_table.__name__)
    if demeter_table not in [Field, FieldTrial, Plot]:
        raise ValueError(f'Groupers are not supported by table "{table_name}".')

    def _sql_field_level():
        select_field = """
            organization.name as organization_name,
            organization.organization_id,
            field.name as field_name,
            field.field_id,
            field.date_start as field_date_start,
            field.date_end as field_date_end,
            field.geom_id as field_geom_id,
            field.grouper_id as field_grouper_id,
            field.details as field_details
        """
        join_field = """JOIN field USING(organization_id)"""
        return select_field, join_field

    def _sql_field_trial_level():
        field_select_stmt, field_join_stmt = _sql_field_level()
        select_field_trial = (
            field_select_stmt
            + """
            , field_trial.name as field_trial_name,
            field_trial.field_trial_id,
            field_trial.date_start as field_trial_date_start,
            field_trial.date_end as field_trial_date_end,
            field_trial.geom_id as field_trial_geom_id,
            field_trial.grouper_id as field_trial_grouper_id,
            field_trial.details as field_trial_details
        """
        )
        join_field_trial = field_join_stmt + """ JOIN field_trial USING(field_id)"""
        return select_field_trial, join_field_trial

    def _sql_plot_level():
        field_trial_select_stmt, field_trial_join_stmt = _sql_field_trial_level()
        select_plot = (
            field_trial_select_stmt
            + """
            , plot.name as plot_name,
            plot.plot_id,
            plot.geom_id as plot_geom_id,
            plot.treatment_id,
            plot.block_id,
            plot.replication_id,
            plot.details as plot_details
        """
        )
        join_plot = field_trial_join_stmt + """ JOIN plot USING(field_trial_id)"""
        return select_plot, join_plot

    if demeter_table is Field:
        select_stmt, join_stmt = _sql_field_level()
    if demeter_table is FieldTrial:
        select_stmt, join_stmt = _sql_field_trial_level()
    if demeter_table is Plot:
        select_stmt, join_stmt = _sql_plot_level()
    stmt = """
    select %(select_stmt)s
    from organization
    %(join_stmt)s
    where organization_id = %(organization_id)s
    """
    params = {
        "select_stmt": AsIs(select_stmt),
        "join_stmt": AsIs(join_stmt),
        "organization_id": AsIs(organization_id),
    }
    cursor.execute(stmt, params)
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(
            f'Failed to get "{table_name}"" objects for Organization ID {organization_id}'
        )

    return DataFrame(results)
