"""Util functions for querying and translating Demeter data."""
from typing import Any

from demeter.data import Field, FieldTrial, Plot
from demeter.db import TableId
from pandas import DataFrame, concat, notnull

from demeter_utils.query._translate import camel_to_snake
from demeter_utils.query.demeter._core import basic_demeter_query
from demeter_utils.query.demeter._crop_type import join_crop_type


def get_act(
    cursor: Any,
    act_type: str,
    demeter_table: TableId,
    field_ids: list[int],
    field_trial_ids: list[int],
    plot_ids: list[int],
    date_performed_rename: str = "date_performed",
    explode_details: bool = False,
) -> DataFrame:
    """Get data from the Act table based on Field, FieldTrial, and Plot IDs.

    Args:
        cursor: Connection to demeter
        field_id (int or list[int]): Field id[s] to extract planting data for.
        date_performed_rename (str): Column name to change "date_performed"; defaults to "date_planted".
    """
    table_name = camel_to_snake(demeter_table.__name__)
    if demeter_table not in [Field, FieldTrial, Plot]:
        raise ValueError(f'Groupers are not supported by table "{table_name}".')

    act_name = act_type.upper()
    geom_name = f"geom_id_{act_name.lower()}"

    df_table_ids = get_demeter_table_ids(
        cursor, demeter_table, field_ids, field_trial_ids, plot_ids
    )
    # Get any application activities that are present for Fields, FieldTrials, and Plots, then join them to `df_table_ids`
    # using the appropriate table_level_id as the join column.
    df_application_acts = DataFrame()
    df_apps = DataFrame()
    for table_level, table_level_ids in zip(
        [Field, FieldTrial, Plot], [field_ids, field_trial_ids, plot_ids]
    ):
        table_level_name = camel_to_snake(table_level.__name__)
        table_level_id = table_level_name + "_id"

        # 1. Get APPLICATION activities for this table_level, filtering by `table_level_id`
        df_application_acts_ = basic_demeter_query(
            cursor,
            table="act",
            conditions={"act_type": act_name, table_level_id: table_level_ids},
            explode_details=explode_details,
        )
        if len(df_application_acts_.columns) == 0:
            continue
        cols_drop = list(
            set(["field_id", "field_trial_id", "plot_id"]) - set([table_level_id])
        )
        df_application_acts = (
            df_application_acts_[notnull(df_application_acts_[table_level_id])]
            .drop(columns=cols_drop)
            .rename(columns={"geom_id": geom_name})
        )

        # 2. Join on `table_level_id`
        df_apps_ = df_table_ids.merge(
            df_application_acts, how="inner", on=table_level_id
        )

        # 3. Concat to main df, keeping only the most specific PLANT activities ('keep=last` effectively overwrites rows
        # if more specific data are available; for example, "plot" is more specific than "field_trial", etc.)
        df_apps = concat(
            [df_apps, df_apps_], ignore_index=True, sort=False
        ).drop_duplicates(
            subset=["field_id", "field_trial_id", "plot_id", geom_name],
            keep="last",
        )
        # Break out of loop when table_level is reached (can't go further because join col won't exist in df_table_ids)
        if table_level is demeter_table:
            break

    # Ensure all `table_ids` are present by concatenating `df_table_ids` to `df_apps` and dropping duplicates
    df_apps_out_ = (
        concat(
            [df_table_ids, df_apps],
            ignore_index=True,
        )
        .drop_duplicates(
            subset=["field_id", "field_trial_id", "plot_id", geom_name],
            keep="last",
        )
        .reset_index(drop=True)
        .rename(columns={"date_performed": date_performed_rename})
    )

    # Join crop type information
    df_apps_out = join_crop_type(cursor, df_apps_out_)
    return df_apps_out


def get_demeter_table_ids(
    cursor: Any,
    demeter_table: TableId,
    field_ids: list[int],
    field_trial_ids: list[int],
    plot_ids: list[int],
) -> DataFrame:
    """Gets table IDs based on `demeter_table`.

    Returns:
        DataFrame: Note that columns are not guaranteed to exist (e.g., if `demeter_table` is `Field`, "plot_id" and
        "field_trial_id" columns will not be present).
    """
    table_name = camel_to_snake(demeter_table.__name__)
    table_id = table_name + "_id"
    table_ids = (
        field_ids
        if demeter_table is Field
        else field_trial_ids
        if demeter_table is FieldTrial
        else plot_ids
    )
    # df_table_ids is not guaranteed to have `plot_id` or `field_trial_id`
    df_table_ids = (
        basic_demeter_query(
            cursor,
            table=table_name,
            conditions={table_id: table_ids},
        )
        .drop(columns=["details", "created", "last_updated"])
        .rename(columns={"geom_id": f"geom_id_{table_name}"})
    )
    return df_table_ids
