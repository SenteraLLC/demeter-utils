"""Util functions for querying and translating Demeter data."""
from typing import Any, List, Union

from demeter.data import Field, FieldTrial, Plot
from demeter.db import TableId
from pandas import DataFrame, concat, json_normalize, notnull

from demeter_utils.query._translate import camel_to_snake
from demeter_utils.query.demeter._core import basic_demeter_query


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


def get_planting(
    cursor: Any,
    demeter_table: TableId,
    field_ids: list[int],
    field_trial_ids: list[int],
    plot_ids: list[int],
    date_performed_rename: str = "date_planted",
) -> DataFrame:
    """Get planting information for a field ID or a list of field IDs.

    Args:
        cursor: Connection to demeter
        field_id (int or list[int]): Field id[s] to extract planting data for.
        cols (None or list[str]): List of column names to extract from act table for planting info. If None, returns all columns.
        colname_date (str): Column name to change "date_performed"; defaults to "date_planted".
    """
    table_name = camel_to_snake(demeter_table.__name__)
    if demeter_table not in [Field, FieldTrial, Plot]:
        raise ValueError(f'Groupers are not supported by table "{table_name}".')

    df_table_ids = get_demeter_table_ids(
        cursor, demeter_table, field_ids, field_trial_ids, plot_ids
    )
    # Get any planting activities that are present for Fields, FieldTrials, and Plots, then join them to `df_table_ids`
    # using the appropriate table_level_id as the join column.
    df_plant_acts = DataFrame()
    df_planted = DataFrame()
    for table_level, table_level_ids in zip(
        [Field, FieldTrial, Plot], [field_ids, field_trial_ids, plot_ids]
    ):
        table_level_name = camel_to_snake(table_level.__name__)
        table_level_id = table_level_name + "_id"

        # 1. Get PLANT activities for this table_level, filtering by `table_level_id`
        df_plant_acts_ = basic_demeter_query(
            cursor,
            table="act",
            conditions={"act_type": "PLANT", table_level_id: table_level_ids},
            # explode_details=True,
        )
        if len(df_plant_acts_.columns) == 0:
            continue
        cols_drop = list(
            set(["field_id", "field_trial_id", "plot_id"]) - set([table_level_id])
        )
        df_plant_acts = (
            df_plant_acts_[notnull(df_plant_acts_[table_level_id])]
            .drop(columns=cols_drop)
            .rename(columns={"geom_id": "geom_id_plant"})
        )

        # 2. Join on `table_level_id`
        df_planted_ = df_table_ids.merge(df_plant_acts, how="inner", on=table_level_id)

        # 3. Concat to main df, keeping only the most specific PLANT activities ('keep=last` effectively overwrites rows
        # if more specific data are available; for example, "plot" is more specific than "field_trial", etc.)
        df_planted = concat(
            [df_planted, df_planted_], ignore_index=True, sort=False
        ).drop_duplicates(
            subset=["field_id", "field_trial_id", "plot_id", "geom_id_plant"],
            keep="last",
        )
        # Break out of loop when table_level is reached (can't go further because join col won't exist in df_table_ids)
        if table_level is demeter_table:
            break

    # Ensure all `table_ids` are present by concatenating `df_table_ids` to `df_planted` and dropping duplicates
    df_planted_out = (
        concat(
            [df_table_ids, df_planted],
            ignore_index=True,
        )
        .drop_duplicates(
            subset=["field_id", "field_trial_id", "plot_id", "geom_id_plant"],
            keep="last",
        )
        .reset_index(drop=True)
        .rename(columns={"date_performed": date_performed_rename})
    )
    return df_planted_out

    # crop_type_ids = df_planted_out["crop_type_id"].unique()
    # df_crop = basic_demeter_query(
    #     cursor,
    #     table="crop_type",
    #     conditions={"crop_type_id": crop_type_ids.tolist()},
    #     explode_details=True,
    # )
    # df_planted_out.merge(df_crop, how="left", on="crop_type_id")


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
