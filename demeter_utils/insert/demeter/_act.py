import logging
from dataclasses import asdict
from typing import Union

from demeter.data import Act, CropType, insertOrGetAct, insertOrGetCropType
from geopandas import GeoDataFrame
from pandas import DataFrame, isna
from psycopg2.extras import NamedTupleCursor
from tqdm import tqdm

from demeter_utils.insert.demeter._core import DEMETER_IDS
from demeter_utils.query.demeter import basic_demeter_query
from demeter_utils.update import update_details


def insert_act(
    cursor: NamedTupleCursor,
    act_type: str,
    df_management: Union[DataFrame, GeoDataFrame],
    df_demeter_object: Union[DataFrame, GeoDataFrame],
    demeter_object_join_cols: list[str] = ["field_trial_name"],
    act_date_col: str = "Act_date",
    crop_col: str = None,
    product_name_col: str = None,
    act_details_col_list: list = [],
) -> tuple[DataFrame, DataFrame]:
    """
    Insert Field Activities and [optional] CropType for multiple demeter objects (i.e., fields, field_trials, or plots).

    Args:
        cursor (NamedTupleCursor): Cursor to demeter schema.
        act_type (str): Type of activity to insert.
        df_management (DataFrame): Management data.
        df_demeter_object (GeoDataFrame): Demeter object data.

        demeter_id_of_act (str): Demeter IDs (one of ["field_id", "field_trial_id", "plot_id"]) to be passed to Act
            records being inserted. Choose the level that corresponds to the specificity of the Activity being inserted.
            For example, if an entire FieldTrial was planted on the same date and into the same crop/product,
            `"field_trial_id"` is appropriate. If each Plot from a FieldTrial had a different fertilizer application
            (product, rate, timing, etc.), `"plot_id"` is appropriate. The passed Demeter ID will be the ID inserted
            into the Act record. Note there is a constraint on the Act table to allow one and only one of
            ["field_id", "field_trial_id", "plot_id"].

        demeter_object_join_cols (list[str], optional): Columns to join `df_management` to `df_demeter_object`. Defaults
            to ["field_trial_name"].

        act_date_col (str, optional): Column in `df_management` that contains the date of the activity. Defaults to
            "Act_date".

        crop_col (str, optional): Column in `df_management` that contains the crop name. Defaults to None.
        product_name_col (str, optional): Column in `df_management` that contains the product name. Defaults to None.

        act_details_col_list (list, optional): List of columns in `df_management` that contain details about the
            activity. Defaults to [].

    """
    logging.info("   Inserting Crop Types")
    if act_type not in ["APPLY", "HARVEST", "MECHANICAL", "PLANT", "TILL"]:
        raise ValueError(
            f'act_type must be one of ["APPLY", "HARVEST", "MECHANICAL", "PLANT", "TILL"], not {act_type}'
        )

    # Passing a crop_type to Activity table is optional
    df_crop_types = (
        _assign_crop_type(cursor, df_management, crop_col, product_name_col)
        if any([crop_col, product_name_col])
        else None
    )

    logging.info("   Inserting %s Activities", act_type)
    df_act = _build_activity_dataframe(
        df_management,
        df_demeter_object,
        demeter_object_join_cols,
        act_date_col,
        act_details_col_list,
        df_crop_types,
        crop_col,
        product_name_col,
    )

    df_act = _insert_or_update_act(
        cursor,
        act_type,
        df_act,
        act_date_col,
        act_details_col_list,
    )
    return df_crop_types, df_act


def _build_activity_dataframe(
    df_management: DataFrame,
    df_demeter_object: GeoDataFrame,
    demeter_object_join_cols: list[str] = ["field_trial_name"],
    act_date_col: str = "Act_date",
    act_details_col_list: list = [],
    df_crop_types: DataFrame = None,
    crop_col: str = None,
    product_name_col: str = None,
) -> DataFrame:
    # Get all column in df_demeter_object.columns that begin with  any of the strings in DEMETER_IDS
    cols_demeter_ids = [
        c
        for c in df_demeter_object.columns
        if any(c.startswith(s) for s in DEMETER_IDS)
    ]
    # Separate out crop_types in case they are not provided (df_crop_types is optional).
    cols_crop_types = (
        [
            crop_col,
            product_name_col,
        ]
        if df_crop_types is not None
        else []
    )
    df_act_ = (
        df_management[
            [act_date_col]
            + demeter_object_join_cols
            + cols_crop_types
            + act_details_col_list
        ]
        .drop_duplicates()
        .reset_index(drop=True)
        .merge(
            df_demeter_object[demeter_object_join_cols + cols_demeter_ids],
            on=demeter_object_join_cols,
        )
    )
    df_act = (
        df_act_.merge(df_crop_types, on=[crop_col, product_name_col])
        if df_crop_types is not None
        else df_act_
    )
    return df_act


def _assign_crop_type(
    cursor: NamedTupleCursor,
    df_management: DataFrame,
    crop_col: str = "Crop",
    product_name_col: str = "Variety",
) -> DataFrame:
    """Insert CropType."""
    df_crop_types = df_management[[crop_col, product_name_col]].drop_duplicates()
    crop_type_ids = []
    for ind in tqdm(range(len(df_crop_types)), desc="Inserting Crop Types:"):
        row = df_crop_types.iloc[ind]
        crop_type = CropType(
            crop=row[crop_col].upper(), product_name=row[product_name_col].upper()
        )
        crop_type_id = insertOrGetCropType(cursor, crop_type)
        crop_type_ids.append(crop_type_id)
    df_crop_types["crop_type_id"] = crop_type_ids
    return df_crop_types


def _insert_or_update_act(
    cursor: NamedTupleCursor,
    act_type: str,
    df_act: DataFrame,
    act_date_col: str = "Act_date",
    act_details_col_list: list = [],
) -> DataFrame:
    """
    Insert or update Activities from `df_act`. If the act already exists, updates the act if "details" have changed.
    """
    act_ids = []
    for ind in tqdm(range(len(df_act)), desc=f"Inserting {act_type} Activities:"):
        row = df_act.iloc[ind]
        crop_type_id = int(row.crop_type_id) if "crop_type_id" in row.index else None
        # TODO: Support for geom_id (should only be used if more specific than field, field_trial, or plot_ids)
        # Choose one of field_id, field_trial_id, or plot_id to pass to Act
        # plot_id is most specific
        plot_id = int(row.plot_id) if "plot_id" in row.index else None

        # field_trial_id is 2nd most specific
        field_trial_id = (
            int(row.field_trial_id)
            if ("field_trial_id" in row.index and isna(plot_id))
            else None
        )
        # field_id is least specific
        field_id = (
            int(row.field_id)
            if ("field_id" in row.index and isna(field_trial_id) and isna(plot_id))
            else None
        )
        if all([isna(i) for i in [plot_id, field_trial_id, field_id]]):
            # Is warning enough?
            logging.warning("plot_id, field_trial_id, and field_id are all NULL.")

        act = Act(
            act_type=act_type,
            date_performed=row[act_date_col],
            crop_type_id=crop_type_id,
            field_id=field_id,
            field_trial_id=field_trial_id,
            plot_id=plot_id,
            geom_id=None,
            details={
                k: None if isna(v) else v
                for k, v in row[row.index.isin(act_details_col_list)].to_dict().items()
            },
        )
        act_id = insertOrGetAct(cursor, act)

        # UPDATE DETAILS
        # TODO: How can the following be refactored into insertOrUpdateOrGetAct(cursor, act) function?
        # Now that we have act_id, we can check for differences in "details" column between act and act_id
        table_cols = [
            "act_type",
            "date_performed",
            "crop_type_id",
            "field_id",
            "field_trial_id",
            "plot_id",
            "geom_id",
            "details",
        ]
        # Get record corresponding to passed act_id
        act_record = basic_demeter_query(
            cursor,
            table="act",
            cols=["act_id"] + table_cols,
            conditions={"act_id": act_id},
        ).to_records()
        # Create Act object from record
        act_db = Act(
            act_type=act_record.act_type[0],
            date_performed=act_record.date_performed[0],
            crop_type_id=act_record.crop_type_id[0],
            field_id=act_record.field_id[0],
            field_trial_id=act_record.field_trial_id[0],
            plot_id=act_record.plot_id[0],
            geom_id=act_record.geom_id[0],
            details=act_record.details[0],
        )
        # Convert dataclass to dict for comparison
        act_db_dict = {k: asdict(act_db)[k] for k in table_cols}
        act_dict = {k: asdict(act)[k] for k in table_cols}

        # If dicts aren't the same, update details column
        if act_dict != act_db_dict:
            logging.info(
                "     Updating details column in act table for act_id %s", act_id
            )
            update_details(
                cursor,
                demeter_table=Act,
                table_id=act_record.act_id[0],
                details=act_dict["details"],
            )

        act_ids.append(act_id)
    df_act["act_id_" + act_type.lower()] = act_ids
    return df_act
