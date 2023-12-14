"""Util functions for querying and translating Demeter data."""
from typing import Any

from pandas import DataFrame

from demeter_utils.query._translate import reorder_dataframe_columns
from demeter_utils.query.demeter._core import basic_demeter_query


def join_crop_type(cursor: Any, df: DataFrame) -> DataFrame:
    """Joins crop type information to `df`."""
    if "crop_type_id" not in df.columns:
        raise ValueError("Column 'crop_type_id' is required.")
    # Join crop type information
    crop_type_ids = df["crop_type_id"].unique().tolist()
    if None in crop_type_ids:
        crop_type_ids.remove(None)
    # crop_type_ids = [x for x in df["crop_type_id"].unique().tolist() if x is not None]
    if len(crop_type_ids) == 0:
        return df
    df_crop = basic_demeter_query(
        cursor,
        table="crop_type",
        conditions={"crop_type_id": crop_type_ids},
        explode_details=True,
    ).drop(columns=["created", "last_updated"])
    df_out = df.merge(df_crop, how="left", on="crop_type_id")

    cols_to_reorder = df_crop.drop(columns="crop_type_id").columns.tolist()
    return reorder_dataframe_columns(
        df_out, col_to_insert_after="crop_type_id", cols_to_reorder=cols_to_reorder
    )
