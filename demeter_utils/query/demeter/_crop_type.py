"""Util functions for querying and translating Demeter data."""
from typing import Any

from pandas import DataFrame

from demeter_utils.query.demeter._core import basic_demeter_query


def join_crop_type(cursor: Any, df: DataFrame) -> DataFrame:
    """Joins crop type information to `df`."""
    if "crop_type_id" not in df.columns:
        raise ValueError("Column 'crop_type_id' is required.")
    # Join crop type information
    crop_type_ids = df["crop_type_id"].unique().tolist()
    df_crop = basic_demeter_query(
        cursor,
        table="crop_type",
        conditions={"crop_type_id": crop_type_ids},
        explode_details=True,
    ).drop(columns=["created", "last_updated"])
    return df.merge(df_crop, how="left", on="crop_type_id")
