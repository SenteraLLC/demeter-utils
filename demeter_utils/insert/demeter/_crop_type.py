import logging

from demeter.data import CropType, insertOrGetCropType
from pandas import DataFrame, isna
from psycopg2.extras import NamedTupleCursor
from tqdm import tqdm


def insert_or_get_crop_type(
    cursor: NamedTupleCursor,
    df_management: DataFrame,
    crop_col: str = "Crop",
    product_name_col: str = "Variety",
) -> DataFrame:
    """Insert CropType."""
    logging.info("   Inserting Crop Types")
    df_crop_types = df_management[[crop_col, product_name_col]].drop_duplicates()
    crop_type_ids = []
    for ind in tqdm(range(len(df_crop_types)), desc="Inserting Crop Types:"):
        row = df_crop_types.iloc[ind]
        crop = row[crop_col] if crop_col in row.index else None
        crop = crop.upper() if not isna(crop) else None
        product_name = row[product_name_col] if product_name_col in row.index else None
        product_name = product_name.upper() if not isna(product_name) else None
        crop_type = CropType(crop=crop, product_name=product_name)
        crop_type_id = insertOrGetCropType(cursor, crop_type)
        crop_type_ids.append(crop_type_id)
    df_crop_types["crop_type_id"] = crop_type_ids
    return df_crop_types
