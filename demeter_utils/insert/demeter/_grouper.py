import logging
from typing import Union

from demeter.data import Grouper, insertOrGetGrouper
from geopandas import GeoDataFrame
from pandas import DataFrame
from psycopg2.extras import NamedTupleCursor
from tqdm import tqdm


def insert_groupers(
    cursor: NamedTupleCursor,
    gdf_grouper: Union[DataFrame, GeoDataFrame],
    organization_id: int,
    grouper_variable: str,
    parent_grouper_id: int = None,
) -> DataFrame:
    """Assign "Children" Grouper IDs."""
    df_groups = gdf_grouper[[grouper_variable]].drop_duplicates().reset_index(drop=True)
    group_ids = []
    logging.info(' Inserting unique "%s" as the Grouper variable', grouper_variable)
    parent_grouper_id = (
        int(parent_grouper_id) if parent_grouper_id else parent_grouper_id
    )
    for ind in tqdm(
        range(len(df_groups)),
        desc="Inserting Groupers",
    ):
        row = df_groups.iloc[ind]
        group = Grouper(
            name=row[grouper_variable],
            organization_id=int(organization_id),
            parent_grouper_id=parent_grouper_id,
            details={},
        )
        group_id = insertOrGetGrouper(cursor, group)
        group_ids.append(group_id)
    df_groups["organization_id"] = organization_id
    df_groups["grouper_id"] = group_ids
    return df_groups
