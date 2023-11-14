import logging
from ast import literal_eval
from datetime import datetime
from os import getenv

from demeter.data import (
    Field,
    FieldGroup,
    insertOrGetField,
    insertOrGetFieldGroup,
    insertOrGetGeom,
)
from geopandas import GeoDataFrame
from psycopg2.extras import NamedTupleCursor
from sqlalchemy.engine import Connection
from tqdm import tqdm


def _assign_field_group_ids(
    cursor: NamedTupleCursor,
    org_name: str,
) -> list:
    """
    Assigns Demeter DB Field Group IDs for each asset in `ASSET_SENTERA_ID` from environment variable.

    Args:
        cursor (NamedTupleCursor): Cursor to demeter schema
        org_name (str): Name of organization (top level Field Group).

    Returns:
        list: Field Group IDs for each asset in `ASSET_SENTERA_ID`.
    """
    ASSET_SENTERA_ID = literal_eval(getenv("ASSET_SENTERA_ID"))  # noqa: N806

    field_group = FieldGroup(
        name=org_name, details={"sentera_id": getenv("ORG_SENTERA_ID")}
    )
    field_group_id = insertOrGetFieldGroup(cursor, field_group)

    field_group_db_ids = []
    for asset in ASSET_SENTERA_ID.keys():
        temp_field_group = FieldGroup(
            name=asset,
            parent_field_group_id=field_group_id,
            details={
                "sentera_id": ASSET_SENTERA_ID[asset],
            },
        )
        temp_field_group_id = insertOrGetFieldGroup(cursor, temp_field_group)
        field_group_db_ids += [temp_field_group_id]
    return field_group_db_ids


def _assign_field_ids(
    cursor: NamedTupleCursor,
    gdf_exp_design: GeoDataFrame,
    field_group_db_ids: list,
    year: int,
) -> dict:
    """
    Assigns Demeter DB Field IDs to each plot in `gdf_exp_design`.

    Returns:
        plot_demeter_ids (dict): Dictionary of plot names and their Demeter DB Field IDs.
    """
    for col in ["site_name", "plot_id", "range", "row", gdf_exp_design.geometry.name]:
        if col not in gdf_exp_design.columns:
            raise RuntimeError(
                f'Column "{col}" is required, but is not present in gdf_exp_design.'
            )
    ASSET_SENTERA_ID = literal_eval(getenv("ASSET_SENTERA_ID"))  # noqa: N806

    plot_demeter_ids = {}
    gdf_exp_design.reset_index(drop=True, inplace=True)
    logging.info('  Inserting Plot geometries as "Fields"')
    for ind in tqdm(
        range(len(gdf_exp_design)),
        desc="Inserting plot experimental design information",
    ):
        row = gdf_exp_design.loc[ind]
        name = f'{row["site_name"].replace(" ", "-").lower()}_{row["plot_id"]}'

        # Get Field Group and Geometry
        matched_field_group = list(ASSET_SENTERA_ID.keys()).index(row["site_name"])
        geometry = row[gdf_exp_design.geometry.name].geoms[0]
        temp_geom_id = insertOrGetGeom(cursor, geometry)

        # Assign Field
        temp_f = Field(
            geom_id=temp_geom_id,
            date_start=datetime(year, 1, 1),
            date_end=datetime(year, 12, 31),
            # name=f"{row['site_name']}_{row['sentera_id']}",
            name=name,
            field_group_id=field_group_db_ids[matched_field_group],
            details={
                "site_name": row["site_name"],
                "plot_id": int(row["plot_id"]),
                "range": int(row["range"]),
                "row": int(row["row"]),
            },
        )
        temp_f_id = insertOrGetField(cursor, temp_f)
        plot_demeter_ids[name] = temp_f_id
    return plot_demeter_ids


def insert_field_and_field_group(
    conn: Connection,
    gdf_exp_design: GeoDataFrame,
    org_name: str,
    year: int,
) -> tuple[list, dict]:
    """Insert experimental design/plot information into `demeter` database.

    Inserts:
    - FieldGroup
    - Geom (each plot)
    - Field (each plot)

    Args:
        conn (Connection): Connection to demeter schema
        gdf_exp_design (GeoDataFrame): See `demeter_utils.cli.download_field_insights_data`
        org_name (str): Name of organization (top level Field Group).
        year (int, optional): Year of experiment.

    Returns:
        tuple[list, dict]: Field Group IDs for each asset in `ASSET_SENTERA_ID` and dictionary of plot names and their
        Demeter DB Field IDs.
    """
    connect = conn.connection

    with connect.cursor() as cursor:
        # insert field groups
        logging.info('  Inserting Field Groups under "%s" org', org_name)
        field_group_db_ids = _assign_field_group_ids(cursor, org_name=org_name)
        # Insert plots (as Fields)
        plot_demeter_ids = _assign_field_ids(
            cursor, gdf_exp_design, field_group_db_ids, year=year
        )
        connect.commit()
    connect.close()
    return field_group_db_ids, plot_demeter_ids
