import logging
from datetime import datetime

from demeter.data import (
    Act,
    CropType,
    Field,
    Grouper,
    Observation,
    ObservationType,
    UnitType,
    insertOrGetAct,
    insertOrGetCropType,
    insertOrGetField,
    insertOrGetGeom,
    insertOrGetGrouper,
    insertOrGetObservation,
    insertOrGetObservationType,
    insertOrGetUnitType,
)
from geopandas import GeoDataFrame
from pandas import DataFrame
from sqlalchemy.engine import Connection
from tqdm import tqdm

from ._utils import get_date_planted_for_plot, get_unix_from_datetime
from .constants import ASSET_SENTERA_ID


def insert_data(
    conn: Connection, gdf_plot: GeoDataFrame, file_metadata: dict, df_ndvi: DataFrame
) -> None:
    """Insert Bayer Canola Phase I data into `demeter`.

    Currently inserts:
    - Grouper
    - Geom (each plot)
    - Field (each plot)
    - CropType (corn, no product name)
    - Act (planting)
    - Observation (maturity date)
    - Observation (plot mean NDVI)

    Args:
        conn (Connection): Connection to demeter schema
        gdf_plot, file_metadata: See `collect_data()`
        df_ndvi: See `collect_ndvi_data()`
    """
    connect = conn.connection

    with connect.cursor() as cursor:
        # insert field groups
        logging.info("   Inserting Field Groups")
        bayer_canola_fg = Grouper(name="Bayer Canola")
        bayer_canola_fg_id = insertOrGetGrouper(cursor, bayer_canola_fg)

        asset_demeter_ids = []
        for asset in ASSET_SENTERA_ID.keys():
            temp_fg = Grouper(
                name=asset,
                parent_field_group_id=bayer_canola_fg_id,
                details=file_metadata[asset],  # add geojson metadata here
            )
            temp_fg_id = insertOrGetGrouper(cursor, temp_fg)
            asset_demeter_ids += [temp_fg_id]

        # insert crop type
        crop_type = CropType(crop="corn")
        crop_type_id = insertOrGetCropType(cursor, crop_type)

        # insert plots as fields
        plot_demeter_ids = {}
        gdf_plot.reset_index(drop=True, inplace=True)
        logging.info("   Inserting Fields and Acts")
        for ind in tqdm(range(len(gdf_plot)), desc="Inserting plot data:"):
            row = gdf_plot.loc[ind]
            matched_fg = list(ASSET_SENTERA_ID.keys()).index(row["site"])

            # geometry
            geometry = row["geometry"].geoms[0]
            temp_geom_id = insertOrGetGeom(cursor, geometry)

            # field
            temp_f = Field(
                geom_id=temp_geom_id,
                date_start=datetime(2021, 1, 1),
                date_end=datetime(2021, 12, 31),
                name=f"{row['site']}_{row['sentera_id']}",
                field_group_id=asset_demeter_ids[matched_fg],
                details={
                    "sentera_id": int(row["sentera_id"]),
                    "range": int(row["range"]),
                    "column": int(row["column"]),
                },
            )
            temp_f_id = insertOrGetField(cursor, temp_f)
            plot_demeter_ids[f"{row['site']}_{row['sentera_id']}"] = temp_f_id

            # planting act
            date_planted_utc = get_date_planted_for_plot(
                site=row["site"],
                sentera_id=row["sentera_id"],
                geom=row["geometry"],
                df=gdf_plot,
            )
            temp_act = Act(
                act_type="plant",
                field_id=temp_f_id,
                date_performed=date_planted_utc,
                crop_type_id=crop_type_id,
            )
            _ = insertOrGetAct(cursor, temp_act)

        connect.commit()

        # insert observation types
        logging.info("   Inserting Observation and Unit Types")
        obs_type = {}
        maturity_obs_type = ObservationType(type_name="maturity date")
        obs_type["maturity date"] = insertOrGetObservationType(
            cursor, maturity_obs_type
        )

        ndvi_obs_type = ObservationType(type_name="plot mean ndvi (71203-00)")
        obs_type["ndvi"] = insertOrGetObservationType(cursor, ndvi_obs_type)

        # insert unit types
        unit_type = {}
        maturity_unit_type = UnitType(
            unit_name="UNIX timestamp", observation_type_id=obs_type["maturity date"]
        )
        unit_type["maturity date"] = insertOrGetUnitType(cursor, maturity_unit_type)

        ndvi_unit_type = UnitType(
            unit_name="Unitless", observation_type_id=obs_type["ndvi"]
        )
        unit_type["ndvi"] = insertOrGetUnitType(cursor, ndvi_unit_type)
        connect.commit()

        # insert maturity date observations
        for _, row in gdf_plot.iterrows():
            field_name = f"{row['site']}_{row['sentera_id']}"
            field_id = plot_demeter_ids[field_name]

            date_maturity = datetime.strptime(row["date_maturity"], "%m/%d/%Y")
            date_observed = datetime.strptime(row["date_observed"], "%m/%d/%Y")

            # we cannot enter a date as `value_observed` so we convert to UNIX timestamp
            date_maturity_unix = get_unix_from_datetime(date_maturity)
            obs = Observation(
                field_id=field_id,
                unit_type_id=unit_type["maturity date"],
                observation_type_id=obs_type["maturity date"],
                value_observed=date_maturity_unix,
                date_observed=date_observed,
            )
            _ = insertOrGetObservation(cursor, obs)
        connect.commit()

        #  insert NDVI observations
        for _, row in df_ndvi.iterrows():
            field_name = f"{row['site']}_{row['sentera_id']}"
            field_id = plot_demeter_ids[field_name]

            # we cannot enter a date as `value_observed` so we enter it as `date_observed`
            obs = Observation(
                field_id=field_id,
                unit_type_id=unit_type["ndvi"],
                observation_type_id=obs_type["ndvi"],
                value_observed=row["ndvi_mean"],
                date_observed=row["date_observed"],
            )
            _ = insertOrGetObservation(cursor, obs)
        connect.commit()
    connect.close()
