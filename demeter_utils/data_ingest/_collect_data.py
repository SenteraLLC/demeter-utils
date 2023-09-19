import logging
from datetime import datetime
from os import getenv
from os.path import join
from typing import Tuple

from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import merge as pd_merge
from pandas import read_csv

from demeter_utils.data_ingest._utils import (
    get_cv_connection,
    get_ndvi_plot_ratings_for_asset,
    get_plot_boundaries_for_asset,
)
from demeter_utils.data_ingest.constants import ASSET_SENTERA_ID, CSV_COLUMN_NAMES


def collect_ndvi_data(gdf_plot: GeoDataFrame) -> DataFrame:
    """Collect and organize NDVI Plot Ratings data for the Bayer Canola Phase 1 project.

    Extract data from all available NDVI Plot Ratings feature sets after the earliest planting date for each site.

    Gathered data and data sources:
    1. NDVI Plot Ratings
    --- Plot-level mean NDVI from drone imagery

    Args:
        gdf_plot: See collect_data() below for more information.

    Returns:
        df_ndvi (DataFrame) long-format dataframe with plot-level mean NDVI values from all available NDVI Plot
            Ratings geojson files
    """
    logging.info("   Getting observed NDVI plot ratings from CloudVault")
    client, ds = get_cv_connection()

    df_obs = None
    for asset_name in ASSET_SENTERA_ID.keys():
        print(asset_name)

        # get earliest planting date
        inds = gdf_plot["site"] == asset_name
        date_plant_earliest = (
            gdf_plot.loc[inds, "date_planted"]
            .map(lambda d: datetime.strptime(d, "%m/%d/%Y"))
            .min()
        )
        asset_id = ASSET_SENTERA_ID[asset_name]

        # get observed plot-level mean NDVI from CloudVault
        df_obs_temp = get_ndvi_plot_ratings_for_asset(
            client, ds, asset_id, date_plant_earliest
        )
        df_obs_temp.insert(0, "site", asset_name)

        if df_obs is None:
            df_obs = df_obs_temp.copy()
        else:
            df_obs = pd_concat([df_obs, df_obs_temp], axis=0, ignore_index=True)

    # the new script for processing plot ratings does not include Sentera ID :(
    complete = df_obs["sentera_id"] > 0
    df_sentera_id = df_obs.loc[
        complete, ["site", "range", "column", "sentera_id"]
    ].drop_duplicates()

    len_check = len(df_obs)
    df_obs = pd_merge(
        df_obs.drop(columns=["sentera_id"]),
        df_sentera_id,
        on=["site", "range", "column"],
    )
    assert len(df_obs) == len_check, "Re-merge for Sentera ID didn't work."

    df_reduced = pd_merge(
        df_obs,
        gdf_plot[["site", "sentera_id", "range", "column"]],
        on=["site", "sentera_id", "range", "column"],
    )

    return df_reduced


def collect_data() -> Tuple[GeoDataFrame, dict]:
    """Collect and organize raw data for the Bayer Canola Phase 1 project.

    From CloudVault, earliest available “NDVI Plot Ratings” GeoJSON for an asset after planting is used.

    Not all plots at the locations have available planting dates, so those are removed and not inserted into
    the database.

    Gathered data and data sources:
    1. Field trial site/location (CSV)
    2. Plots
    --- Plot IDs (CSV and GeoJSON)
    --- Plot boundaries (GeoJSON)
    --- Planting date (CSV)
    --- Maturity date (CSV)
    --- Date observed for maturity info (CSV)

    Returns:
        gdf_plot (GeoDataFrame) geodataframe containing plot metadata for all sites, including geometry, plot location
            information (i.e., range, column), site, sentera ID, maturity date, planting date and date of maturity
            observation

        file_metadata (dict) Dictionary that maps each site to the GeoJSON file on CloudVault that was used to inform
            plot boundaries; to be saved with the site-level field group to ensure reproducibility
    """
    logging.info("   Collecting and cleaning field notes data from CSV files")
    demeter_dir = str(getenv("DEMETER_DIR"))
    data_dir = join(demeter_dir, "projects/bayer_canola/phase1/data/field_notes")

    id_columns = ["site", "sentera_id", "range", "column"]
    df = None
    for asset_name in ASSET_SENTERA_ID.keys():
        fname_csv = join(data_dir, f"{asset_name}.csv")
        df_temp = read_csv(fname_csv)

        # clean up and standardize
        df_temp.rename(columns=CSV_COLUMN_NAMES, inplace=True)
        df_temp.insert(0, "site", asset_name)
        df_temp = df_temp[
            id_columns + ["date_planted", "date_maturity", "date_observed"]
        ]

        if df is None:
            df = df_temp.copy()
        else:
            df = pd_concat([df, df_temp], axis=0, ignore_index=True)

    logging.info("   Getting plot geometries from CloudVault")
    client, ds = get_cv_connection()

    gdf = None
    file_metadata = {}

    for asset_name in ASSET_SENTERA_ID.keys():
        print(asset_name)

        # get earliest planting date
        inds = df["site"] == asset_name
        date_plant_earliest = (
            df.loc[inds, "date_planted"]
            .map(lambda d: datetime.strptime(d, "%m/%d/%Y"))
            .min()
        )

        # get plot boundaries
        asset_id = ASSET_SENTERA_ID[asset_name]
        gdf_temp, file_metadata_temp = get_plot_boundaries_for_asset(
            client, ds, asset_id, date_plant_earliest
        )
        gdf_temp.insert(0, "site", asset_name)

        if gdf is None:
            gdf = gdf_temp.copy()
        else:
            gdf = pd_concat([gdf, gdf_temp], axis=0, ignore_index=True)

        file_metadata[asset_name] = file_metadata_temp

    gdf["sentera_id"] = gdf["sentera_id"].astype(int)
    gdf_plot = pd_merge(gdf, df, on=["site", "sentera_id", "range", "column"])

    msg = "Range/column values do not line up between files."
    assert len(gdf_plot) == len(df), msg

    return gdf_plot, file_metadata
