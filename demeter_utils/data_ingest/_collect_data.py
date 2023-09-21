import logging
from datetime import datetime

from geopandas import GeoDataFrame
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import merge as pd_merge

from demeter_utils.data_ingest._utils import (
    get_cv_connection,
    get_ndvi_plot_ratings_for_asset,
)
from demeter_utils.data_ingest.constants import ASSET_SENTERA_ID


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
