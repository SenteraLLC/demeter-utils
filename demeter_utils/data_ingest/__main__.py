"""
Script to load and organize Customer data from Odyssey (customer data files) and CloudVault.

To run: `poetry run python3 -m demeter_utils.data_ingest --date_on_or_after 2023-05-01 --analytic_name "Plot Multispectral Indices and Uniformity"`
"""

# TODO: This is currently picking up some `gql` INFO messaging with the logger. Might want to remove that.

import argparse
import logging
from ast import literal_eval
from datetime import datetime
from os import getenv
from os.path import join

from dotenv import load_dotenv
from geopandas import GeoDataFrame
from pandas import merge as pd_merge
from pandas import read_csv
from utils.logging.tqdm import logging_init

from demeter_utils.data_ingest._collect_data import load_field_insights_data

if __name__ == "__main__":
    """Main function."""
    c = load_dotenv()
    logging_init()
    parser = argparse.ArgumentParser(
        description="Load and organize FIRST Seed Tests data from Odyssey and CloudVault."
    )
    parser.add_argument(
        "--date_on_or_after",
        type=str,
        help="Earliest date to load data from.",
        default="2023-05-01",
    )
    parser.add_argument(
        "--analytic_name",
        type=str,
        help="Name of analytic to load from CloudVault.",
        default="Plot Multispectral Indices and Uniformity",
    )
    parser.add_argument(
        "--filepath_exp_design",
        type=str,
        help="Filename containing the `df_exp_design` information.",
    )
    # parser.add_argument(
    #     "--database_host",
    #     type=str,
    #     help="Host of demeter database; can be 'AWS' or 'LOCAL'.",
    #     default="LOCAL",
    # )
    # parser.add_argument(
    #     "--database_env",
    #     type=str,
    #     help="Database instance; can be 'DEV' or 'PROD'.",
    #     default="DEV",
    # )

    # date_on_or_after = datetime(2023, 5, 1)
    # analytic_name = "Plot Multispectral Indices and Uniformity"
    # database_host = "LOCAL"
    # database_env = "DEV"

    # set up args
    args = parser.parse_args()
    date_on_or_after = datetime.strptime(args.date_on_or_after, "%Y-%m-%d")
    analytic_name = args.analytic_name
    filepath_exp_design = args.filepath_exp_design
    # database_host = args.database_host
    # database_env = args.database_env

    # ssh_env_name = f"SSH_DEMETER_{database_host}" if database_host == "AWS" else None
    # database_env_name = f"DEMETER-{database_env}_{database_host}"

    logging.info("Collecting field data from Odyssey")
    primary_keys = ["site_name", "plot_id"]

    df_exp_design = read_csv(filepath_exp_design)

    logging.info("Collecting multispectral data from CloudVault")
    # TODO: How to load in the asset_sentera_id_dict?
    ASSET_SENTERA_ID = literal_eval(getenv("ASSET_SENTERA_ID"))
    gdf_plots, df_long = load_field_insights_data(
        asset_sentera_id_dict=ASSET_SENTERA_ID,
        date_on_or_after=date_on_or_after,
        analytic_name=analytic_name,
    )

    gdf_exp_design = GeoDataFrame(
        pd_merge(df_exp_design, gdf_plots, on=primary_keys, how="inner"),
        geometry=gdf_plots.geometry.name,
        crs=gdf_plots.crs,
    )
    # gdf_exp_design[pd.isnull(gdf_exp_design.geometry)]
    if (len(df_exp_design) != len(gdf_plots)) | (len(gdf_exp_design) != len(gdf_plots)):
        raise RuntimeError(
            "Number of plots from experimental design CSVs and CloudVault GEOJSONs do not match."
        )

    # df_long_meta = pd_merge(
    #     gdf_exp_design.loc[:, gdf_exp_design.columns != gdf_exp_design.geometry.name],
    #     df_long,
    #     on=primary_keys,
    #     how="inner",
    # )

    data_dir = join(str(getenv("DEMETER_DIR")), "projects/mosaic_co_stats/data")

    logging.info("Saving gdf_exp_design.%s to Odyssey...", "parquet")
    gdf_exp_design.to_parquet(join(data_dir, "gdf_exp_design.parquet"))

    logging.info("Saving gdf_exp_design.%s to Odyssey...", "geojson")
    gdf_exp_design.to_file(join(data_dir, "gdf_exp_design.geojson"), driver="GeoJSON")

    logging.info("Saving df_long.%s to Odyssey...", "parquet")
    df_long.to_parquet(join(data_dir, "df_long.parquet"))

    # TODO: Load into database
    # logging.info("Connecting to database.")
    # conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    # logging.info("Inserting data to database.")
    # insert_data(conn, gdf_plot, file_metadata, df_ndvi)

    # conn.close()

    logging.info("Import complete.")
