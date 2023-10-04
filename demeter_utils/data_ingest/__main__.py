"""
Script to load and organize Customer data from Odyssey (customer data files) and CloudVault.

To run: `poetry run python3 -m mosaic_co_stats.data_ingest --date_on_or_after 2023-05-01 --analytic_name "Plot Multispectral Indices and Uniformity and Masking" --project_name "mosaic/phase3_stats"`
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
from gql.transport.requests import log as requests_logger
from pandas import DataFrame
from pandas import merge as pd_merge
from pandas import read_csv
from utils.logging.tqdm import logging_init

from demeter_utils.data_ingest._collect_data import load_field_insights_data

requests_logger.setLevel(logging.WARNING)
if __name__ == "__main__":
    """Main function."""
    c = load_dotenv()
    logging_init()
    parser = argparse.ArgumentParser(
        description="Compiles data from Odyssey and CloudVault for all sites, surveys, and plots, converting from wide to long format."
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
        default="",
    )
    parser.add_argument(
        "--project_name",
        type=str,
        help='Project name (Odyssey directory path between "projects" and "data").',
        default="mosaic/phase3_stats",
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
    # # analytic_name = "Plot Multispectral Indices and Uniformity"
    # analytic_name = "Plot Multispectral Indices and Uniformity and Masking"
    # filepath_exp_design = join(
    #     str(getenv("DEMETER_DIR")),
    #     "projects/first_seed_tests/data/seed_id - Cannon Falls.csv",
    # )
    # project_name = "mosaic/phase3_stats"

    # set up args
    args = parser.parse_args()
    date_on_or_after = datetime.strptime(args.date_on_or_after, "%Y-%m-%d")
    analytic_name = args.analytic_name
    filepath_exp_design = args.filepath_exp_design
    project_name = args.project_name
    # database_host = args.database_host
    # database_env = args.database_env

    # ssh_env_name = f"SSH_DEMETER_{database_host}" if database_host == "AWS" else None
    # database_env_name = f"DEMETER-{database_env}_{database_host}"

    primary_keys = ["site_name", "plot_id"]

    logging.info("Collecting data from CloudVault")
    # TODO: How to load in the asset_sentera_id_dict?
    ASSET_SENTERA_ID = literal_eval(getenv("ASSET_SENTERA_ID"))
    gdf_plots, df_long = load_field_insights_data(
        asset_sentera_id_dict=ASSET_SENTERA_ID,
        date_on_or_after=date_on_or_after,
        analytic_name=analytic_name,
        primary_keys=primary_keys,
    )

    data_dir = join(str(getenv("DEMETER_DIR")), "projects", project_name, "data")

    logging.info("Saving df_long.%s to Odyssey...", "parquet")
    df_long.to_parquet(
        join(data_dir, f"df_long-{analytic_name.replace(' ', '_').lower()}.parquet")
    )

    logging.info("Collecting field data from Odyssey")
    try:
        df_exp_design = read_csv(filepath_exp_design)

        logging.info(
            "Merging Plot IDs from CloudVault with Experimental Design data from Odyssey"
        )
        gdf_exp_design = GeoDataFrame(
            pd_merge(df_exp_design, gdf_plots, on=primary_keys, how="inner"),
            geometry=gdf_plots.geometry.name,
            crs=gdf_plots.crs,
        )

    except:  # noqa: E722
        # TODO: Beef this try/except up
        df_exp_design = DataFrame()
        gdf_exp_design = gdf_plots.copy()

    # gdf_exp_design[pd.isnull(gdf_exp_design.geometry)]
    if (len(df_exp_design) != len(gdf_plots)) | (len(gdf_exp_design) != len(gdf_plots)):
        # raise RuntimeError("Number of plots from experimental design CSVs and CloudVault GEOJSONs do not match.")
        logging.warning(
            "Number of plots from experimental design CSVs and CloudVault GEOJSONs do not match."
        )

    logging.info("Saving gdf_exp_design.%s to Odyssey...", "parquet")
    gdf_exp_design.to_parquet(join(data_dir, "gdf_exp_design.parquet"))

    logging.info("Saving gdf_exp_design.%s to Odyssey...", "geojson")
    gdf_exp_design.to_file(join(data_dir, "gdf_exp_design.geojson"), driver="GeoJSON")

    # TODO: Load into database
    # logging.info("Connecting to database.")
    # conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    # logging.info("Inserting data to database.")
    # insert_data(conn, gdf_plot, file_metadata, df_ndvi)

    # conn.close()

    logging.info("Import complete.")
