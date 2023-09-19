"""Script to gather and insert Bayer Canola Phase 1 data into the `demeter` database.

This script currently only populates field groups, fields (plots) with geometries, and planting dates.

Data is sourced from local CSV files of customer field notes, as well as CloudVault.
"""

# TODO: This is currently picking up some `gql` INFO messaging with the logger. Might want to remove that.
# TODO: Fix the fiona error. I think this is a result of the source file and not the process.

import argparse
import logging

from demeter.db import getConnection
from dotenv import load_dotenv
from utils.logging.tqdm import logging_init

from demeter_utils.data_ingest._collect_data import collect_data, collect_ndvi_data
from demeter_utils.data_ingest._insert_data import insert_data

if __name__ == "__main__":
    """Main function."""
    c = load_dotenv()
    logging_init()

    parser = argparse.ArgumentParser(
        description="Collect and insert Bayer Canola data into Demeter schema."
    )

    parser.add_argument(
        "--database_host",
        type=str,
        help="Host of demeter database; can be 'AWS' or 'LOCAL'.",
        default="LOCAL",
    )

    parser.add_argument(
        "--database_env",
        type=str,
        help="Database instance; can be 'DEV' or 'PROD'.",
        default="DEV",
    )

    # set up args
    args = parser.parse_args()
    database_host = args.database_host
    database_env = args.database_env

    ssh_env_name = f"SSH_DEMETER_{database_host}" if database_host == "AWS" else None
    database_env_name = f"DEMETER-{database_env}_{database_host}"

    logging.info("Collecting field data")
    gdf_plot, file_metadata = collect_data()

    logging.info("Collecting NDVI data from CloudVault")
    df_ndvi = collect_ndvi_data(gdf_plot=gdf_plot)

    logging.info("Connecting to database.")
    conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    logging.info("Inserting data to database.")
    insert_data(conn, gdf_plot, file_metadata, df_ndvi)

    conn.close()

    logging.info("Import complete.")
