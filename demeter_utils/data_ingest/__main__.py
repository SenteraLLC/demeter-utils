"""
Script to load and organize Customer data from Local File Directory (customer data files) and CloudVault.

Required environment variables (add to the .env file):
# Connection to Sentera API
SENTERA_EMAIL="first.last@sentera.com"
SENTERA_PROD_PW="my_s3cret_passw0rd"
SENTERA_API_PROD_URL="https://api.sentera.com"

# Store to local directory
DEMETER_DIR="/mnt/d/"

# Choose the CloudVault Organization and Assets to query
ORG_SENTERA_ID="zuhhzlj_OR_b2muTheMosaic_CV_prod_faffd93_210608_225434"
ASSET_SENTERA_ID="{'01ABG-IN-INUS':'3tbdeyl_AS_b2muTheMosaic_CV_prod_8864cee1_230524_213525','02ABG-SD-SDUS':'t0xm2ob_AS_b2muTheMosaic_CV_prod_3b36d337_230509_200217','03ATC-WIUS':'v7vbcx4_AS_b2muTheMosaic_AD_45r99qc0_73d05f3b0_20230529_195111','04ALR-IAUS':'zhgliiv_AS_b2muTheMosaic_CV_prod_8864cee1_230524_204238','05CSM-ILUS':'tfi5mh3_AS_b2muTheMosaic_AD_45r99qc0_73d05f3b0_20230530_220619','06NEA-NEUS':'vhv4tyg_AS_b2muTheMosaic_CV_prod_3b36d337_230509_201119','07SRC-KSUS':'1hlnmpz_AS_b2muTheMosaic_CV_prod_7f0de7fa_230425_185154','08TRE-ARUS':'2xs6862_AS_b2muTheMosaic_CV_prod_7f0de7fa_230426_035714','09VET-MNUS':'g9xrnvl_AS_b2muTheMosaic_CV_prod_3b36d337_230509_201946','10NEL-MOUS':'e9733ki_AS_b2muTheMosaic_AD_qzr1p013_f1789ac10_20230413_165619',}"

To run: `poetry run python3 -m demeter_utils.data_ingest --date_on_or_after 2023-05-01 --analytic_name "Plot Multispectral Indices and Uniformity and Masking" --project_name "mosaic/phase3_stats"`
"""
import argparse
import logging
from ast import literal_eval
from datetime import datetime
from os import getenv
from os.path import join
from pathlib import Path

from dotenv import load_dotenv
from geopandas import GeoDataFrame
from gql.transport.requests import log as requests_logger
from pandas import DataFrame
from pandas import merge as pd_merge
from pandas import read_csv
from utils.logging.tqdm import logging_init

from demeter_utils.data_ingest._collect_data import load_field_insights_data

# This reduces the verosity of `gql` logging
requests_logger.setLevel(logging.WARNING)

if __name__ == "__main__":
    """Main function."""
    c = load_dotenv()
    logging_init()
    parser = argparse.ArgumentParser(
        description="Compiles data from Local File Directory and CloudVault for all sites, surveys, and plots, converting from wide to long format."
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
        help='Project name (Local File Directory path between "projects" and "data").',
        default="mosaic/phase3_stats",
    )

    args = parser.parse_args()
    date_on_or_after = datetime.strptime(args.date_on_or_after, "%Y-%m-%d")
    analytic_name = args.analytic_name
    filepath_exp_design = args.filepath_exp_design
    project_name = args.project_name

    date_on_or_after = datetime(2023, 5, 1)
    analytic_name = "Plot Multispectral Indices and Uniformity and Masking"
    filepath_exp_design = ""
    project_name = "mosaic/phase3_stats"

    # TODO: Option to add to a database
    # database_host = args.database_host
    # database_env = args.database_env
    # ssh_env_name = f"SSH_DEMETER_{database_host}" if database_host == "AWS" else None
    # database_env_name = f"DEMETER-{database_env}_{database_host}"

    PRIMARY_KEYS = ["site_name", "plot_id"]

    logging.info("Reading Environment variables from .env")
    analytic_fname = analytic_name.replace(" ", "_").lower()
    data_dir = join(
        str(getenv("DEMETER_DIR")), "projects", project_name, "data", analytic_fname
    )
    ORG_SENTERA_ID = getenv("ASSET_SENTERA_ID")
    ASSET_SENTERA_ID = literal_eval(getenv("ASSET_SENTERA_ID"))
    SENTERA_EMAIL = getenv("SENTERA_EMAIL")
    SENTERA_PROD_PW = getenv("SENTERA_PROD_PW")
    SENTERA_API_PROD_URL = getenv("SENTERA_API_PROD_URL")
    if (
        any(
            [
                data_dir,
                ORG_SENTERA_ID,
                ASSET_SENTERA_ID,
                SENTERA_EMAIL,
                SENTERA_PROD_PW,
                SENTERA_API_PROD_URL,
            ]
        )
        is None
    ):
        raise RuntimeError("Environment variables not set.")

    Path(data_dir).mkdir(parents=True, exist_ok=True)

    logging.info("Collecting data from CloudVault")
    gdf_plots, df_long = load_field_insights_data(
        asset_sentera_id_dict=ASSET_SENTERA_ID,
        date_on_or_after=date_on_or_after,
        analytic_name=analytic_name,
        primary_keys=PRIMARY_KEYS,
    )

    logging.info("Saving df_long.%s to Local File Directory...", "parquet")
    df_long.to_parquet(join(data_dir, f"df_long-{analytic_fname}.parquet"))

    logging.info("Collecting field data from Local File Directory")
    try:
        df_exp_design = read_csv(filepath_exp_design)

        logging.info(
            "Merging Plot IDs from CloudVault with Experimental Design data from Local File Directory"
        )
        gdf_exp_design = GeoDataFrame(
            pd_merge(df_exp_design, gdf_plots, on=PRIMARY_KEYS, how="inner"),
            geometry=gdf_plots.geometry.name,
            crs=gdf_plots.crs,
        )

    except:  # noqa: E722
        # TODO: Beef this try/except up
        df_exp_design = DataFrame()
        gdf_exp_design = gdf_plots.copy()

    if (len(df_exp_design) != len(gdf_plots)) | (len(gdf_exp_design) != len(gdf_plots)):
        # raise RuntimeError("Number of plots from experimental design CSVs and CloudVault GEOJSONs do not match.")
        logging.warning(
            "Number of plots from experimental design CSVs and CloudVault GEOJSONs do not match."
        )

    logging.info("Saving gdf_exp_design.%s to Local File Directory...", "parquet")
    gdf_exp_design.to_parquet(
        join(data_dir, f"gdf_exp_design-{analytic_fname}.parquet")
    )

    logging.info("Saving gdf_exp_design.%s to Local File Directory...", "geojson")
    gdf_exp_design.to_file(
        join(data_dir, f"gdf_exp_design-{analytic_fname}.geojson"), driver="GeoJSON"
    )

    # TODO: Load into database
    # logging.info("Connecting to database.")
    # conn = getConnection(env_name=database_env_name, ssh_env_name=ssh_env_name)

    # logging.info("Inserting data to database.")
    # insert_data(conn, gdf_plot, file_metadata, df_ndvi)

    # conn.close()

    logging.info("Import complete.")
