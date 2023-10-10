"""
Script to load and organize Customer data from Local File Directory (customer data files) and CloudVault.

Required environment variables (add to the .env file):
# Connection to Sentera API
SENTERA_EMAIL="first.last@sentera.com"
SENTERA_PROD_PW="my_s3cret_passw0rd"
SENTERA_API_PROD_URL="https://api.sentera.com"

# Store to local directory
DEMETER_DIR="//172.25.0.20/Sentera/Departments/GIS/demeter"

# Choose the CloudVault Organization and Assets to query
ORG_SENTERA_ID="zuhhzlj_OR_b2muTheMosaic_CV_prod_faffd93_210608_225434"
ASSET_SENTERA_ID="{'01ABG-IN-INUS':'3tbdeyl_AS_b2muTheMosaic_CV_prod_8864cee1_230524_213525','02ABG-SD-SDUS':'t0xm2ob_AS_b2muTheMosaic_CV_prod_3b36d337_230509_200217','03ATC-WIUS':'v7vbcx4_AS_b2muTheMosaic_AD_45r99qc0_73d05f3b0_20230529_195111','04ALR-IAUS':'zhgliiv_AS_b2muTheMosaic_CV_prod_8864cee1_230524_204238','05CSM-ILUS':'tfi5mh3_AS_b2muTheMosaic_AD_45r99qc0_73d05f3b0_20230530_220619','06NEA-NEUS':'vhv4tyg_AS_b2muTheMosaic_CV_prod_3b36d337_230509_201119','07SRC-KSUS':'1hlnmpz_AS_b2muTheMosaic_CV_prod_7f0de7fa_230425_185154','08TRE-ARUS':'2xs6862_AS_b2muTheMosaic_CV_prod_7f0de7fa_230426_035714','09VET-MNUS':'g9xrnvl_AS_b2muTheMosaic_CV_prod_3b36d337_230509_201946','10NEL-MOUS':'e9733ki_AS_b2muTheMosaic_AD_qzr1p013_f1789ac10_20230413_165619',}"

To run: `poetry run python3 -m demeter_utils.cli.download_field_insights_data --date_on_or_after 2023-05-01 --analytic_name "Plot Multispectral Indices and Uniformity and Masking" --project_name "mosaic/phase3_stats"`
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
from pandas import concat as pd_concat
from utils.logging.tqdm import logging_init

from demeter_utils.data_ingest.cloudvault._long import load_field_insights_data

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
        "--cols_ignore",
        type=str,
        help="List of column names to ignore when converting from wide to long. See `demeter_utils.data_ingest.cloudvault.load_field_insights_data() for more information.",
        default="['num_rows','stroke','stroke-opacity','fill','fill-opacicity',]",
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
    project_name = args.project_name
    cols_ignore = literal_eval(args.cols_ignore)

    # date_on_or_after = datetime(2023, 5, 1)
    # analytic_name = "Plot Multispectral Indices and Uniformity and Masking"
    # project_name = "mosaic/phase3_stats"
    # cols_ignore = [
    #     "num_rows",
    #     "stroke",
    #     "stroke-opacity",
    #     "fill",
    #     "fill-opacity",
    #     "Trial Name",
    #     "mean_x",
    #     "mean_y",
    #     "Treatment",
    #     "id",
    #     "left",
    #     "top",
    #     "right",
    #     "bottom",
    # ]

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
    gdf_plots = GeoDataFrame()
    df_long = DataFrame()
    for asset_name, asset_sentera_id in ASSET_SENTERA_ID.items():
        gdf_plots_temp, df_long_temp = load_field_insights_data(
            asset_name=asset_name,
            asset_sentera_id=asset_sentera_id,
            date_on_or_after=date_on_or_after,
            analytic_name=analytic_name,
            primary_keys=PRIMARY_KEYS,
            cols_ignore=cols_ignore,
        )

        gdf_plots = (
            pd_concat([gdf_plots, gdf_plots_temp], axis=0, ignore_index=True)
            if len(gdf_plots_temp.columns) != 0
            else gdf_plots_temp.copy()
        )
        df_long = (
            pd_concat([df_long, df_long_temp], axis=0, ignore_index=True)
            if len(df_long_temp.columns) != 0
            else df_long_temp.copy()
        )
    gdf_plots.drop_duplicates(subset=PRIMARY_KEYS, inplace=True)
    df_long.drop_duplicates(inplace=True)

    # TODO: Check if any gdf_plots columns are empty. If so, issue a warning suggesting that user adds to `cols_ignore`
    cols_sparse = gdf_plots.columns[gdf_plots.isna().any()].tolist()
    if len(cols_sparse) > 0:
        logging.warning(
            '  `gdf_plots` contains columns with sparse data - consider passing "%s" to the `cols_ignore` arg.',
            ",".join(cols_sparse),
        )

    logging.info("Field Insights retrieval complete.")
    logging.info("    %s record(s) retrieved", format(len(df_long), ","))
    logging.info(
        "  %s unique observation type(s)",
        format(len(df_long.drop_duplicates(subset=["observation_type"])), ","),
    )
    logging.info(
        "  %s unique descriptive statistic(s)",
        format(len(df_long.drop_duplicates(subset=["statistic_type"])), ","),
    )
    logging.info(
        "  %s unique observation x statistics combination(s)",
        format(
            len(df_long.drop_duplicates(subset=["observation_type", "statistic_type"])),
            ",",
        ),
    )
    logging.info("  %s unique site(s)", format(len(df_long["site_name"].unique()), ","))
    logging.info(
        "  %s unique collection(s)",
        format(len(df_long.drop_duplicates(subset=["site_name", "date"])), ","),
    )
    logging.info(
        "  %s unique plot(s)",
        format(len(df_long.drop_duplicates(subset=["site_name", "plot_id"])), ","),
    )

    logging.info(
        "Saving %s to Local File Directory...", f"df_long-{analytic_fname}.parquet"
    )
    df_long.to_parquet(join(data_dir, f"df_long-{analytic_fname}.parquet"))

    logging.info(
        "Saving %s to Local File Directory...",
        f"gdf_exp_design-{analytic_fname}.parquet",
    )
    gdf_plots.to_parquet(join(data_dir, f"gdf_exp_design-{analytic_fname}.parquet"))
    logging.info(
        "Saving %s to Local File Directory...",
        f"gdf_exp_design-{analytic_fname}.geojson",
    )
    gdf_plots.to_file(
        join(data_dir, f"gdf_exp_design-{analytic_fname}.geojson"), driver="GeoJSON"
    )
