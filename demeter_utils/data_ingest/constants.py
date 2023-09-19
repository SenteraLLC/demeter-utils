"""Constant values to be used throughout data ingestion process for Phase 1."""
from datetime import datetime, timedelta

ORG_SENTERA_ID = "hnowyaq_OR_uoqbSaskatoon_CV_prod_877281e_210429_163958"

ASSET_SENTERA_ID = {
    "SKKI": "6eacned_AS_uoqbSaskatoon_CV_prod_22f00be_210614_201847",
    "SKAB": "arpl1ru_AS_uoqbSaskatoon_CV_prod_22f00be_210614_201844",
    "SKKR": "lsg6qgi_AS_uoqbSaskatoon_CV_prod_22f00be_210614_201847",
    "SKRO": "ev71kb6_AS_uoqbSaskatoon_CV_prod_22f00be_210614_201845",
}

CSV_COLUMN_NAMES = {
    "SenteraID": "sentera_id",
    "Date of Maturity": "date_maturity",
    "Planting_date": "date_planted",
    "Date maturity note was taken": "date_observed",
    "Date Maturity note was taken": "date_observed",
}

# For some unknown reason, this collection does not have "published" images according to GraphQL
# so image query returns an empty dataframe.
# The timestamp given here is based on one image in the collection: "965192274_IMG_00001_785.jpg"
IMAGE_DATE_HACK = {
    "uc1fwmm_CO_uoqbSaskatoon_CV_prod_16c4a2b_210819_153555": datetime(
        2021, 8, 18, 20, 7, 5
    )
}

# All image timestamps were incorrectly converted to UTC time twice. Since all locations are -6 hours
# from UTC according to CloudVault, we can use the following time delta to convert these image timestamps back
# to true UTC time.
TIMEDELTA_HACK = timedelta(hours=-6)
