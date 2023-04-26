# %% Imports
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from numpy import arange, datetime64
from pandas import read_csv
from scipy.interpolate import PchipInterpolator

from demeter_utils.temporal_inference import (
    get_datetime_skeleton_for_ts,
    populate_fill_in_values,
    train_inference_from_df,
)

# %% Testing on real data
# link to data: https://sentera.sharepoint.com/:x:/s/demeter/ESR0PKnkjQBIkYDkT9NBVS8B1h5kJHTbJE2tCLgM7QWP7A?e=Vtcdlw

########################################################
### Given a "complete" dataset for the desired temporal resolution (likely includes both "true" and "fill-in"),
### fit a new interpolation function on the "complete" dataset and return values for each datetime required.
########################################################
# %% DEM 357
# loading the data
df_true_in = read_csv("df_drone_imagery.csv")
df_full = df_true_in.copy()

# Convert the 'date_start' column to a datetime.datetime() object
df_full["date_observed"] = (df_full["date_observed"]).astype(datetime64)

# select column for factoring
col_factor = "field_id"
col_datetime = "date_observed"
col_value = "value_observed"

# NOTE: This chunk is required only if factoring is required
# subsetting the data to include only 5 field_id and 1 `unit_type_id`
df_subset = df_full[df_full["unit_type_id"] == 1]
df = df_subset[(df_subset[col_factor].isin([1, 2, 3, 4, 5]))]

# create a datetime skeleton for interpolation
# NOTE: Had an issue with `dtype = datetime64[ns] and datetime64[us]` conversion
# issue resolved by using `.astype('datetime64[ns]) at the end`
datetime_interp = arange(
    datetime(2022, 6, 1), datetime(2022, 9, 30), timedelta(days=1)
).astype("datetime64[ns]")

# %% DEM 358
# use of factor to account for each field_id.
for factor in df[col_factor].unique():
    df_factor = df.loc[df[col_factor] == factor]

    # generate a skeleton dataframe with `sample_values` for each datetime where available and NA where `sample_values` are unavailable
    df_skeleton = get_datetime_skeleton_for_ts(
        df_true_data=df_factor,
        datetime_start=datetime(2022, 3, 1),
        datetime_end=datetime(2022, 10, 31),
        temporal_resolution=timedelta(days=7),
        tolerance_alpha=0.5,
        col_datetime="date_observed",
        col_value="value_observed",
        recalibrate=True,
    )

    # load the standard/reference data
    # TODO: Use the function by Marissa
    GIMMS_COLS = {
        "START DATE": "date_start",
        "END DATE": "date_end",
        "SAMPLE VALUE": "sample_value",
        "SAMPLE COUNT": "n_pixels_sample",
        "MEAN VALUE": "mean_hist_value",
        "MIN VALUE": "min_hist_value",
        "MAX VALUE": "max_hist_value",
    }
    req = requests.get(
        "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2022&start_month=1&num_months=12&format=csv"
    )
    text = StringIO(req.content.decode("utf-8"))
    df_gimms_ndvi = read_csv(text, skiprows=14).rename(columns=GIMMS_COLS)[
        GIMMS_COLS.values()
    ]

    # generate the infer_function
    infer_function = train_inference_from_df(
        df_reference=df_gimms_ndvi,
        interp_type=PchipInterpolator,
    )

    # generate dataframe where NA values are replaced with values obtained using the `infer_function`
    df_complete = populate_fill_in_values(
        df_skeleton=df_skeleton,
        infer_function=infer_function,
        col_datetime="datetime_skeleton",
        col_value="value_observed",
    )

    # %% DEM 360
    # train a new infer_function on df_complete
    infer_function_new = train_inference_from_df(
        df_reference=df_complete,
        interp_type=PchipInterpolator,
        col_datetime="datetime_skeleton",
        col_value="value_observed",
    )

    # %% Plots
    # create a datetime skeleton for interpolation
    datetime_interp = arange(
        datetime(2022, 3, 1), datetime(2022, 10, 31), timedelta(days=1)
    ).astype("datetime64[ns]")

    value_interpolated = infer_function_new(datetime_interp)

    plt.plot(datetime_interp, value_interpolated, "--", label="interpolated")

    # True data points
    plt.plot(df_factor["date_observed"], df_factor["value_observed"], "o", label="True")

    # True and Infered data points
    plt.plot(
        df_complete["datetime_skeleton"],
        df_complete["value_observed"],
        "v",
        label="True + Inferred",
    )

    plt.legend()
    # plt.ylim(0, 1)
    plt.xticks(rotation=60)
    plt.show()
    plt.title(factor + 1)
