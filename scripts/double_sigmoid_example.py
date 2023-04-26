# %% Imports
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from numpy import arange, array, datetime64
from pandas import DataFrame, read_csv

from demeter_utils.temporal_inference import (
    dbl_sigmoid_function_1,
    dbl_sigmoid_function_2,
    get_datetime_skeleton_for_ts,
    populate_fill_in_values_1,
)

# TODO: Replace `populate_fill_in_values_1` with `populate_fill_in_values` in import and
# delete `populate_fill_in_values_1` from `__init__.py` once `dbl_sigmoid_function` can take `datetime` as input instead of `doy`

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

# to calculate `doy'
# NOTE: can be deleted when `dbl_sigmoid_functions` takes `datetime` as input
year = (df[col_datetime].dt.year).unique
date_year_start = datetime(2022, 1, 1)  # TODO: extract `year` from 'col_datetime'
df["doy_obs"] = (df[col_datetime] - date_year_start).dt.days

# TODO: Delete this if code works fine
# create a datetime skeleton for interpolation
# datetime_interp = arange(
#     datetime(2022, 6, 1), datetime(2022, 9, 30), timedelta(days=1)
# ).astype("datetime64[ns]")

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

    # remove rows where "sample_value" is NaN
    df_gimms_ndvi = df_gimms_ndvi[df_gimms_ndvi["sample_value"].notna()]

    # Convert the 'date_start' column to a datetime.datetime() object
    df_gimms_ndvi["date_start"] = (df_gimms_ndvi["date_start"]).astype(datetime64)

    # %% NOTE: This cell is extra chunk of code to add `doy_obs` column to `df_gimms_ndvi`. Can be deleted when
    # double sigmoid function can take datetime as input.
    year = (df_gimms_ndvi["date_start"].dt.year).unique
    date_year_start = datetime(2022, 1, 1)  # TODO: Get `year` value from the dataframe
    df_gimms_ndvi["doy_obs"] = (df_gimms_ndvi["date_start"] - date_year_start).dt.days

    # generate the infer_function
    infer_function = dbl_sigmoid_function_2(
        df=df_gimms_ndvi, col_datetime="doy_obs", col_value="sample_value", guess=None
    )

    # %% NOTE: col_datetime="datetime_skeleton" is converted to `doy_obs` for use here
    # TODO: This cell can be deleted once `dbl_sigmoid_function_2` can take `datetime` as input instead of `doy`

    year = (df_skeleton["datetime_skeleton"].dt.year).unique
    date_year_start = datetime(2022, 1, 1)
    df_skeleton["doy_obs"] = (
        df_skeleton["datetime_skeleton"] - date_year_start
    ).dt.days
    # %%

    # generate dataframe where NA values are replaced with values obtained using the `infer_function`
    df_complete = populate_fill_in_values_1(
        df_skeleton=df_skeleton,
        infer_function=infer_function,
        col_datetime="doy_obs",  # TODO: Check the df_skeleton for input
        col_value="value_observed",
    )

    # %% DEM 360
    # train a new infer_function on df_complete
    infer_function_new = dbl_sigmoid_function_2(
        df=df_complete,
        col_datetime="doy_obs",
        col_value="value_observed",
        guess=None,
    )

    # %% Plots
    # create a datetime skeleton for interpolation
    # TODO: delete doy_interp once the datetime_interp works
    # TODO: replace doy_interp with `datetime_inter` for plotting
    # TODO: replace `doy_obs` with `datetime_skeleton` for plotting
    doy_interp = arange(0, 365, 7)

    # datetime_interp = arange(
    #     datetime(2022, 3, 1), datetime(2022, 10, 31), timedelta(days=1)
    # ).astype("datetime64[ns]")

    value_interpolated = infer_function_new(doy_interp)

    plt.plot(doy_interp, value_interpolated, "--", label="interpolated")

    # Check the distribution of interpolated values and the observed values
    colors = {True: "blue", False: "red"}
    plt.scatter(
        df_complete["doy_obs"],
        df_complete["value_observed"],
        c=df_complete["true_data"].map(colors),
    )

    plt.legend()
    # plt.ylim(0, 1)
    plt.xticks(rotation=60)
    plt.show()
    plt.title(factor + 1)


######################################################
## Code cleaned above this
#######################################################

# %%
##################################################################
#### Sample Double sigmoid function [Based on datetime]
#### NOTE: Not working as expected, need work around `datetime`
##################################################################
datetime_obs = df_gimms_ndvi["date_start"]
value_obs = df_gimms_ndvi["sample_value"]

# some random model parameters
params = array(
    [
        0.22,
        0.8,
        0.035,
        int(datetime(2022, 6, 6).strftime("%Y%m%d")),
        # int(datetime(2022, 6, 6).strftime("%Y%m%d%H%M%S")),
        # to_numeric(Series(datetime(2022,6,6)))[0],
        0.045,
        int(datetime(2022, 8, 6).strftime("%Y%m%d"))
        # to_numeric(Series(datetime(2022,8,6)))[0],
    ]
)

# create a datetime skeleton for interpolation
datetime_pred = arange(
    datetime(2022, 3, 1), datetime(2022, 10, 30), timedelta(days=1)
).astype(datetime)

# convert the datetime to an integer
df_temp = DataFrame(datetime_pred, columns=["datetime_pred"])
df_temp["datetime_pred_int"] = df_temp.apply(
    lambda row: int((row.datetime_pred).strftime("%Y%m%d")),
    axis=1,
)
datetime_pred_int = df_temp["datetime_pred_int"]

# datetime_pred_int = to_numeric(Series(datetime_pred))
# df_temp["datetime_pred_int"] = df_temp.apply(
#     lambda row: (to_numeric(Series([row.datetime_pred])))[0],
#     axis=1,
# )

# t = to_numeric(Series(datetime(2022,7,6)))
# t = int(datetime(2022, 7, 6).strftime("%Y%m%d"))

# generate predicted values using the function
value_pred = dbl_sigmoid_function_1(params=params, t=datetime_pred_int)

# plot the observed data
plt.scatter(datetime_obs, value_obs, c="green")

# plot the predicted data
plt.plot(datetime_pred, value_pred, "--", label="Predicted")
plt.show()


###########################################
### Example use for `dbl_sigmoid_function_1
###########################################
# %%
parameters = [0.169, 1.1859, 0.069, 169.5442, 0.0228, 244.912]
doy_pred = arange(0, 365, 7)
values_pred = dbl_sigmoid_function_1(params=parameters, t=doy_pred)
# %%
