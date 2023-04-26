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

# TODO: Delete `populate_fill_in_values_1` from import as well as from `` `__init__.py` once `dbl_sigmoid_function` can take `datetime` as input instead of `doy`

###########################################
### Example use for `dbl_sigmoid_function_1
###########################################
# %% `
parameters = [0.169, 1.1859, 0.069, 169.5442, 0.0228, 244.912]
doy_pred = arange(0, 365, 7)
values_pred = dbl_sigmoid_function_1(params=parameters, t=doy_pred)


###########################################
### Example use for `dbl_sigmoid_function_2`
###########################################
# %%
# link to the data: https://sentera.sharepoint.com/:x:/s/demeter/EYyjTpLaVCNPqqnIMYc4H0sBdfkcZPmv7fhnONJ483R0FA?e=jTNm2m
# loading the data
df_true_in = read_csv("df_drone_imagery.csv")
df_full = df_true_in.copy()

# Convert the 'date_start' column to a datetime.datetime() object
df_full["date_observed"] = (df_full["date_observed"]).astype(datetime64)

# select column for factoring as well for `datetime` and `value`
col_factor = "field_id"
col_datetime = "date_observed"
col_value = "value_observed"

# NOTE: This is required only if factoring is required
# subsetting the data to include only 5 field_id and 1 `unit_type_id`
df_subset = df_full[df_full["unit_type_id"] == 1]
df = df_subset[(df_subset[col_factor].isin([1, 2, 3, 4, 5]))]

#  to calculate `doy' NOTE: can be deleted when `dbl_sigmoid_functions` takes `datetime` as input
year = (df[col_datetime].dt.year).unique
date_year_start = datetime(2022, 1, 1)  # TODO: extract `year` from 'col_datetime'
df["doy_obs"] = (df[col_datetime] - date_year_start).dt.days

# %%
###########################################
### CASE 1: train a function on true/observed data, predict values for each DOY, and generate a plot
###########################################

# NOTE: a for loop is used to account for factors in the dataframe
for factor in df[col_factor].unique():
    df_factor = df.loc[df[col_factor] == factor]

    # Obtain a double sigmoid function for a given data
    dbl_sigmoid_func = dbl_sigmoid_function_2(
        df=df_factor, col_datetime="doy_obs", col_value="value_observed", guess=None
    )

    # obtain the interpolated values based on the the function for each week of year
    doy_pred = arange(0, 365, 7)
    values_pred = dbl_sigmoid_func(doy_pred)

    # Plot the results
    plt.scatter(
        df_factor["doy_obs"], df_factor["value_observed"], c="green", label="observed"
    )
    plt.scatter(doy_pred, values_pred, c="orange", label="predicted")
    plt.legend()
    plt.title(factor)
    plt.show()

# %%
###########################################
### CASE 2: Obtain a `df_skeleton`, train a function on reference data, replace NaN values in df_skeleton from reference dataset,
### train a function on complete true and false dataset, generate new reference values and replace NaN values in df_skeleton,
### train a function on new complete true and false dataset, predict values for each DOY, and generate plots
###########################################

for factor in df[col_factor].unique():
    df_factor = df.loc[df[col_factor] == factor]

    # generate a skeleton dataframe with `col_value` for each datetime where available and NA where `col_value` are unavailable
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

    # %%

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
    # generate a dataframe where NA values in the input df are replaced with values obtained using the `infer_function`
    df_skeleton_final = populate_fill_in_values_1(
        df_skeleton=df_skeleton,
        infer_function=infer_function,
        col_datetime="doy_obs",  # TODO: Check the df_skeleton for input
        col_value="value_observed",
    )

    # re-train the infer_function on `df_skeleton_final`
    infer_function_2 = dbl_sigmoid_function_2(
        df=df_skeleton_final,
        col_datetime="doy_obs",
        col_value="value_observed",
        guess=None,
    )

    # generate another dataframe where NA values in the input df `df_skeleton` are replaced with values obtained using the `infer_function_2`
    df_skeleton_final_2 = populate_fill_in_values_1(
        df_skeleton=df_skeleton,
        infer_function=infer_function_2,
        col_datetime="doy_obs",
        col_value="value_observed",
    )

    # re-train the infer_function on `df_skeleton_final_2`
    infer_function_3 = dbl_sigmoid_function_2(
        df=df_skeleton_final_2,
        col_datetime="doy_obs",
        col_value="value_observed",
        guess=None,
    )

    # obtain the interpolated values based on the `infer_function_3`
    doy_pred = arange(0, 365, 7)
    values_pred_infer3 = infer_function_3(doy_pred)
    values_pred_infer2 = infer_function_2(doy_pred)
    values_pred_infer = infer_function(doy_pred)

    # Plot the results
    plt.plot(doy_pred, values_pred_infer3, "--", label="predicted_3")
    plt.plot(doy_pred, values_pred_infer2, "--", label="predicted_2")
    plt.plot(doy_pred, values_pred_infer, "--", label="predicted")
    plt.plot(
        df_gimms_ndvi["doy_obs"], df_gimms_ndvi["sample_value"], "--", label="reference"
    )

    colors = {True: "blue", False: "red"}
    plt.scatter(
        df_skeleton_final_2["doy_obs"],
        df_skeleton_final_2["value_observed"],
        c=df_skeleton_final_2["true_data"].map(colors),
    )
    plt.legend()
    plt.title(factor)
    plt.show()

######################################################
## Code cleaned above this
#######################################################

# %%
##################################################################
#### Double sigmoid function [Based on datetime]
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
