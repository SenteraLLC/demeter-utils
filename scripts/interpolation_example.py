# %% Imports
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from pandas import DataFrame, Series, read_csv, to_numeric
from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

from demeter_utils.temporal_inference import (
    get_datetime_skeleton_for_ts,
    populate_fill_in_values,
    train_inference_from_df,
)

# NOTE: Steps 0, 1, and 2 are same as in `fill_in_example.py`
# %% Step 0: Generate dataframe from dem-357

# load/generate a `as-available` dataframe
dates = [
    datetime(2022, 4, 1),
    datetime(2022, 5, 13),
    datetime(2022, 6, 20),
    datetime(2022, 8, 15),
]
values = [0.25, 0.32, 0.50, 0.65]
df_test = DataFrame(data={"date_start": dates, "sample_value": values})

# generate a skeleton dataframe with `sample_values` for each datetime where available and NA where `sample_values` are unavailable
df_skeleton = get_datetime_skeleton_for_ts(
    df_true_data=df_test,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    temporal_resolution=timedelta(days=7),
    tolerance_alpha=0.5,
    col_datetime="date_start",
    col_value="sample_value",
    recalibrate=False,
)

# %% Step 1: Load the reference data and generate an infer_function

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

# %% Step 2: generate dataframe where NA values are replaced with values obtained using the `infer_function`

df_skeleton_final = populate_fill_in_values(
    df_skeleton=df_skeleton, infer_function=infer_function
)

# %% Step 3: interpolate the values in `df_skeleton_final` obtained from Step 2 with a new interpolation function

interp_function = train_inference_from_df(
    df_reference=df_skeleton_final,
    interp_type=PchipInterpolator,
    col_datetime="datetime_skeleton",
    col_value="sample_value",
)

# to obtain interpolated value for a single datetime
interp_function(to_numeric(Series([datetime(2022, 10, 31)])))[0]

# to obtain interpolated value for a list of datetime
datetime_ls = [
    datetime(2022, 5, 31),
    datetime(2022, 6, 22),
    datetime(2022, 6, 29),
    datetime(2022, 7, 15),
]

interp_value = []
for x in datetime_ls:
    value = interp_function(to_numeric(Series([x])))[0]
    interp_value.append(value)

# to obtain interpolated value for a complete df
df_skeleton_new = get_datetime_skeleton_for_ts(
    df_true_data=df_skeleton_final,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    temporal_resolution=timedelta(days=1),  # temporal resolution of 1 day
    tolerance_alpha=0.5,
    col_datetime="datetime_skeleton",
    col_value="sample_value",
    recalibrate=True,
)

df_interpolated = populate_fill_in_values(
    df_skeleton=df_skeleton_new,
    infer_function=interp_function,
    col_datetime="datetime_skeleton",
    col_value="sample_value",
)

# %% Check the distribution of interpolated values and the observed values
colors = {True: "blue", False: "red"}
plt.scatter(
    df_interpolated["datetime_skeleton"],
    df_interpolated["sample_value"],
    c=df_interpolated["true_data"].map(colors),
)
plt.xticks(rotation=60)
plt.legend(colors)
plt.show()

# %% Testing final interpolation with different interpolation function

interp_type = [Akima1DInterpolator, CubicSpline, PchipInterpolator]

interp_functions = []
for i in interp_type:
    interp_function = train_inference_from_df(
        df_reference=df_skeleton_final,
        interp_type=i,
        col_datetime="datetime_skeleton",
        col_value="sample_value",
    )
    interp_functions.append(interp_function)

dfs_interpolated = []
for i in interp_functions:
    df_interpolated = populate_fill_in_values(
        df_skeleton=df_skeleton_new,
        infer_function=i,
        col_datetime="datetime_skeleton",
        col_value="sample_value",
    )
    dfs_interpolated.append(df_interpolated)

df_interpolated_akima = dfs_interpolated[0]
df_interpolated_cubicspline = dfs_interpolated[1]
df_interpolated_pchip = dfs_interpolated[2]

# %% Plots
datetime_interp = df_interpolated_akima["datetime_skeleton"]

plt.plot(datetime_interp, df_interpolated_akima["sample_value"], "-", label="Akima1D")
plt.plot(
    datetime_interp, df_interpolated_cubicspline["sample_value"], "--", label="spline"
)
plt.plot(datetime_interp, df_interpolated_pchip["sample_value"], "-.", label="pchip1")
plt.legend()
plt.title("Interpolated Graph")
plt.ylim(0, 1)
plt.xticks(rotation=60)
plt.show()
