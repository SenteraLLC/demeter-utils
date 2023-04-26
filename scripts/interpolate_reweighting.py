# %% Imports
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from numpy import datetime64
from pandas import DataFrame, read_csv
from scipy.interpolate import PchipInterpolator

from demeter_utils.temporal_inference import (
    assign_group_weights,
    get_datetime_skeleton,
    get_inference_fx_from_df_reference,
    get_mean_temporal_resolution,
    moving_average_weights,
    populate_fill_in_values,
    weighted_moving_average,
)

# %% Functions


def plot_ts(df_skeleton_final: DataFrame, df_line: DataFrame = None):
    colors = {True: "blue", False: "red"}
    plt.scatter(
        df_skeleton_final["datetime_skeleton"],
        df_skeleton_final["sample_value"],
        c=df_skeleton_final["true_data"].map(colors),
    )
    plt.xticks(rotation=60)

    if df_line is not None:
        plt.plot(df_line["datetime"].to_list(), df_line["weighted_mean"].to_list())

    plt.show()


def plot_new_weights(x, wts, source):
    colors = {True: "blue", False: "red"}
    plt.scatter(
        x,
        wts,
        c=source.map(colors),
    )
    plt.xticks(rotation=60)

    plt.show()


# %% Load the reference data and generate an infer_function

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

infer_function = get_inference_fx_from_df_reference(
    df_reference=df_gimms_ndvi,
    interp_type=PchipInterpolator,
)

# %% Example 1: Made up data

dates = [
    datetime(2022, 6, 6),
    datetime(2022, 6, 20),
    datetime(2022, 7, 25),
    datetime(2022, 9, 10),
]
values = [0.41, 0.55, 0.73, 0.54]
df_test = DataFrame(data={"date_start": dates, "sample_value": values})

temp_resolution = timedelta(days=10)

df_skeleton = get_datetime_skeleton(
    df_true=df_test,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    col_datetime="date_start",
    col_value="sample_value",
    temporal_resolution_min=temp_resolution,
    tolerance_alpha=0.5,
    recalibrate=True,
)

df_skeleton_full = populate_fill_in_values(
    df_skeleton=df_skeleton,
    infer_function=infer_function,
    col_value="sample_value",
    col_datetime="datetime_skeleton",
)
# plot_ts(df_skeleton_full)


df_skeleton_full["source"] = df_skeleton_full["true_data"].map(
    {False: "GIMMS", True: "Drone"}
)
group_weights = {"GIMMS": 0.10, "Drone": 1.0}
df_skeleton_full["weight"] = assign_group_weights(
    groups=df_skeleton_full["source"], group_weights=group_weights
)
bin_dt = df_skeleton_full["datetime_skeleton"]
values = df_skeleton_full["sample_value"]
weights = df_skeleton_full["weight"]
# OR
# group_weights = {False: 0.10, True: 1.0}
# df_skeleton_full["weight"] = assign_group_weights(groups=df_skeleton_full["true_data"], group_weights=group_weights)
step_size = temp_resolution
window_size = temp_resolution * 1.5

df_weighted_mean = weighted_moving_average(
    bin_dt, values, weights, step_size, window_size
)
plot_ts(df_skeleton_full, df_weighted_mean)

new_wts = moving_average_weights(bin_dt, weights, step_size, window_size)
plot_new_weights(
    new_wts["datetime"], new_wts["weights_moving_avg"], df_skeleton_full["true_data"]
)

# %% Example 2: Loaded data

df_true = read_csv("/mnt/c/Users/Tyler/Downloads/df_drone_imagery1.csv")
# df_true = read_csv("/Users/marissakivi/Desktop/df_drone_imagery1.csv")
df_true.rename(
    columns={"date_observed": "date_start", "value_observed": "sample_value"},
    inplace=True,
)
df_true["date_start"] = (df_true["date_start"]).astype(datetime64)

temporal_resolution_min = timedelta(days=15)
step_size = timedelta(days=10)
window_size = step_size * 1.5

df_skeleton = get_datetime_skeleton(
    df_true=df_true,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    col_datetime="date_start",
    col_value="sample_value",
    # temporal_resolution_min=None,
    temporal_resolution_min=temporal_resolution_min,
    tolerance_alpha=0.5,
    recalibrate=True,
)

df_skeleton_full = populate_fill_in_values(
    df_skeleton=df_skeleton,
    infer_function=infer_function,
    col_value="sample_value",
    col_datetime="datetime_skeleton",
)
# plot_ts(df_skeleton_full)

group_weights = {False: 0.10, True: 1.0}
df_skeleton_full["weight"] = assign_group_weights(
    groups=df_skeleton_full["true_data"], group_weights=group_weights
)
bin_dt = df_skeleton_full["datetime_skeleton"]
values = df_skeleton_full["sample_value"]
weights = df_skeleton_full["weight"]


df_weighted_mean = weighted_moving_average(
    bin_dt, values, weights, step_size, window_size
)
plot_ts(df_skeleton_full, df_weighted_mean)

new_wts = moving_average_weights(bin_dt, weights, step_size, window_size)
plot_new_weights(
    new_wts["datetime"], new_wts["weights_moving_avg"], df_skeleton_full["true_data"]
)

# %% Example 3: EOY data

dates = [
    datetime(2022, 7, 25),
    datetime(2022, 9, 10),
]
values = [0.73, 0.54]
df_test = DataFrame(data={"date_start": dates, "sample_value": values})

temp_resolution = timedelta(days=10)

df_skeleton = get_datetime_skeleton(
    df_true=df_test,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    col_datetime="date_start",
    col_value="sample_value",
    temporal_resolution_min=temp_resolution,
    tolerance_alpha=0.5,
    recalibrate=True,
)

df_skeleton_full = populate_fill_in_values(
    df_skeleton=df_skeleton,
    infer_function=infer_function,
    col_value="sample_value",
    col_datetime="datetime_skeleton",
)
# plot_ts(df_skeleton_full)

df_skeleton_full["source"] = df_skeleton_full["true_data"].map(
    {False: "GIMMS", True: "Drone"}
)
weights = {"GIMMS": 0.10, "Drone": 1.0}
df_skeleton_full["weight"] = assign_group_weights(
    groups=df_skeleton_full["source"], group_weights=group_weights
)
bin_dt = df_skeleton_full["datetime_skeleton"]
values = df_skeleton_full["sample_value"]
weights = df_skeleton_full["weight"]
step_size = temp_resolution
window_size = temp_resolution * 1.5

df_weighted_mean = weighted_moving_average(
    bin_dt, values, weights, step_size, window_size
)
plot_ts(df_skeleton_full, df_weighted_mean)

new_wts = moving_average_weights(bin_dt, weights, step_size, window_size)
plot_new_weights(
    new_wts["datetime"], new_wts["weights_moving_avg"], df_skeleton_full["true_data"]
)

# %% Example 4: Higher resolution
dates = [
    datetime(2022, 6, 6),
    datetime(2022, 6, 20),
    datetime(2022, 7, 25),
    datetime(2022, 9, 10),
]
values = [0.41, 0.55, 0.73, 0.54]
df_test = DataFrame(data={"date_start": dates, "sample_value": values})

temp_resolution = timedelta(days=5)

df_skeleton = get_datetime_skeleton(
    df_true=df_test,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    col_datetime="date_start",
    col_value="sample_value",
    temporal_resolution_min=temp_resolution,
    tolerance_alpha=0.5,
    recalibrate=True,
)

df_skeleton_full = populate_fill_in_values(
    df_skeleton=df_skeleton,
    infer_function=infer_function,
    col_value="sample_value",
    col_datetime="datetime_skeleton",
)
# plot_ts(df_skeleton_full)

df_skeleton_full["source"] = df_skeleton_full["true_data"].map(
    {False: "GIMMS", True: "Drone"}
)
weights = {"GIMMS": 0.10, "Drone": 1.0}
df_skeleton_full["weight"] = assign_group_weights(
    groups=df_skeleton_full["source"], group_weights=group_weights
)
bin_dt = df_skeleton_full["datetime_skeleton"]
values = df_skeleton_full["sample_value"]
weights = df_skeleton_full["weight"]
step_size = temp_resolution
window_size = temp_resolution * 1.5

df_weighted_mean = weighted_moving_average(
    bin_dt, values, weights, step_size, window_size
)
plot_ts(df_skeleton_full, df_weighted_mean)

new_wts = moving_average_weights(bin_dt, weights, step_size, window_size)
plot_new_weights(
    new_wts["datetime"], new_wts["weights_moving_avg"], df_skeleton_full["true_data"]
)


# %% Check the distribution of interpolated values and the observed values

# 1. Get average temporal resolution of "true" data
temporal_res_true = get_mean_temporal_resolution(
    df_skeleton_full, col_subset="true_data", col_date="datetime_skeleton", subset=True
)
temporal_res_fillin = get_mean_temporal_resolution(
    df_skeleton_full, col_subset="true_data", col_date="datetime_skeleton", subset=False
)
temporal_res_all = get_mean_temporal_resolution(
    df_skeleton_full, col_subset="true_data", col_date="datetime_skeleton", subset=None
)
