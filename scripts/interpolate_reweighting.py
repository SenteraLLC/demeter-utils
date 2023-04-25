# %% Imports
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from numpy import average, ceil, datetime64, exp, max, min, power, zeros
from pandas import DataFrame, Series, Timedelta, read_csv, to_datetime
from scipy.interpolate import PchipInterpolator

from demeter_utils.temporal_inference import (
    get_datetime_skeleton,
    get_inference_fx_from_df_reference,
    get_mean_temporal_resolution,
    populate_fill_in_values,
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
        plt.plot(df_line["x"].to_list(), df_line["y"].to_list())

    plt.show()


def gaussian(x, mu, sig):
    return exp(-power(x - mu, 2.0) / (2 * power(sig, 2.0)))


def _make_datetime_to_unix_with_origin(dt: datetime, origin: datetime) -> int:
    tdelta = (to_datetime(dt) - to_datetime(origin)) // Timedelta("1s")
    return tdelta


def _make_unix_with_origin_to_datetime(unix: int, origin: datetime) -> int:
    tdelta = unix * Timedelta("1s")
    dt = to_datetime(origin) + tdelta
    return dt.to_pydatetime()


def plot_weighted_moving_average(
    x: Series, y: Series, weights: Series, step_size: timedelta, window_size: timedelta
):
    # Convert all temporal arguments into units of seconds since x.min()
    xt = [_make_datetime_to_unix_with_origin(dt, origin=x.min()) for dt in x]
    step_size_t = step_size // Timedelta("1s")
    window_size_t = window_size // Timedelta("1s")

    # Get the bin centers
    num_steps = ceil((max(xt) - min(xt)) / step_size_t)
    bin_centers = [min(xt) + (step_size_t * idx) for idx in range(int(num_steps))]

    weighted_mean = zeros(len(bin_centers))
    sum_weights = zeros(len(x))

    for index in range(len(bin_centers)):
        bin_center = bin_centers[index]

        # For each bin, we first want to weight the values based on their distance from `bin_center`
        # We can use a Gaussian distribution for this where the standard deviation is 1/2 * `window_size`
        gaussian_wts = [
            gaussian(xt_value, mu=bin_center, sig=window_size_t / 2) for xt_value in xt
        ]

        # Then, we use our preset weights to adjust the weight even further
        combined_weights = [
            gaussian_wts[ind] * weights.to_list()[ind] for ind in range(len(xt))
        ]

        total_wt = sum(combined_weights)
        sum_weights = [
            sum_weights[ind] + (combined_weights[ind] / total_wt)
            for ind in range(len(x))
        ]

        # Then, take the weighted average
        weighted_mean[index] = average(y, weights=combined_weights)

    # convert time back
    bin_centers_dt = [
        _make_unix_with_origin_to_datetime(bin_center, x.min())
        for bin_center in bin_centers
    ]

    df_line = DataFrame(
        data={
            "x": bin_centers_dt,
            "y": weighted_mean,
        }
    )
    plot_ts(df_skeleton_full, df_line)

    return [wt / len(bin_centers) for wt in sum_weights]


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
weights = {"GIMMS": 0.10, "Drone": 1.0}
df_skeleton_full["weight"] = df_skeleton_full["source"].map(weights)

new_wts = plot_weighted_moving_average(
    x=df_skeleton_full["datetime_skeleton"],
    y=df_skeleton_full["sample_value"],
    weights=df_skeleton_full["weight"],
    step_size=temp_resolution,
    window_size=temp_resolution * 1.5,
)

plot_new_weights(
    df_skeleton_full["datetime_skeleton"],
    new_wts,
    df_skeleton_full["true_data"],
)

# %% Example 2: Loaded data

df_true = read_csv("/Users/marissakivi/Desktop/df_drone_imagery1.csv")
df_true.rename(
    columns={"date_observed": "date_start", "value_observed": "sample_value"},
    inplace=True,
)
df_true["date_start"] = (df_true["date_start"]).astype(datetime64)

df_skeleton = get_datetime_skeleton(
    df_true=df_true,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    col_datetime="date_start",
    col_value="sample_value",
    temporal_resolution_min=timedelta(days=15),
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
df_skeleton_full["weight"] = df_skeleton_full["source"].map(weights)

new_wts = plot_weighted_moving_average(
    x=df_skeleton_full["datetime_skeleton"],
    y=df_skeleton_full["sample_value"],
    weights=df_skeleton_full["weight"],
    step_size=temp_resolution,
    window_size=temp_resolution * 1.5,
)

plot_new_weights(
    df_skeleton_full["datetime_skeleton"],
    new_wts,
    df_skeleton_full["true_data"],
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
df_skeleton_full["weight"] = df_skeleton_full["source"].map(weights)

new_wts = plot_weighted_moving_average(
    x=df_skeleton_full["datetime_skeleton"],
    y=df_skeleton_full["sample_value"],
    weights=df_skeleton_full["weight"],
    step_size=temp_resolution,
    window_size=temp_resolution * 1.5,
)

plot_new_weights(
    df_skeleton_full["datetime_skeleton"],
    new_wts,
    df_skeleton_full["true_data"],
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
df_skeleton_full["weight"] = df_skeleton_full["source"].map(weights)

new_wts = plot_weighted_moving_average(
    x=df_skeleton_full["datetime_skeleton"],
    y=df_skeleton_full["sample_value"],
    weights=df_skeleton_full["weight"],
    step_size=temp_resolution,
    window_size=temp_resolution * 2,
)

plot_new_weights(
    df_skeleton_full["datetime_skeleton"],
    new_wts,
    df_skeleton_full["true_data"],
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
