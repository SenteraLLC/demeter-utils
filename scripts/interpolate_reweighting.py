# %% Imports
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import read_csv, to_datetime
from scipy.interpolate import BSpline, splrep

from demeter_utils.time import convert_dt_to_unix, get_timedelta_days
from demeter_utils.time_series.interpolate import (
    assign_group_weights,
    weighted_moving_average,
)

col_dt = "date_observed"
col_value = "value_observed"
step_size = timedelta(days=14)
window_size = timedelta(days=10)
group_weights = {"GIMMS": 0.05, "drone": 1.0}


# %% Visualization functions


def spline_smoothing(df_wtd_mean: DataFrame, s: float = None) -> DataFrame:
    # create daily time series for plotting smoothing splines
    date_start = df_wtd_mean["t"].min()
    date_end = df_wtd_mean["t"].max()
    n_days = abs(int(get_timedelta_days(date_start=date_start, date_end=date_end)))
    x_hat = [date_start + (idx * timedelta(days=1)) for idx in range(n_days + 1)]

    # convert all datetimes to unix
    xt_hat = [convert_dt_to_unix(xval, relative_epoch=date_start) for xval in x_hat]
    xt = [
        convert_dt_to_unix(xval, relative_epoch=date_start) for xval in df_wtd_mean["t"]
    ]

    splines = splrep(x=xt, y=df_wtd_mean["y"], s=s)
    fx = BSpline(*splines)
    y = fx(xt_hat)

    return DataFrame(data={"t": x_hat, "y": y})


def plot_ts(df: DataFrame, df_wtd: DataFrame = None, df_splines: DataFrame = None):
    colors = {"drone": "black", "GIMMS": "red"}
    plt.scatter(
        df[col_dt],
        df[col_value],
        c=df["source"].map(colors),
    )
    plt.xticks(rotation=60)

    if df_wtd is not None:
        plt.plot(df_wtd["t"].to_list(), df_wtd["y"].to_list(), c="black")

    if df_splines is not None:
        plt.plot(df_splines["t"].to_list(), df_splines["y"].to_list(), "--", c="black")

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
df_gimms_clean = df_gimms_ndvi.rename(
    columns={"date_start": col_dt, "sample_value": col_value}
).dropna(subset=[col_value], axis=0)
df_gimms_clean[col_dt] = to_datetime(df_gimms_clean[col_dt])
df_gimms_clean.insert(0, "source", "GIMMS")

# %% Example 1: Made up data
dates = [
    datetime(2022, 6, 6),
    datetime(2022, 6, 20),
    datetime(2022, 7, 25),
    datetime(2022, 9, 10),
]
values = [0.41, 0.55, 0.73, 0.54]
df_test = DataFrame(
    data={
        "source": ["drone"] * len(dates),
        col_dt: dates,
        col_value: values,
    }
)

df_full = pd_concat([df_gimms_clean[df_test.columns], df_test], axis=0)


df_full["weight"] = assign_group_weights(
    groups=df_full["source"], group_weights=group_weights
)

df_weighted_mean = weighted_moving_average(
    t=df_full[col_dt],
    y=df_full[col_value],
    step_size=step_size,
    window_size=window_size,
    weights=df_full["weight"],
)

df_splines = spline_smoothing(df_weighted_mean)

plot_ts(df_full, df_splines=df_splines)

# %% Example 2: Loaded data

# df_true = read_csv("/mnt/c/Users/Tyler/Downloads/df_drone_imagery1.csv")
df_true = read_csv(
    "/Users/marissakivi/Desktop/df_drone_imagery1.csv", parse_dates=[col_dt]
)
df_true.insert(0, "source", "drone")
df_full = pd_concat([df_gimms_clean, df_true[["source", col_dt, col_value]]], axis=0)


df_full["weight"] = assign_group_weights(
    groups=df_full["source"], group_weights=group_weights
)
df_weighted_mean = weighted_moving_average(
    t=df_full[col_dt],
    y=df_full[col_value],
    step_size=step_size,
    window_size=window_size,
    weights=df_full["weight"],
)

df_splines = spline_smoothing(df_weighted_mean)

plot_ts(df_full, df_splines=df_splines)


# %% Example 3: EOY data

dates = [
    datetime(2022, 7, 25),
    datetime(2022, 9, 10),
]
values = [0.73, 0.54]
df_test = DataFrame(data={col_dt: dates, col_value: values})
df_test.insert(0, "source", "drone")

df_full = pd_concat([df_gimms_clean[df_test.columns], df_test], axis=0)

df_full["weight"] = assign_group_weights(
    groups=df_full["source"], group_weights=group_weights
)

df_weighted_mean = weighted_moving_average(
    t=df_full[col_dt],
    y=df_full[col_value],
    step_size=step_size,
    window_size=window_size,
    weights=df_full["weight"],
)

df_splines = spline_smoothing(df_weighted_mean)

plot_ts(df_full, df_splines=df_splines)

# %%
