# %% Imports
from datetime import datetime, timedelta
from io import StringIO
from typing import Callable

import matplotlib.pyplot as plt
import requests
from numpy import ceil
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import read_csv, to_datetime
from scipy.interpolate import UnivariateSpline

from demeter_utils.time import convert_dt_to_unix
from demeter_utils.time_series.interpolate import weighted_moving_average


# %% Visualization function
def _plot_ndvi_fx(df: DataFrame, df_line: DataFrame, pipeline: Callable):
    """Create colored scatterplot of NDVI data with smoothed weighted mean curve shown in red dashed line.

    Args:
        df (DataFrame): Dataframe containing NDVI data, with columns "date_observed", "value_observed", and
            "source".

        df_line (DataFrame): Dataframe containing weighted moving average bins (t) and mean values (y).

        pipeline (Callable): Fitted smoothing spline function to `df_line` which also performs conversion from
            datetime to unix.
    """
    # get x values for plotting at daily resolution
    num_steps = int(ceil((df_line["t"].max() - df_line["t"].min()) / timedelta(days=1)))
    x = [df_line["t"].min() + (idx * timedelta(days=1)) for idx in range(num_steps + 1)]
    y = pipeline(x)
    df_fit = DataFrame(data={"x": x, "y": y})

    # create plot
    colors = {"drone": "black", "GIMMS": "green", "Sentinel-2": "red"}
    fig = plt.figure()

    for label in colors.keys():
        df_color_subset = df.loc[df["source"] == label]
        if len(df_color_subset) > 0:
            plt.scatter(
                df_color_subset["date_observed"],
                df_color_subset["value_observed"],
                c=colors[label],
                label=label,
            )
    plt.xticks(rotation=60)
    plt.plot(
        df_fit["x"].to_list(), df_fit["y"].to_list(), c="red", label="fitted NDVI curve"
    )
    plt.legend()

    return fig


# %% Load data

col_dt = "date_observed"
col_value = "value_observed"

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
df = pd_concat([df_gimms_clean[df_test.columns], df_test], axis=0).sort_values(
    by=col_dt
)

step_size = timedelta(days=14)
window_size = timedelta(days=10)
group_weights = {"GIMMS": 0.05, "drone": 1.0}
col_group = "source"
wts = df[col_group].map(group_weights)

# get weighted moving average
df_line = weighted_moving_average(
    t=df[col_dt],
    y=df[col_value],
    step_size=step_size,
    window_size=window_size,
    weights=wts,
)
xt = convert_dt_to_unix(df_line["t"], relative_epoch=df_line["t"].min())

# fit univariate smoothing spline to weighted mean curves
# `s` represents smoothing factor (i.e., upper limit for sum of squared errors), let's use 5% of mean value
fx = UnivariateSpline(x=xt, y=df_line["y"], s=df[col_value].mean() * 0.05)


def pipeline(dt) -> float:
    t = convert_dt_to_unix(dt, relative_epoch=df_line["t"].min())
    return fx(t)


fig = _plot_ndvi_fx(df=df, df_line=df_line, pipeline=pipeline)
fig.show()


# %% Example 2: Loaded data
df_true = read_csv(
    "/Users/marissakivi/Desktop/df_drone_imagery1.csv", parse_dates=[col_dt]
)
df_true["date_observed"] = to_datetime(df_true["date_observed"])
df_true.insert(0, "source", "drone")

df = pd_concat(
    [df_gimms_clean[df_test.columns], df_true[df_test.columns]], axis=0
).sort_values(by=col_dt)

step_size = timedelta(days=14)
window_size = timedelta(days=10)
group_weights = {"GIMMS": 0.05, "drone": 1.0}
col_group = "source"
wts = df[col_group].map(group_weights)

# get weighted moving average
df_line = weighted_moving_average(
    t=df[col_dt],
    y=df[col_value],
    step_size=step_size,
    window_size=window_size,
    weights=wts,
)
xt = convert_dt_to_unix(df_line["t"], relative_epoch=df_line["t"].min())

# fit univariate smoothing spline to weighted mean curves
# `s` represents smoothing factor (i.e., upper limit for sum of squared errors), let's use 5% of mean value
fx = UnivariateSpline(x=xt, y=df_line["y"], s=df[col_value].mean() * 0.05)


def pipeline(dt) -> float:
    t = convert_dt_to_unix(dt, relative_epoch=df_line["t"].min())
    return fx(t)


fig = _plot_ndvi_fx(df=df, df_line=df_line, pipeline=pipeline)
fig.show()

# %% Example 3: EOY data
dates = [
    datetime(2022, 7, 25),
    datetime(2022, 9, 10),
]
values = [0.73, 0.54]
df_test = DataFrame(data={col_dt: dates, col_value: values})
df_test.insert(0, "source", "drone")

df = pd_concat([df_gimms_clean[df_test.columns], df_test], axis=0).sort_values(
    by=col_dt
)

step_size = timedelta(days=14)
window_size = timedelta(days=10)
group_weights = {"GIMMS": 0.05, "drone": 1.0}
col_group = "source"
wts = df[col_group].map(group_weights)

# get weighted moving average
df_line = weighted_moving_average(
    t=df[col_dt],
    y=df[col_value],
    step_size=step_size,
    window_size=window_size,
    weights=wts,
)
xt = convert_dt_to_unix(df_line["t"], relative_epoch=df_line["t"].min())

# fit univariate smoothing spline to weighted mean curves
# `s` represents smoothing factor (i.e., upper limit for sum of squared errors), let's use 5% of mean value
fx = UnivariateSpline(x=xt, y=df_line["y"], s=df[col_value].mean() * 0.05)


def pipeline(dt) -> float:
    t = convert_dt_to_unix(dt, relative_epoch=df_line["t"].min())
    return fx(t)


fig = _plot_ndvi_fx(df=df, df_line=df_line, pipeline=pipeline)
fig.show()

# %%
