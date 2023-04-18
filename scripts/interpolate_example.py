# %%
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from pandas import DataFrame, read_csv
from scipy.interpolate import Akima1DInterpolator

from demeter_utils.interpolate._interpolate import (
    generate_fill_in_values,
    get_datetime_skeleton_for_ts,
)

# from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator


# %% psuedo-inference function
def fx(dt: datetime) -> float:
    day = dt.day
    return (1 / 200) * (day - 31) * (-day - 1)


colors = {True: "blue", False: "red"}
ends = [datetime(2022, 1, 1), datetime(2022, 1, 31)]


def plot_and_compare(df_test: DataFrame):
    df = get_datetime_skeleton_for_ts(
        df_true_data=df_test,
        datetime_start=ends[0],
        datetime_end=ends[1],
        temporal_resolution=timedelta(days=2),
        tolerance_alpha=0.5,
        col_datetime="date",
        col_value="value",
        recalibrate=True,
    )
    value = [1] * len(df)

    df_recalibrate = get_datetime_skeleton_for_ts(
        df_true_data=df_test,
        datetime_start=ends[0],
        datetime_end=ends[1],
        temporal_resolution=timedelta(days=2),
        tolerance_alpha=0.5,
        col_datetime="date",
        col_value="value",
        recalibrate=False,
    )
    value_recalibrate = [2] * len(df_recalibrate)
    plt.scatter(df["datetime_skeleton"], value, c=df["within_tolerance"].map(colors))
    plt.scatter(
        df_recalibrate["datetime_skeleton"],
        value_recalibrate,
        c=df["within_tolerance"].map(colors),
    )
    plt.axvline(x=ends[0], c="black")
    plt.axvline(x=ends[1], c="black")
    plt.ylim(0, 3)
    plt.xticks(rotation=60)
    plt.show()


# %% test case 1: all dates within range, no observed dates matched on start or end date
dates = [
    datetime(2022, 1, 5),
    datetime(2022, 1, 14),
    datetime(2022, 1, 20),
    datetime(2022, 1, 28),
]
values = [fx(dt) for dt in dates]
df_test = DataFrame(data={"date": dates, "value": values})

plot_and_compare(df_test)

# %% test case 2: earliest date before start date and latest date inside date range
dates = [
    datetime(2021, 12, 31),
    datetime(2022, 1, 14),
    datetime(2022, 1, 20),
    datetime(2022, 1, 28),
]
values = [fx(dt) for dt in dates]
df_test = DataFrame(data={"date": dates, "value": values})

plot_and_compare(df_test)

# %% test case 3: start date on inside of range
dates = [
    datetime(2022, 1, 2),
    datetime(2022, 1, 14),
    datetime(2022, 1, 20),
    datetime(2022, 1, 28),
]
values = [fx(dt) for dt in dates]
df_test = DataFrame(data={"date": dates, "value": values})

plot_and_compare(df_test)

# %% test case 4: start and end date on inside of range
dates = [
    datetime(2022, 1, 2),
    datetime(2022, 1, 14),
    datetime(2022, 1, 20),
    datetime(2022, 1, 30),
]
values = [fx(dt) for dt in dates]
df_test = DataFrame(data={"date": dates, "value": values})

plot_and_compare(df_test)

# %% test case 5: start and end date on outside of range
dates = [
    datetime(2021, 12, 31),
    datetime(2022, 1, 14),
    datetime(2022, 1, 20),
    datetime(2022, 2, 1),
]
values = [fx(dt) for dt in dates]
df_test = DataFrame(data={"date": dates, "value": values})

plot_and_compare(df_test)

# %% test: only 2 dates, both within datetime_start/end
dates = [
    datetime(2022, 1, 14),
    datetime(2022, 1, 20, 18),
]
values = [fx(dt) for dt in dates]
df_test = DataFrame(data={"date": dates, "value": values})

plot_and_compare(df_test)

# %%


# %% Test for function `generate_fill_in_values`

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
    "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2019&start_month=1&num_months=12&format=csv"
)
text = StringIO(req.content.decode("utf-8"))
df_gimms_ndvi = read_csv(text, skiprows=14).rename(columns=GIMMS_COLS)[
    GIMMS_COLS.values()
]


# using `generate_fill_in_values` to generate df_interp
df_reference_interp = df_interp = generate_fill_in_values(
    df_reference_ndvi=df_gimms_ndvi,
    datetime_start=datetime(2019, 5, 6),
    datetime_end=datetime(2019, 10, 1),
    temporal_resolution=timedelta(days=1),
    interp_function=Akima1DInterpolator,
)
# %%
