# %%
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from pandas import DataFrame, read_csv
from scipy.interpolate import PchipInterpolator

from demeter_utils.temporal_inference import (
    get_datetime_skeleton_for_ts,
    get_inference_fx_from_df_reference,
    populate_fill_in_values,
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

#######################################################################

# %% Test for function `generate_fill_in_values`
# %% Step 0: Generate dataframe from dem-357

dates = [
    datetime(2022, 4, 1),
    datetime(2022, 5, 13),
    datetime(2022, 6, 20),
    datetime(2022, 8, 15),
]
values = [0.25, 0.32, 0.50, 0.65]
df_test = DataFrame(data={"date_start": dates, "sample_value": values})

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

infer_function = get_inference_fx_from_df_reference(
    df_reference=df_gimms_ndvi,
    interp_type=PchipInterpolator,
)

# %% Step 2: generate dataframe from dem-358

df_skeleton_final = populate_fill_in_values(
    df_skeleton=df_skeleton, infer_function=infer_function
)

# %% Check the distribution of interpolated values and the observed values
df_final = df_skeleton_final
colors = {True: "blue", False: "red"}
plt.scatter(
    df_final["datetime_skeleton"],
    df_final["sample_value"],
    c=df_final["true_data"].map(colors),
)
plt.xticks(rotation=60)
plt.show()
