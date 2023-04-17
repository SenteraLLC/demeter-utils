# %%
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
from pandas import DataFrame

from demeter_utils.interpolate._interpolate import find_fill_in_dates


# %% psuedo-inference function
def fx(dt: datetime) -> float:
    day = dt.day
    return (1 / 200) * (day - 31) * (-day - 1)


colors = {True: "blue", False: "red"}
ends = [datetime(2022, 1, 1), datetime(2022, 1, 31)]


def plot_and_compare(df_test: DataFrame):
    df = find_fill_in_dates(
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

    df_recalibrate = find_fill_in_dates(
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


# %% test case 1: missing start and end dates
dates = [
    datetime(2022, 1, 5),
    datetime(2022, 1, 14),
    datetime(2022, 1, 20),
    datetime(2022, 1, 28),
]
values = [fx(dt) for dt in dates]
df_test = DataFrame(data={"date": dates, "value": values})

plot_and_compare(df_test)

# %% test case 2: start date on outside of range
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

# %% test:
dates = [
    datetime(2022, 1, 14),
    datetime(2022, 1, 20, 18),
]
values = [fx(dt) for dt in dates]
df_test = DataFrame(data={"date": dates, "value": values})

plot_and_compare(df_test)

# %%
