# %% Imports
from datetime import datetime
from io import StringIO

import matplotlib.pyplot as plt
import requests
from numpy import datetime64
from pandas import DataFrame, read_csv
from scipy.interpolate import PchipInterpolator

from demeter_utils.temporal_inference import (
    get_datetime_skeleton,
    get_inference_fx_from_df_reference,
    get_mean_temporal_resolution,
    populate_fill_in_values,
)


# %% Functions
def plot_ts(df_skeleton_final: DataFrame):
    colors = {True: "blue", False: "red"}
    plt.scatter(
        df_skeleton_final["datetime_skeleton"],
        df_skeleton_final["sample_value"],
        c=df_skeleton_final["true_data"].map(colors),
    )
    plt.xticks(rotation=60)
    plt.show()


# %% Example 1: Made up data

dates = [
    datetime(2022, 6, 6),
    datetime(2022, 6, 20),
    datetime(2022, 7, 3),
    datetime(2022, 7, 25),
    datetime(2022, 9, 10),
]
values = [0.41, 0.55, 0.68, 0.73, 0.51]
df_test = DataFrame(data={"date_start": dates, "sample_value": values})

df_skeleton = get_datetime_skeleton(
    df_true=df_test,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    col_datetime="date_start",
    col_value="sample_value",
    temporal_resolution_min=None,
    tolerance_alpha=0.5,
    recalibrate=True,
)

# %% Example 2: Loaded data
df_true = read_csv("/mnt/c/Users/Tyler/Downloads/df_drone_imagery1.csv")
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
    temporal_resolution_min=None,
    tolerance_alpha=0.5,
    recalibrate=True,
)

# %% Step 1: Load the reference data and generate an infer_function

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

# %% Step 2: Populate full dataframe

df_skeleton_full = populate_fill_in_values(
    df_skeleton=df_skeleton,
    infer_function=infer_function,
    col_value="sample_value",
    col_datetime="datetime_skeleton",
)
plot_ts(df_skeleton_full)

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


# %%
