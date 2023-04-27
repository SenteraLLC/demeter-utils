# %% Imports
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from pandas import DataFrame, read_csv
from scipy.interpolate import PchipInterpolator

from demeter_utils.temporal_inference import (
    get_datetime_skeleton_for_ts,
    populate_fill_in_values,
    train_inference_from_df,
)

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

infer_function = train_inference_from_df(
    df_reference=df_gimms_ndvi,
    interp_type=PchipInterpolator,
)

# %% Step 2: generate dataframe from dem-358

df_skeleton_final = populate_fill_in_values(
    df_skeleton=df_skeleton, infer_function=infer_function
)

# %% Check the distribution of interpolated values and the observed values
colors = {True: "blue", False: "red"}
plt.scatter(
    df_skeleton_final["datetime_skeleton"],
    df_skeleton_final["sample_value"],
    c=df_skeleton_final["true_data"].map(colors),
)
plt.xticks(rotation=60)
plt.show()
# %%
