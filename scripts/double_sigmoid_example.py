# %% Imports
from datetime import datetime, timedelta
from io import StringIO

import matplotlib.pyplot as plt
import requests
from numpy import arange
from pandas import Timedelta, read_csv, to_datetime

from demeter_utils.temporal_inference import (
    fit_double_sigmoid,
    get_datetime_skeleton_for_ts,
    populate_fill_in_values,
)

# TODO: Replace `populate_fill_in_values_1` with `populate_fill_in_values` in import and
# delete `populate_fill_in_values_1` from `__init__.py` once `dbl_sigmoid_function` can take `datetime` as input instead of `doy`

# %% Testing on real data
# link to data: https://sentera.sharepoint.com/:x:/s/demeter/ESR0PKnkjQBIkYDkT9NBVS8B1h5kJHTbJE2tCLgM7QWP7A?e=Vtcdlw

########################################################
### Given a "complete" dataset for the desired temporal resolution (likely includes both "true" and "fill-in"),
### fit a new interpolation function on the "complete" dataset and return values for each datetime required.
########################################################
# %% DEM 357
# loading the data
df_true_in = read_csv(
    "/Users/marissakivi/Desktop/df_drone_imagery1.csv", parse_dates=["date_observed"]
)
df = df_true_in.copy()

col_datetime = "date_observed"
col_value = "value_observed"
col_t = "t_observed"
year = 2022  # we have to limit to one growing season for this function


# Convert temporal column to numeric for use in `double_sigmoid`
def get_unix_from_origin(dt: datetime, origin: datetime) -> int:
    this_timedelta = dt - origin
    return this_timedelta // Timedelta("1s")


# %% DEM 358: Load standard reference data and fit inference function

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

df_gimms_ndvi = df_gimms_ndvi.loc[
    df_gimms_ndvi["sample_value"].notna()
]  # remove rows where "sample_value" is NaN


df_gimms_ndvi["date_start"] = to_datetime(
    df_gimms_ndvi["date_start"]
)  # Convert the 'date_start' column to a datetime.datetime() object

df_gimms_ndvi[col_t] = df_gimms_ndvi["date_start"].map(
    lambda dt: get_unix_from_origin(dt, origin=datetime(year, 1, 1))
)

# generate the infer_function
pars, fx, guess = fit_double_sigmoid(
    df=df_gimms_ndvi, col_t=col_t, col_value="sample_value", guess=None
)

# %% Plot first inference function
t_interp = arange(
    0, df_gimms_ndvi[col_t].max(), round(df_gimms_ndvi[col_t].max() / 1000)
)
t_interp = df_gimms_ndvi[col_t]
y_interp = fx(t_interp)

plt.plot(t_interp, y_interp, "--", label="interpolated")


# Check the distribution of interpolated values and the observed values
plt.scatter(
    df_gimms_ndvi[col_t],
    df_gimms_ndvi["sample_value"],
)

plt.legend()
plt.xticks(rotation=60)
plt.show()

# %% DEM 358: Fill in values based on fitted inference functino

# generate a skeleton dataframe with `sample_values` for each datetime where available and NA where `sample_values` are unavailable
df_skeleton = get_datetime_skeleton_for_ts(
    df_true_data=df,
    datetime_start=datetime(2022, 3, 1),
    datetime_end=datetime(2022, 10, 31),
    temporal_resolution=timedelta(days=15),
    tolerance_alpha=0.5,
    col_datetime=col_datetime,
    col_value=col_value,
    recalibrate=True,
)

df_skeleton[col_t] = df_skeleton["datetime_skeleton"].map(
    lambda dt: get_unix_from_origin(dt, origin=datetime(year, 1, 1))
)

# generate dataframe where NA values are replaced with values obtained using `infer_function(col_datetime)`
df_complete = populate_fill_in_values(
    df_skeleton=df_skeleton,
    infer_function=fx,
    col_datetime=col_t,
    col_value=col_value,
)

# train a new infer_function on df_complete
pars_full, fx_full, guess = fit_double_sigmoid(
    df=df_complete, col_t=col_t, col_value=col_value, guess=None
)

# %% Plot
t_interp = arange(0, df_complete[col_t].max(), round(df_complete[col_t].max() / 1000))
y_interp = fx_full(t_interp)

plt.plot(t_interp, y_interp, "--", label="interpolated")

# Check the distribution of interpolated values and the observed values
colors = {True: "blue", False: "red"}
plt.scatter(
    df_complete[col_t],
    df_complete[col_value],
    c=df_complete["true_data"].map(colors),
)

plt.legend()
# plt.ylim(0, 1)
plt.xticks(rotation=60)
plt.show()
# %%
