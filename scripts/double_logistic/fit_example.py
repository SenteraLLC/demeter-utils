# %% Imports
from datetime import datetime
from functools import partial
from io import StringIO

import matplotlib.pyplot as plt
import requests
from pandas import DataFrame, date_range, read_csv, to_datetime
from scipy.optimize import minimize

from demeter_utils.time import convert_dt_to_unix
from scripts.double_logistic.functions import double_logistic, estimate_inflection

# %% Get data
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
    "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2021&start_month=1&num_months=12&format=csv"
)
text = StringIO(req.content.decode("utf-8"))
df_gimms_ndvi = read_csv(text, skiprows=14).rename(columns=GIMMS_COLS)[
    GIMMS_COLS.values()
]

# %% Clean data for function
col_datetime = "date_start"
col_value = "sample_value"
col_unix = "t_unix"
col_t = "t"
year = 2022

df = df_gimms_ndvi.loc[df_gimms_ndvi[col_value].notna()]
df[col_datetime] = to_datetime(df[col_datetime])
df.sort_values(by=[col_datetime], inplace=True)
df.reset_index(drop=True, inplace=True)

# %% Fit double logistic function

# Input parameters
df_in = df.copy()
col_datetime = col_datetime
col_value = col_value

# Functions vars
col_unix = "t_unix"
col_t = "t"

# %% Transform and standardize temporal dimension to improve convergence
date_min = df_in[col_datetime].min()
df_in[col_unix] = df_in[col_datetime].map(
    lambda dt: convert_dt_to_unix(dt, relative_epoch=date_min)
)

# standardize to reduce scale (mean = 0, sd = 1)
t_mean = df_in[col_unix].mean()
t_sd = df_in[col_unix].std()


def dt_transformation(dt: datetime) -> float:
    unix = convert_dt_to_unix(dt, relative_epoch=date_min)  # convert to psuedo-unix
    return (unix - t_mean) / t_sd  # scale


df_in[col_t] = df_in[col_datetime].map(lambda dt: dt_transformation(dt))

# %% Estimate initial values of parameters based on time series
ymax = df_in[col_value].max()
ymin = df_in[col_value].min()

# determine left and right side of curve
max_threshold = 0.1
max_bound = ymax * (1 - max_threshold)
ind_max = df_in.loc[df_in[col_value] >= max_bound].index.values

df_left = df_in.iloc[: min(ind_max) + 1, :]
df_right = df_in.iloc[max(ind_max) :, :]

left_params = estimate_inflection(
    t=df_left[col_t],
    y=df_left[col_value],
    ymin=ymin,
    ymax=ymax,
)

right_params = estimate_inflection(
    t=df_right[col_t],
    y=df_right[col_value],
    ymin=ymin,
    ymax=ymax,
)

# %% Fit double logistic
t = df_in[col_t].astype(float)
y = df_in[col_value].astype(float)

guess = {
    "ymin": ymin,
    "ymax": ymax,
    "t_incr": left_params["t"],
    "t_decr": right_params["t"],
    "rate_incr": left_params["rate"],
    "rate_decr": right_params["rate"],
}
guess_values = [*guess.values()]


def _cost_function(p, t, y):
    partial_fx = partial(
        double_logistic,
        ymin=p[0],
        ymax=p[1],
        t_incr=p[2],
        t_decr=p[3],
        rate_incr=p[4],
        rate_decr=p[5],
    )
    y_pred = partial_fx(t)
    se = (y_pred - y) ** 2
    return se.sum()


# minimize cost function with initial values
opt = minimize(_cost_function, guess_values, args=(t, y))
popt = opt.x
pars = {
    "ymin": popt[0],
    "ymax": popt[1],
    "t_incr": popt[2],
    "t_decr": popt[3],
    "rate_incr": popt[4],
    "rate_decr": popt[5],
}


# create partial function that takes `datetime`, transforms it appropriately, and estimates value
def fitted_double_logistic(dt: datetime) -> float:
    partial_fx = partial(
        double_logistic,
        ymin=pars["ymin"],
        ymax=pars["ymax"],
        t_incr=pars["t_incr"],
        t_decr=pars["t_decr"],
        rate_incr=pars["rate_incr"],
        rate_decr=pars["rate_decr"],
    )

    t = dt_transformation(dt)
    return partial_fx(t)


# %% Plot
dt_fit = date_range(
    start=df_in[col_datetime].min(), end=df_in[col_datetime].max(), periods=100
)
df_fit = DataFrame(data={"t": dt_fit, "value": fitted_double_logistic(dt=dt_fit)})

plt.scatter(df_in[col_datetime], df_in[col_value], c="black")
plt.plot(df_fit["t"], df_fit["value"])

plt.legend()
plt.xticks(rotation=60)
plt.show()

# %% Plots for the Confluence page
# # %% Plot
# from scripts.double_logistic.functions import _cubic_poly_predict
# from numpy import polyfit, arange


# # fit a cubic polynomial f(t)
# t = df_right[col_t]
# y = df_right[col_value]
# coef = polyfit(x = t, y = y, deg = 3)
# t_fit = arange(start = min(t), stop = max(t), step = 0.05)
# df_fit = DataFrame(
#     data = {'t': t_fit, 'value': _cubic_poly_predict(coef, t_fit)}
# )

# # determine t where f(t) = midpoint
# midpoint = (ymax + ymin)/2
# intersect_diff_value = abs(df_fit['value'] - midpoint)
# intersect = min(intersect_diff_value)
# intersect_ind = df_fit.loc[intersect_diff_value == intersect].index.values[0]
# t_intersect_right = df_fit.at[intersect_ind, 't']
# val_intersect_right = df_fit.at[intersect_ind, 'value']

# # fit a cubic polynomial f(t)
# t = df_left[col_t]
# y = df_left[col_value]
# coef = polyfit(x = t, y = y, deg = 3)

# t_fit = arange(start = min(t), stop = max(t), step = 0.05)
# df_fit2 = DataFrame(
#     data = {'t': t_fit, 'value': _cubic_poly_predict(coef, t_fit)}
# )

# # determine t where f(t) = midpoint
# intersect_diff_value = abs(df_fit2['value'] - midpoint)
# intersect = min(intersect_diff_value)
# intersect_ind = df_fit2.loc[intersect_diff_value == intersect].index.values[0]
# t_intersect_left = df_fit2.at[intersect_ind, 't']
# val_intersect_left = df_fit2.at[intersect_ind, 'value']


# # plt.scatter(
# #     df_in[col_t],
# #     df_in[col_value],
# #     c = 'black'
# # )

# plt.hlines(y = [ymin, ymax], xmin = -1.5, xmax = 1.5)
# plt.plot(df_fit['t'], df_fit['value'], c = 'red')
# plt.plot(df_fit2['t'], df_fit2['value'], c = 'red')

# plt.scatter(
#     x = [t_intersect_left],
#     y = [val_intersect_left],
#     c = 'blue'
# )
# plt.scatter(
#     x = [t_intersect_right],
#     y = [val_intersect_right],
#     c = 'blue'
# )

# plt.legend()
# plt.xticks(rotation=60)
# plt.show()
# %%
