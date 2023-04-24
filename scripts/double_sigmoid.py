from datetime import datetime
from io import StringIO

import matplotlib.pyplot as plt
import requests
from numpy import arange, array, exp
from pandas import read_csv

# %% Double sigmoid function [CASE 1: Generate y_pred based on input model parameters]
# Modified from https://geog0111.readthedocs.io/en/latest/Chapter7_FittingPhenologyModels.html


def dbl_sigmoid_function_1(p, t):
    """The double sigmoid function defined over t (where t is an array).
    Takes a vector of 6 parameters
    where, p0 = value_observed at planting/emergence,
    p1 = max value_observed at peak growing season
    p2 = rate of increase of value_observed at p3
    p3 = x-value (doy_obs) for which p2 is calculated
    p4 = rate of decrease of value_observed at p5
    p5 = x-value (doy_obs) for which p4 is calcualted
    """

    sigma1 = 1.0 / (1 + exp(p[2] * (t - p[3])))
    sigma2 = 1.0 / (1 + exp(-p[4] * (t - p[5])))
    y = p[0] - p[1] * (sigma1 + sigma2 - 1)
    return y


# Example use:

# load sample data
req = requests.get(
    "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2019&start_month=1&num_months=12&format=csv"
)
text = StringIO(req.content.decode("utf-8"))
df_gimms_ndvi = read_csv(text, skiprows=14)

# %% TODO: This chunk of code needs cleaning
# create a new column `dap` for day after planting in data `df_gimms_ndvi`
df_gimms_ndvi["START DATE"] = df_gimms_ndvi["START DATE"].astype("datetime64")

# TODO: extract year and for 'START DATE to calculate doy'
year = (df_gimms_ndvi["START DATE"].dt.year).unique
date_year_start = datetime(2019, 1, 1)
df_gimms_ndvi["doy_obs"] = (df_gimms_ndvi["START DATE"] - date_year_start).dt.days

doy_obs = df_gimms_ndvi["doy_obs"]
value_obs = df_gimms_ndvi["SAMPLE VALUE"]
# %%

# some random model parameters
p0 = array([0.22, 0.8, 0.035, 170, 0.045, 260])

# generate an array of predicted value based on the function and parameters
doy_pred = arange(0, 365, 1)
value_pred = dbl_sigmoid_function_1(p0, doy_pred)

# plot the observed data
plt.scatter(doy_obs, value_obs, c="green")

# plot the predicted data
plt.plot(doy_pred, value_pred, "--", label="Predicted")
plt.show()
