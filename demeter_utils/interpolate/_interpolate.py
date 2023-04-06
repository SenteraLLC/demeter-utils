from datetime import datetime
from io import StringIO
from typing import Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from pandas import DataFrame
from pandas import concat as pd_concat
from pandas import read_csv
from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

# from pandas import merge as pd_merge
# import seaborn as sns

#    """"
# Step 1: Load data that already exists (that may or may not represent an entire growing season)

#     The dataset should have at least 'doy' and 'value_observed' columns
#    """


# %% Function 1
def find_fill_in_dates(
    df_true_data: DataFrame,
    starttime: int,
    endtime: int,
    temporal_resolution: int,
    date_plant: datetime,
) -> DataFrame:
    """
    # Step 2: Given an argument `temporal_resolution`, determine which times/days are absent from input data (step 1)

    Args:
        df_true_data (`DataFrame`): Input ("true") data.
        starttime (`int`): The relative starting date for which data are required.
        endtime (`int`): The relative end date for which data are required.
        temporal_resolution (`int`): The minimum temporal resolution of the combined "true" and "fill-in" data.
        date_plant (`datetime`): The date of planting, if available. Otherwise first day of year which will return "doy" instead of "dap"

    Returns:
        `DataFrame`: Whether "true" data are available for a given temporal resolution and date range (True or False).
    """
    # rename the data frame
    df_in = df_true_data

    # create a new column `dap` for day after planting in data `df_in`
    df_in["dap"] = (df_in["date_observed"] - date_plant).dt.days

    # create a empty dataframe with all column in `df_in` dataframe and add a column `dap` with user input `starttime`, `endtime` and `temporal_resolution`
    df_join = DataFrame(data=[], columns=df_in.columns)
    df_join["dap"] = np.arange(
        np.timedelta64(starttime, "D"),
        np.timedelta64(endtime, "D"),
        np.timedelta64(temporal_resolution, "D"),
    ).astype(np.timedelta64)
    df_join["dap"] = df_join["dap"].dt.days

    # concat two dataframes `df_in` and `df_join`; if `dap` values in two dataframe is duplicate, keep the one from `df_in` only
    df_observed = (
        pd_concat([df_in, df_join])
        .drop_duplicates(subset=["dap"], keep="first")
        .reset_index(drop=True)
    )

    # Extract the day of year from `dap` column and store it in a new column `doy_obs`
    df_observed["doy_obs"] = df_observed["dap"] + date_plant.timetuple().tm_yday

    # add new column `available` to `df_inventory` where true or false is returned based the condition, 'value_observed <=1'
    available = []
    for i in df_observed["value_observed"]:
        if i <= 1:
            available.append("True")
        else:
            available.append("False")

    df_observed["available"] = available

    return df_observed


# %% Example use:

# TODO: read data from cloud
# df_true_data = read_csv(
#     "/root/git/demeter-utils/df_drone_imagery1.csv",
#     parse_dates=["date_observed", "last_updated"],
# )
# df_observed = find_fill_in_dates(df_true_data, 0, 120, 10, datetime(2022, 5, 6))


# %% Example:
#     True data:
#         DAP     value
#         20      0.4
#         30      0.5
#         40      0.6

#     Desired output for:
#         `generate_fill_in_values_a(df_true_data, temporal_resolution=10, starttime=0, endtime=100)
#             day     available   value
#             0       False       NA
#             10      False       NA
#             20      True        0.4
#             30      True        0.5
#             40      True        0.6
#             50      False       NA
#             60      False       NA
#             70      False       NA
#             80      False       NA
#             90      False       NA
#             100     False       NA


# %% Function 2

#    """"
#     Step 1: Load the standard/reference data (from gimms api)

#     The dataset should have at least 'START DATE' and 'SAMPLE VALUE' columns
#    """


def generate_fill_in_values(
    df_gimms_ndvi: DataFrame,
    interp_function: str,
) -> DataFrame:
    """
    # Step 2: Given the arguments `df_gimms_ndvi' and `interp_function`, return a dataframe with interpolated values

    Args:
        df_gimms_ndvi (`DataFrame`): Input ("reference") data.
        interp_function (`str`): Model type for interpolation, "CubicSpline" for cubic spline, "Akima1DInterpolator" for akima1DInterpolator, "PchipInterpolator" for pchip_interpolator

    Returns:
        `DataFrame`: Input dataset plus two added columns (three if model type required) 'doy_interp' for interpolated temporal resolution and 'ndvi_interp' for interpolated ndvi values.
    """
    # %%
    # create an arrary of doy values for interpolation
    doy_interp = np.arange(0, 365, 1).astype(int)

    # Remove NA values from the 'sample value' column in the data 'df_gimms_ndvi'
    df_gimms_ndvi = df_gimms_ndvi[df_gimms_ndvi["SAMPLE VALUE"].notna()]

    # TODO: Define condition to tun the below two chunks only if `doy_obs` column is not present in the dataset
    # Convert the 'start date' column to a datetime object
    df_gimms_ndvi["START DATE"] = (df_gimms_ndvi["START DATE"]).astype(np.datetime64)

    # Extract the day of year from the 'start date' column and store it in a new column 'doy_obs'
    df_gimms_ndvi["doy_obs"] = df_gimms_ndvi["START DATE"].apply(
        lambda x: x.timetuple().tm_yday
    )

    # remane data frame to 'df_forinterp'
    df_forinterp = df_gimms_ndvi

    # create a new data fram 'df_interp' to store the interpolated values.
    df_interp = pd.DataFrame(columns=["model_type", "doy_interp", "ndvi_interp"])

    # generate interpolated ndvi values using the interp_function specified in function
    ndvi_interp = interp_function(
        x=df_forinterp["doy_obs"], y=df_forinterp["SAMPLE VALUE"]
    )(doy_interp)

    data = {
        "model_type": interp_function,
        "doy_interp": doy_interp,
        "ndvi_interp": ndvi_interp,
    }
    df_temp = pd.DataFrame(data=data)
    df_interp = pd.concat([df_interp, df_temp], axis=0)

    # remove NA values from the dataframe
    df_interp = df_interp[df_interp["ndvi_interp"].notna()]
    return df_interp


# %% Example use:

# import the data from gimms portal
# TODO: Use the function by Marissa
# req = requests.get(
#     "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2019&start_month=1&num_months=12&format=csv"
# )
# text = StringIO(req.content.decode("utf-8"))
# df_gimms_ndvi = read_csv(text, skiprows=14)

# df_interp = generate_fill_in_values(df_gimms_ndvi, Akima1DInterpolator)


# %% Function 3
def fill_missing_values(
    df_observed: DataFrame,
    df_interp: DataFrame,
) -> DataFrame:
    """
    Given two pandas dataframes 'df_observed' and 'df_interp', replace missing 'ndvi' values in 'df_observed'
    with corresponding 'ndvi' values from 'df_interp' if the 'dap' values in both dataframes match.

    Parameters:
    df_observed (DataFrame): first dataframe obtained by using `find_fill_in_date` function
    df_interp (DataFrame): second dataframe obtained by using `generate_fill_in_values` function

    Returns:
    df_final (pandas.DataFrame): updated first dataframe with missing values filled
    """

    # Make a copy of df_observed to avoid modifying the original dataframe
    df_final = df_observed.copy()

    # Find the missing 'value_observed' values in df_observed
    missing_ndvi = df_final["available"] == "False"

    # Create a dictionary to map 'doy_interp' values in df_interp to 'ndvi_interp' values
    doy_ndvi_dict = dict(zip(df_interp["doy_interp"], df_interp["ndvi_interp"]))

    # For each missing 'value_observed' value in df_observed, replace it with the corresponding 'ndvi_interp' value from df_interp
    for index, row in df_final[missing_ndvi].iterrows():
        doy = row["doy_obs"]
        if doy in doy_ndvi_dict:
            df_final.at[index, "value_observed"] = doy_ndvi_dict[doy]

    return df_final


# Example usage

# load the true data
# TODO: read data from cloud
df_true_data = read_csv(
    "/root/git/demeter-utils/df_drone_imagery1.csv",
    parse_dates=["date_observed", "last_updated"],
)

# using `find_fill_in_dates function` to generate df_observed
df_observed = find_fill_in_dates(df_true_data, 0, 120, 10, datetime(2022, 5, 6))

# load the standard/reference data
# TODO: Use the function by Marissa
req = requests.get(
    "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2019&start_month=1&num_months=12&format=csv"
)
text = StringIO(req.content.decode("utf-8"))
df_gimms_ndvi = read_csv(text, skiprows=14)

# using `generate_fill_in_values` to generate df_interp
df_interp = generate_fill_in_values(df_gimms_ndvi, Akima1DInterpolator)

# using `fill_missing_values` to generate df_final
df_complete_data = fill_missing_values(df_observed, df_interp)


# %% Function 4
def fit_on_complete_data(
    df_complete_data: DataFrame,
    unfitted_interp_func: Union[Akima1DInterpolator, CubicSpline, PchipInterpolator],
) -> Union[Akima1DInterpolator, CubicSpline, PchipInterpolator]:
    """
    Step XX: Given a "complete" dataset for the desired temporal resolution (likely includes both
    "true" and "fill-in"), fit and return a new interpolation function on the "complete" dataset
    """
    print("Hello Step 3")


# rename column names
# df_complete_data = df_complete_data.rename(
#     columns={"value_observed": "SAMPLE VALUE", "date_observed": "START DATE"}
# )

# fit_complete_data = generate_fill_in_values(df_complete_data, PchipInterpolator)


# %% Generate the interpolated values for each DOY (defined dates) using different methods and plot them

df = df_complete_data
df = df.rename(columns={"value_observed": "ndvi_obs"})
df = df.sort_values(by=["doy_obs"])
doy_obs = df["doy_obs"]
ndvi_obs = df["ndvi_obs"]
doy_interp = np.arange(120, 250, 1, dtype=int)

ndvi_cubic = CubicSpline(doy_obs, ndvi_obs)(doy_interp)
ndvi_akima = Akima1DInterpolator(doy_obs, ndvi_obs)(doy_interp)
ndvi_pchip1 = PchipInterpolator(doy_obs, ndvi_obs)(doy_interp)
# ndvi_pchip2 = pchip_interpolate(doy_obs, ndvi_obs)(doy_interp)

plt.plot(doy_interp, ndvi_cubic, "--", label="spline")
plt.plot(doy_interp, ndvi_akima, "-", label="Akima1D")
plt.plot(doy_interp, ndvi_pchip1, "-", label="pchip1")
# plt.plot(doy_interp, ndvi_pchip2, '-', label='pchip2')
plt.plot(doy_obs, ndvi_obs, "o")

# TODO: plot 'ndvi_obs' with two color; one for `true` and other for `false`
# colors = {'True':'green', 'False':'red'}
# plt.plot(doy_obs, ndvi_obs, c = df['available'].map(colors))

plt.legend()
plt.ylim(0, 1)
plt.show()

# %%
# to store the interpolated values
# data = {
#     "ndvi_cubic": ndvi_cubic,
#     "ndvi_akima": ndvi_akima,
#     "ndvi_pchip1": ndvi_pchip1,
# }
# df_temp = pd.DataFrame(data=data)


# %% From Tyler
# # %% Function 2
# def generate_fill_in_values_b(
#     df_std_ndvi: DataFrame,
#     df_inventory: DataFrame,
#     interp_func: Union[Akima1DInterpolator, CubicSpline, PchipInterpolator],
# ) -> DataFrame:
#     """
#     # Step 2b: Given a fitted interpolation function, generate "fill-in" data for all absent dates

#     Returns:
#         `DataFrame`: Keeps "true" data alone, but interpolates for all required "fill-in" dates.
#     """

#     # df["value"] = df["value"].fillna(0)

#     print("Hello B")

# def fit_on_complete_data(
#     df_complete_data: DataFrame,
#     unfitted_interp_func: Union[Akima1DInterpolator, CubicSpline, PchipInterpolator],
# ) -> Union[Akima1DInterpolator, CubicSpline, PchipInterpolator]:
#     """
#     Step XX: Given a "complete" dataset for the desired temporal resolution (likely includes both
#     "true" and "fill-in"), fit and return a new interpolation function on the "complete" dataset
#     """
#     print("Hello Step 3")

# %%
