from datetime import datetime

import numpy as np
import pandas as pd
from pandas import DataFrame
from pandas import concat as pd_concat


def find_fill_in_dates(
    df_true_data: DataFrame,
    starttime: int,
    endtime: int,
    temporal_resolution: int,
    date_plant: datetime,
) -> DataFrame:
    """Determine which 'days of year' of 'day after planting' are absent in the input data
       The input data should have at least `date_observed` and `value_observed` column

    Args:
        df_true_data (`DataFrame`): Input ("true") data that is available for a season/year.
        starttime (`int`): The relative starting date for which data are required.
        endtime (`int`): The relative end date for which data are required.
        temporal_resolution (`int`): The minimum temporal resolution desired for the output data.
        date_plant (`datetime`): The date of planting, if available. Otherwise first day of year which will return "doy" instead of "dap"

    Returns:
        `DataFrame`: input data concat with a new empty data, column `available` will indicate whether "true" data are
        available for a given temporal resolution and date range (if avaialbe `True`, else `False`).
    """
    # rename the data frame
    df_in = df_true_data

    # create a new column `dap` for day after planting in data `df_in`
    df_in["dap"] = (df_in["date_observed"] - date_plant).dt.days

    # create a empty dataframe with all columns in `df_in` dataframe and add a column `dap` based on user input `starttime`, `endtime` and `temporal_resolution`
    df_join = DataFrame(data=[], columns=df_in.columns)
    df_join["dap"] = np.arange(
        np.timedelta64(starttime, "D"),
        np.timedelta64(endtime, "D"),
        np.timedelta64(temporal_resolution, "D"),
    ).astype(np.timedelta64)
    df_join["dap"] = df_join["dap"].dt.days

    # concat two dataframes `df_in` and `df_join`; if `dap` values in two dataframe is duplicate, keep the one from `df_in` only
    # because `df_in` has `true` values
    df_observed = (
        pd_concat([df_in, df_join])
        .drop_duplicates(subset=["dap"], keep="first")
        .reset_index(drop=True)
    )

    # create a new column `doy_obs` by extract the day of year from `dap` column
    df_observed["doy_obs"] = df_observed["dap"] + date_plant.timetuple().tm_yday

    # add new column `available` to `df_observed` where true or false is returned based the condition, 'value_observed <=1'
    available = []
    for i in df_observed["value_observed"]:
        if i <= 1:
            available.append("True")
        else:
            available.append("False")

    df_observed["available"] = available

    # sort the dataframe by `doy_obs` in ascending order
    df_observed = df_observed.sort_values(by=["doy_obs"])

    return df_observed


# %% Example use:

# TODO: read data from cloud
# df_true_data = read_csv(
#     "/root/git/demeter-utils/df_drone_imagery1.csv",
#     parse_dates=["date_observed", "last_updated"],
# )
# df_observed = find_fill_in_dates(df_true_data, 0, 70, 10, datetime(2022, 4, 15))


# %% Example:
#     True data:
#               date_observed         value_observed
#               2022-05-01             0.4
#               2022-05-15             0.5
#               2022-05-25             0.6

#     Desired output for:
#         find_fill_in_dates(df_true_data, starttime=0, endtime=70, temporal_resolution=10, plant_date=datetime(2022,4,15))
#
#               date_observed         value_observed        dap    doy_obs     available
#                 NA                    NA                   0       105         False
#                 NA                    NA                   10      115         False
#               2022-05-01              0.4                  16      121         True
#               2022-05-15              0.5                  20      125         True
#               2022-05-25              0.6                  30      135         True
#                 NA                    NA                   40      145         False
#                 NA                    NA                   50      155         False
#                 NA                    NA                   60      165         False


def generate_fill_in_values(
    df_reference_ndvi: DataFrame,
    interp_function: str,
) -> DataFrame:
    """
    # Generate a dataframe with interpolated values given a standard/reference data `df_reference_ndvi' and `interp_function`
     The input dataset should have at least `START DATE` and `SAMPLE VALUE` columns.

    Args:
        df_reference_ndvi (`DataFrame`): Input ("reference") data.
        interp_function (`str`): Model type for interpolation, "CubicSpline" for cubic spline, "Akima1DInterpolator" for akima1DInterpolator, "PchipInterpolator" for pchip_interpolator

    Returns:
        `DataFrame`: Input dataset plus three added columns; 'model_type', 'doy_interp' for interpolated temporal resolution and 'ndvi_interp' for interpolated ndvi values.
    """

    # create an arrary of day of year `doy_interp` values for interpolation
    doy_interp = np.arange(0, 365, 1).astype(int)

    # Remove NA values from the 'sample value' column in the data 'df_reference_ndvi'
    df_reference_ndvi = df_reference_ndvi[df_reference_ndvi["SAMPLE VALUE"].notna()]

    # TODO: For broader use of thie function, define condition to turn the below two chunks only if `doy_obs` column
    # is not present in the dataset

    # Convert the 'start date' column to a datetime object
    df_reference_ndvi["START DATE"] = (df_reference_ndvi["START DATE"]).astype(
        np.datetime64
    )

    # Extract the day of year from the 'start date' column and store it in a new column 'doy_obs'
    df_reference_ndvi["doy_obs"] = df_reference_ndvi["START DATE"].apply(
        lambda x: x.timetuple().tm_yday
    )

    # remane data frame to 'df_forinterp'
    df_forinterp = df_reference_ndvi

    # create a new data frame 'df_reference_interp' to store the interpolated values.
    df_reference_interp = pd.DataFrame(
        columns=["model_type", "doy_interp", "ndvi_interp"]
    )

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
    df_reference_interp = pd.concat([df_reference_interp, df_temp], axis=0)

    # remove NA values from the dataframe
    df_reference_interp = df_reference_interp[
        df_reference_interp["ndvi_interp"].notna()
    ]
    return df_reference_interp


# %% Example use:

# import the data from gimms portal
# TODO: Use the function by Marissa
# req = requests.get(
#     "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2019&start_month=1&num_months=12&format=csv"
# )
# text = StringIO(req.content.decode("utf-8"))
# df_gimms_ndvi = read_csv(text, skiprows=14)

# df_interp = generate_fill_in_values(df_gimms_ndvi, Akima1DInterpolator)


# %% Example:
#     True data:
#               START DATE         SAMPLE VALUE
#               2022-05-01             0.4
#               2022-05-15             0.5
#               2022-05-25             0.6

#     Desired output for:
#         generate_fill_in_values(df_reference_ndvi = df_gimms_ndvi, interp_function = Akima1DInterpolator))
#
#               model_type         doy_interp        ndvi_interp
#           Akima1DInterpolator         1               0.265
#           Akima1DInterpolator         2               0.275


def fill_missing_values(
    df_observed: DataFrame,
    df_reference_interp: DataFrame,
) -> DataFrame:
    """
    Given two pandas dataframes 'df_observed' and 'df_reference_interp', replace missing 'ndvi' values in 'df_observed'
    with corresponding 'ndvi' values from 'df_reference_interp' if the 'doy_obs'and 'doy_interp' values in two dataframes match.

    Parameters:
    df_observed (DataFrame): first dataframe obtained by using `find_fill_in_date` function
    df_reference_interp (DataFrame): second dataframe obtained by using `generate_fill_in_values` function

    Returns:
    df_final (pandas.DataFrame): updated first dataframe with missing values filled
    """

    # Make a copy of df_observed to avoid modifying the original dataframe
    df_final = df_observed.copy()

    # Find the missing 'value_observed' values in df_observed
    missing_ndvi = df_final["available"] == "False"

    # Create a dictionary to map 'doy_interp' values in df_interp to 'ndvi_interp' values
    doy_ndvi_dict = dict(
        zip(df_reference_interp["doy_interp"], df_reference_interp["ndvi_interp"])
    )

    # For each missing 'value_observed' value in df_observed, replace it with the corresponding 'ndvi_interp' value from df_interp
    for index, row in df_final[missing_ndvi].iterrows():
        doy = row["doy_obs"]
        if doy in doy_ndvi_dict:
            df_final.at[index, "value_observed"] = doy_ndvi_dict[doy]

    return df_final


# %% Example use:

# df_complete_data = fill_missing_values(df_observed, df_reference_interp)

# %% Example:
#       Input data `df_observed`:
#
#               date_observed         value_observed        dap    doy_obs     available
#                 NA                    NA                   0       105         False
#                 NA                    NA                   10      115         False
#               2022-05-01              0.4                  16      121         True
#               2022-05-15              0.5                  20      125         True
#               2022-05-25              0.6                  30      135         True
#                 NA                    NA                   40      145         False
#                 NA                    NA                   50      155         False
#                 NA                    NA                   60      165         False
#
#       Input data `df_reference_interp`:
#
#               model_type         doy_interp        ndvi_interp
#           Akima1DInterpolator         1               0.265
#           Akima1DInterpolator         2               0.275
#
#     Desired output for:
#         fill_missing_values(df_observed, df_reference_interp)
#
#                date_observed         value_observed        dap    doy_obs     available
#                 NA                    0.26                 0       105         False
#                 NA                    0.28                 10      115         False
#               2022-05-01              0.4                  16      121         True
#               2022-05-15              0.5                  20      125         True
#               2022-05-25              0.6                  30      135         True
#                 NA                    0.56                 40      145         False
#                 NA                    0.66                 50      155         False
#                 NA                    0.68                 60      165         False
