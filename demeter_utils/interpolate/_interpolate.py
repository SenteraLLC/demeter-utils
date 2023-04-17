from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pandas import DataFrame
from pandas import concat as pd_concat

# def find_fill_in_dates(
#     df_true_data: DataFrame,
#     starttime: int,
#     endtime: int,
#     temporal_resolution: int,
#     date_plant: datetime,
# ) -> DataFrame:
#     """Determine which 'days of year' of 'day after planting' are absent in the input data
#        The input data should have at least `date_observed` and `value_observed` column

#     Args:
#         df_true_data (`DataFrame`): Input ("true") data that is available for a season/year.
#         starttime (`int`): The relative starting date for which data are required.
#         endtime (`int`): The relative end date for which data are required.
#         temporal_resolution (`int`): The minimum temporal resolution desired for the output data.
#         date_plant (`datetime`): The date of planting, if available. Otherwise first day of year which will return "doy" instead of "dap"

#     Returns:
#         `DataFrame`: input data concat with a new empty data, column `available` will indicate whether "true" data are
#         available for a given temporal resolution and date range (if avaialbe `True`, else `False`).
#     """
#     # rename the data frame
#     df_in = df_true_data

#     # create a new column `dap` for day after planting in data `df_in`
#     df_in["dap"] = (df_in["date_observed"] - date_plant).dt.days

#     # create a empty dataframe with all columns in `df_in` dataframe and add a column `dap` based on user input `starttime`, `endtime` and `temporal_resolution`
#     df_join = DataFrame(data=[], columns=df_in.columns)
#     df_join["dap"] = np.arange(
#         np.timedelta64(starttime, "D"),
#         np.timedelta64(endtime, "D"),
#         np.timedelta64(temporal_resolution, "D"),
#     ).astype(np.timedelta64)
#     df_join["dap"] = df_join["dap"].dt.days

#     # concat two dataframes `df_in` and `df_join`; if `dap` values in two dataframe is duplicate, keep the one from `df_in` only
#     # because `df_in` has `true` values
#     df_observed = (
#         pd_concat([df_in, df_join])
#         .drop_duplicates(subset=["dap"], keep="first")
#         .reset_index(drop=True)
#     )

#     # create a new column `doy_obs` by extract the day of year from `dap` column
#     df_observed["doy_obs"] = df_observed["dap"] + date_plant.timetuple().tm_yday

#     # add new column `available` to `df_observed` where true or false is returned based the condition, 'value_observed <=1'
#     available = []
#     for i in df_observed["value_observed"]:
#         if i <= 1:
#             available.append("True")
#         else:
#             available.append("False")

#     df_observed["available"] = available

#     # sort the dataframe by `doy_obs` in ascending order
#     df_observed = df_observed.sort_values(by=["doy_obs"])

#     return df_observed


# # %% Example use:

# # TODO: read data from cloud
# # df_true_data = read_csv(
# #     "/root/git/demeter-utils/df_drone_imagery1.csv",
# #     parse_dates=["date_observed", "last_updated"],
# # )
# # df_observed = find_fill_in_dates(df_true_data, 0, 70, 10, datetime(2022, 4, 15))


# # %% Example:
# #     True data:
# #               date_observed         value_observed
# #               2022-05-01             0.4
# #               2022-05-15             0.5
# #               2022-05-25             0.6

# #     Desired output for:
# #         find_fill_in_dates(df_true_data, starttime=0, endtime=70, temporal_resolution=10, plant_date=datetime(2022,4,15))
# #
# #               date_observed         value_observed        dap    doy_obs     available
# #                 NA                    NA                   0       105         False
# #                 NA                    NA                   10      115         False
# #               2022-05-01              0.4                  16      121         True
# #               2022-05-15              0.5                  20      125         True
# #               2022-05-25              0.6                  30      135         True
# #                 NA                    NA                   40      145         False
# #                 NA                    NA                   50      155         False
# #                 NA                    NA                   60      165         False


# %%
def generate_fill_in_values(
    df_reference_ndvi: DataFrame,
    datetime_start: datetime,
    datetime_end: datetime,
    temporal_resolution: timedelta,
    interp_function: str,
) -> DataFrame:
    """
    # Generate a dataframe with interpolated values given a standard/reference data `df_reference_ndvi' and `interp_function`
     The input dataset should have at least `date_start` and `sample_value` columns.

    Args:
        df_reference_ndvi (`DataFrame`): Input ("reference") data.
        datetime_start (`datetime.datetime`): Starting datetime for interpolation of reference data.
        datetime_end (`datetime.datetime`): Ending datetime for interpolation of reference data.
        temporal_resolution (`datetime.timedelta`): The minimum temporal resolution desired for the output interpolation of reference data.
        interp_function (`str`): Model type for interpolation, "CubicSpline" for cubic spline, "Akima1DInterpolator" for akima1DInterpolator, "PchipInterpolator" for pchip_interpolator

    Returns `DataFrame` wtih following columns:
        "datetime_interp": Datetime for the value observed/interpolated
        "doy_interp": Day of the year for the corresponding `datetime_interp`
        "model_type": Interpolation model/function used
        "ndvi_interp": Value of NDVI observed/interpolated

    """

    # copy the `df_reference_ndvi` as `df_forinterp`
    df_forinterp = df_reference_ndvi.copy()

    # Remove NA values from the 'sample_value' column in the data 'df_forinterp'
    df_forinterp = df_forinterp[df_forinterp["sample_value"].notna()]

    # Convert the 'date_start' column to a datetime.datetime() object
    df_forinterp["date_start"] = (df_forinterp["date_start"]).astype(np.datetime64)

    # Extract the day of year from the 'date_start' column and store it in a new column 'doy_obs'
    df_forinterp["doy_obs"] = df_forinterp["date_start"].apply(
        lambda x: x.timetuple().tm_yday
    )

    # create an arrary of `datetime_interp` values for interpolation
    datetime_interp = np.arange(datetime_start, datetime_end, temporal_resolution)

    # create a temp df `df_forinterp_temp` to store `datetime_interp` and `doy_interp`
    df_forinterp_temp = pd.DataFrame(
        datetime_interp, columns=["datetime_interp"]
    ).astype(np.datetime64)

    # convert the `datetime_interp` to `doy_interp` and add the values to a new column
    df_forinterp_temp["doy_interp"] = df_forinterp_temp["datetime_interp"].apply(
        lambda x: x.timetuple().tm_yday
    )

    doy_interp = df_forinterp_temp["doy_interp"]

    # %%

    # assign `x_interp`, `x_obs` and `y_obs` values
    x_interp = doy_interp  # Or, datetime_interp: The code does not work for datetime_interp as of now
    x_obs = df_forinterp["doy_obs"]
    y_obs = df_forinterp["sample_value"]

    # create a new data frame 'df_reference_interp' to store the interpolated values.
    df_reference_interp = pd.DataFrame(
        columns=["model_type", "doy_interp", "ndvi_interp"]
    )

    # generate interpolated ndvi values using the interp_function specified in function
    ndvi_interp = interp_function(x=x_obs, y=y_obs)(x_interp)

    data = {
        "model_type": str(interp_function),
        "doy_interp": x_interp,
        "ndvi_interp": ndvi_interp,
    }
    df_temp = pd.DataFrame(data=data)

    # create a df `df_reference_interp` with concat of `df_reference_interp` and `df_temp`
    df_reference_interp = pd_concat([df_reference_interp, df_temp], axis=0)

    # create a df `df_reference_interp_merged` by merging `df_reference_interp` and `df_forinterp_temp` based on 'doy_interp' column
    df_reference_interp_merged = pd.merge(
        df_forinterp_temp, df_reference_interp, on="doy_interp"
    )

    # remove NA values from the dataframe
    df_reference_interp_merged = df_reference_interp_merged[
        df_reference_interp_merged["ndvi_interp"].notna()
    ]
    return df_reference_interp_merged


# %% Example use:

# load the standard/reference data
# TODO: Use the function by Marissa
# GIMMS_COLS = {
#     "START DATE": "date_start",
#     "END DATE": "date_end",
#     "SAMPLE VALUE": "sample_value",
#     "SAMPLE COUNT": "n_pixels_sample",
#     "MEAN VALUE": "mean_hist_value",
#     "MIN VALUE": "min_hist_value",
#     "MAX VALUE": "max_hist_value",
# }
# req = requests.get(
#     "https://glam1.gsfc.nasa.gov/api/gettbl/v4?sat=MOD&version=v11&layer=NDVI&mask=NASS_2011-2016_corn&shape=ADM&ids=110955&ts_type=seasonal&years=2019&start_month=1&num_months=12&format=csv"
# )
# text = StringIO(req.content.decode("utf-8"))
# df_gimms_ndvi = read_csv(text, skiprows=14).rename(columns=GIMMS_COLS)[
#     GIMMS_COLS.values()
# ]

# df_interp = generate_fill_in_values(
#     df_reference_ndvi = df_gimms_ndvi,
#     datetime_start = datetime(2019, 5, 6),
#     datetime_end = datetime(2019, 10, 1),
#     temporal_resolution = timedelta(days=1),
#     interp_function = CubicSpline,
#     )

# %% Example:
#     True data:
#               START DATE         SAMPLE VALUE
#               2022-05-01             0.4
#               2022-05-15             0.5
#               2022-05-25             0.6

#     Desired output for:
#         df_interp = generate_fill_in_values(
#     df_reference_ndvi = df_gimms_ndvi,
#     datetime_start = datetime(2019, 5, 6),
#     datetime_end = datetime(2019, 10, 1),
#     temporal_resolution = timedelta(days=1),
#     interp_function = CubicSpline,
#     )
#
#               model_type         doy_interp        ndvi_interp
#           Akima1DInterpolator         1               0.265
#           Akima1DInterpolator         2               0.275


# %% TODO: Delete if `doy_interp` is used instead of `datetime_interp`
# convert `doy_interp` to float
# def dt64_to_float(dt64):
#     """Converts numpy.datetime64 to year as float.

#     Rounded to days

#     Parameters
#     ----------
#     dt64 : np.datetime64 or np.ndarray(dtype='datetime64[X]')
#         date data

#     Returns
#     -------
#     float or np.ndarray(dtype=float)
#         Year in floating point representation
#     """

#     year = dt64.astype('M8[Y]')
#     days = (dt64 - year).astype('timedelta64[D]')
#     year_next = year + np.timedelta64(1, 'Y')
#     days_of_year = (year_next.astype('M8[D]') - year.astype('M8[D]')
#                     ).astype('timedelta64[D]')
#     dt_float = 1970 + year.astype(float) + days / (days_of_year)
#     return dt_float

# doy_interp_float = dt64_to_float(doy_interp)


# TODO: Delete this if not required
# datetime_obs_array = df_forinterp["date_start"].to_numpy()
# datetime_obs_float = dt64_to_float(datetime_obs_array)
# %%
