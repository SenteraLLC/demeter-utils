from datetime import datetime, timedelta

from numpy import datetime64
from pandas import DataFrame
from pandas import concat as pd_concat

from demeter_utils.interpolate._interpolate import get_datetime_skeleton_for_ts


def interpolation(
    df_complete: DataFrame,
    datetime_start: datetime,
    datetime_end: datetime,
    temporal_resolution: timedelta,
    interp_function: str,
) -> DataFrame:
    """
    Given a pandas dataframe 'df_complete' and 'interp_function', interpolate the values in 'df_complete'

    Parameters:
    df_complete (DataFrame): dataframe returned by the `populate_fill_in_values` function
    datetime_start (`datetime.datetime`): Starting datetime for needed time series [value can be same as in `get_datetime_skeleton_for_ts` function]
    datetime_end (`datetime.datetime`): Ending datetime for needed time series [value can be same as in `get_datetime_skeleton_for_ts` function]
    temporal_resolution (`datetime.timedelta`): The minimum temporal resolution desired for the output time series [value different than in `get_datetime_skeleton_for_ts` function].
    interp_function (`str`): Model type for interpolation, "CubicSpline" for cubic spline, "Akima1DInterpolator" for akima1DInterpolator, "PchipInterpolator" for pchip_interpolator

    Returns:
    df_final (pandas.DataFrame): dataframe with `sample_values` for each datetime within desired datetime range and temporal resolution
    """

    # Make a copy of df_complete
    df_complete_in = df_complete.copy()

    # TODO: Rename the `datetime_skeleton` column in function `populate_fill_in_values`
    # Convert the 'datetime_skeleton' column to a datetime.datetime() object
    df_complete_in["datetime_skeleton"] = (df_complete_in["datetime_skeleton"]).astype(
        datetime64
    )

    # create an `df_skeleton` using fuction `get_datetime_skeleton_for_ts`
    df_skeleton = get_datetime_skeleton_for_ts(
        df_true_data=df_complete_in,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        temporal_resolution=temporal_resolution,
        tolerance_alpha=0.5,
        col_datetime="datetime_skeleton",
        col_value="sample_value",
        recalibrate=True,
    )

    # assign `x_interp`, `x_obs` and `y_obs` values
    x_interp = df_skeleton["datetime_skeleton"]
    x_obs = df_complete_in["datetime_skeleton"]
    y_obs = df_complete_in["sample_value"]

    # create a dictionary of interp_functions

    # generate interpolated sample values using the `interp_function` specified in function
    sample_value_interp = interp_function(x=x_obs, y=y_obs)(x_interp)

    data = {
        "model_type": interp_function,
        "sample_value_interp": sample_value_interp,
    }

    df_temp = DataFrame(data=data)

    # create a df `df_interp` with concat of `df_skeleton` and `df_temp`
    df_interp = pd_concat([df_skeleton, df_temp], axis=1)

    return df_interp
