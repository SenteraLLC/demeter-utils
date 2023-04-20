from numpy import datetime64
from pandas import DataFrame
from pandas import concat as pd_concat


def interpolation(
    df_complete: DataFrame,
    df_skeleton: DataFrame,
    interp_type: str,
    col_datetime: str = "datetime_skeleton",
    col_value: str = "sample_value",
) -> DataFrame:
    """
    Given pandas dataframe 'df_complete', `df_skeleton`, and `interp_type` it will interpolate the values in 'df_complete' based on `interp_type` passed for each datetime in `df_skeleton`

    Args:
        df_complete (DataFrame): dataframe returned by the `populate_fill_in_values` function
        interp_type (`str`): Model type for interpolation, "CubicSpline" for cubic spline, "Akima1DInterpolator" for akima1DInterpolator, "PchipInterpolator" for pchip_interpolator
        col_datetime (`str`): Column name for column in `df_complete` that holds time series temporal data.
        col_value (`str`): Column name for column in `df_complete` that holds time series value data.

    Returns:
    df_final (pandas.DataFrame): dataframe with `sample_values` for each datetime within desired datetime range and temporal resolution
    """

    # Make a copy of df_complete
    df_complete_in = df_complete.copy()

    # TODO: Rename the `datetime_skeleton` column in function `populate_fill_in_values`
    # Convert the 'datetime_skeleton' column to a datetime.datetime() object
    df_complete_in[col_datetime] = (df_complete_in[col_datetime]).astype(datetime64)

    # assign `x_interp`, `x_obs` and `y_obs` values
    x_interp = df_skeleton["datetime_skeleton"]
    x_obs = df_complete_in[col_datetime]
    y_obs = df_complete_in[col_value]

    # generate interpolated sample values using the `interp_type` specified in function
    sample_value_interp = interp_type(x=x_obs, y=y_obs)(x_interp)

    data = {
        "model_type": interp_type,
        "sample_value_interp": sample_value_interp,
    }

    df_temp = DataFrame(data=data)

    # create a df `df_interp` with concat of `df_skeleton` and `df_temp`
    df_interp = pd_concat([df_skeleton, df_temp], axis=1)

    return df_interp
