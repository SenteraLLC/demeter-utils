from typing import Callable

from numpy import datetime64
from pandas import DataFrame


def train_inference_from_df(
    df_reference: DataFrame,
    interp_type: Callable,
    col_datetime: str = "date_start",
    col_value: str = "sample_value",
) -> Callable:
    """
    # Generate a inference function from a reference data based on interpolation type

    Args:
        df_reference (`DataFrame`): Standard ("reference") data.
        interp_type (`Callable`): Function (a model type for interpolation ["CubicSpline", "Akima1DInterpolator" or "PchipInterpolator"]) that takes `datetime` and `sample_value`
        from reference data and `datetime` for desired output data and returns `interpolated sample_value`.

        col_datetime (`str`): Column name for column in `df_reference` that holds time series temporal data.
        col_value (`str`): Column name for column in `df_reference` that holds time series value data.

    Returns the fitted interpolation function
    """

    # copy the `df_reference_ndvi` as `df_forinterp`
    df_forinterp = df_reference.copy()

    # Remove row with NA values in the 'sample_value' column from the data 'df_forinterp'
    df_forinterp = df_forinterp[df_forinterp[col_value].notna()]

    # Convert the 'date_start' column to a datetime.datetime() object
    df_forinterp[col_datetime] = (df_forinterp[col_datetime]).astype(datetime64)

    # assign `x_obs` and `y_obs` values
    x_obs = df_forinterp[col_datetime]
    y_obs = df_forinterp[col_value]

    # define the function
    fx = interp_type(x=x_obs, y=y_obs)

    return fx
