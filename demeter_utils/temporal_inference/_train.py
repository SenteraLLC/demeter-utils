from typing import Callable

from numpy import datetime64
from pandas import DataFrame


def get_inference_fx_from_df_reference(
    df_reference: DataFrame,
    interp_type: str,
) -> Callable:
    """
    # Generate a inference function from a reference data based on interpolation type

    Args:
        df_reference (`DataFrame`): Standard ("reference") data.
        interp_type (`str`): Model type for interpolation, "CubicSpline" for cubic spline, "Akima1DInterpolator" for akima1DInterpolator, "PchipInterpolator" for pchip_interpolator

    Returns the fitted interpolation function
    """

    # copy the `df_reference_ndvi` as `df_forinterp`
    df_forinterp = df_reference.copy()

    # Remove row with NA values in the 'sample_value' column from the data 'df_forinterp'
    df_forinterp = df_forinterp[df_forinterp["sample_value"].notna()]

    # Convert the 'date_start' column to a datetime.datetime() object
    df_forinterp["date_start"] = (df_forinterp["date_start"]).astype(datetime64)

    # assign `x_obs` and `y_obs` values
    x_obs = df_forinterp["date_start"]
    y_obs = df_forinterp["sample_value"]

    # create the `infer_fucntion` function
    def infer_function(x_interp):
        return interp_type(x=x_obs, y=y_obs)(x_interp)

    return infer_function
