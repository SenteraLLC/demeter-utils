from typing import Union

from pandas import DataFrame
from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

# Step 1: Load data that already exists (that may or may not represent an entire growing season)


def generate_fill_in_values_a(
    df_true_data: DataFrame, temporal_resolution: int, starttime: int, endtime: int
) -> DataFrame:
    """
    # Step 2a: Given an argument `temporal_resolution`, determine which times/days are absent from input data (step 1)

    Args:
        df_true_data (`DataFrame`): Input ("true") data.
        temporal_resolution (`int`): The minimum temporal resolution of the combined "true" and "fill-in" data.
        starttime (`int`): The relative starting date for which data are required.
        endtime (`int`): The relative end date for which data are required.

    Returns:
        `DataFrame`: Whether "true" data are available for a given temporal resolution and date range (True or False).
    """
    print("Hello A")
    df_inventory = DataFrame()

    return df_inventory

    # Example:
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
    #             60      False
    #             70      False
    #             80      False
    #             90      False
    #             100     False


def generate_fill_in_values_b(
    df_inventory: DataFrame,
    interp_func: Union[Akima1DInterpolator, CubicSpline, PchipInterpolator],
) -> DataFrame:
    """
    # Step 2b: Given a fitted interpolation function, generate "fill-in" data for all absent dates

    Returns:
        `DataFrame`: Keeps "true" data alone, but interpolates for all required "fill-in" dates.
    """
    print("Hello B")


def fit_on_complete_data(
    df_inventory2: DataFrame,
    unfitted_interp_func: Union[Akima1DInterpolator, CubicSpline, PchipInterpolator],
) -> Union[Akima1DInterpolator, CubicSpline, PchipInterpolator]:
    """
    Step 3: Given a "complete" dataset for the desired temporal resolution (likely includes both
    "true" and "fill-in"), fit and return a new interpolation function on the "complete" dataset
    """
    print("Hello Step 3")
