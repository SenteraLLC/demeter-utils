from datetime import datetime
from typing import Union

import numpy as np
from pandas import DataFrame
from pandas import concat as pd_concat
from scipy.interpolate import Akima1DInterpolator, CubicSpline, PchipInterpolator

# from pandas import merge as pd_merge
# from pandas import read_csv
#    """"
# Step 1: Load data that already exists (that may or may not represent an entire growing season)

#     The dataset should have at least 'doy' and 'value_observed' columns
#    """


# %% Functions
def find_fill_in_dates(
    df_true_data: DataFrame, starttime: int, endtime: int, temporal_resolution: int
) -> DataFrame:
    """
    # Step 2a: Given an argument `temporal_resolution`, determine which times/days are absent from input data (step 1)

    Args:
        df_true_data (`DataFrame`): Input ("true") data.
        starttime (`int`): The relative starting date for which data are required.
        endtime (`int`): The relative end date for which data are required.
        temporal_resolution (`int`): The minimum temporal resolution of the combined "true" and "fill-in" data.

    Returns:
        `DataFrame`: Whether "true" data are available for a given temporal resolution and date range (True or False).
    """
    df_in = df_true_data

    date_plant = datetime(2022, 5, 1)
    df_in["dap"] = (df_in["date_observed"] - date_plant).dt.days

    df_join = DataFrame(data=[], columns=df_in.columns)
    df_join["dap"] = np.arange(
        np.timedelta64(starttime, "D"),
        np.timedelta64(endtime, "D"),
        np.timedelta64(temporal_resolution, "D"),
    ).astype(np.timedelta64)
    df_join["dap"] = df_join["dap"].dt.days

    df_inventory = (
        pd_concat([df_in, df_join])
        .drop_duplicates(subset=["dap"], keep="first")
        .reset_index(drop=True)
    )

    available = []
    for i in df_inventory["value_observed"]:
        if i <= 1:
            available.append("True")
        else:
            available.append("False")

    df_inventory["available"] = available

    return df_inventory


# %%
def generate_fill_in_values_b(
    df_inventory: DataFrame,
    interp_func: Union[Akima1DInterpolator, CubicSpline, PchipInterpolator],
) -> DataFrame:
    """
    # Step 2b: Given a fitted interpolation function, generate "fill-in" data for all absent dates

    Returns:
        `DataFrame`: Keeps "true" data alone, but interpolates for all required "fill-in" dates.
    """

    # df["value"] = df["value"].fillna(0)

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
