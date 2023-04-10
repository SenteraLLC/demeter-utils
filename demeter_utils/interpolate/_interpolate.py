from datetime import datetime

import numpy as np
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
