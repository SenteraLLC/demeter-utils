# %%
from datetime import datetime, timedelta

import numpy as np
from numpy import ceil
from pandas import DataFrame, Timedelta
from pandas import concat as pd_concat
from pandas import merge_asof, read_csv

df_true_data = read_csv(
    "/root/git/test/df_drone_imagery1.csv",
    parse_dates=["date_observed", "last_updated"],
)

df_true_data = df_true_data
datetime_start = datetime(2022, 5, 6)
datetime_end = datetime(2022, 10, 1)
temporal_resolution = timedelta(days=10)
tolerance_alpha = 0.5
col_datetime = "date_observed"
col_value = "value_observed"
# %%


def find_fill_in_dates(
    df_true_data: DataFrame,
    datetime_start: datetime,
    datetime_end: datetime,
    temporal_resolution: timedelta,
    tolerance_alpha: float = 0.5,
    col_datetime: str = "date_observed",
    col_value: str = "value_observed",
    recalibrate: bool = True,
) -> DataFrame:
    """Given a time series dataframe, determines needed data points to achieve specific temporal resolution.

    Args:
        df_true_data (`DataFrame`): Input ("true") data that is available for a time series.
        datetime_start (`datetime.datetime`): Starting datetime for needed time series.
        datetime_end (`datetime.datetime`): Ending datetime for needed time series.
        temporal_resolution (`datetime.timedelta`): The minimum temporal resolution desired for the output time series.

        tolerance_alpha (`float`): Proportion of `temporal_resolution` to use as `tolerance` when
            performing fuzzy merge on proposed and observed datetime values.

        col_datetime (`str`): Column name for column in `df_true_data` that holds time series temporal data.
        col_value (`str`): Column name for column in `df_true_data` that holds time series value data.

        recalibrate (`bool`): Should the proposed temporal skeleton for the time series be recalibrated
            according to available data?

    Returns:
        `DataFrame` containing desired time series data with date information held in `datetime_need`
        where `available` column indicates whether a value is available (True) or needs to be predicted.
    """

    # rename the data frame
    df_in = df_true_data.copy()

    # determine "length_out" based on temporal resolution
    length_out = int(ceil((datetime_end - datetime_start) / temporal_resolution))

    # create an empty dataframe `df_join` and outline the time windows that need to be represented

    df_join = DataFrame(data=[], columns=["within_tolerance"])
    list_rq_datetime = [
        datetime_start + (temporal_resolution * x) for x in range(length_out + 1)
    ]
    df_join["datetime_proposed"] = list_rq_datetime
    df_join["within_tolerance"] = False

    # do fuzzy match on `col_datetime` based on temporal resolution
    df_merged = merge_asof(
        df_join,
        df_in[[col_datetime, col_value]],
        left_on="datetime_proposed",
        right_on=col_datetime,
        tolerance=Timedelta(temporal_resolution * tolerance_alpha),
        direction="nearest",
    )

    # add the rows from `df_true_data` that were not included in df_merged
    df_merged1 = pd_concat([df_merged, df_in[[col_datetime, col_value]]], axis=0)

    df_merged2 = df_merged1[
        (~df_merged1[col_datetime].duplicated()) | (df_merged1[col_datetime].isnull())
    ].reset_index(drop=True)

    # mark which days are available
    df_merged2.loc[df_merged2[col_value].notna(), "within_tolerance"] = True

    df3 = df_merged2.copy()

    # create column `datetime_skeleton` whose values are the same as `col_datetime`
    # where `within_tolerance`=True and otherwise, are the same as `datetime_proposed`
    # [No consideration of the input  `recalibration``]

    df3["datetime_skeleton"] = df_merged2.apply(
        lambda x: x[col_datetime]
        if x["within_tolerance"] is True
        else x["datetime_proposed"],
        axis=1,
    )

    # sort by "datetime_skeleton"
    df3 = df3.sort_values(by="datetime_skeleton")

    # retun only select column in the output dataframe
    df5 = df3[["within_tolerance", col_datetime, "datetime_skeleton", col_value]].copy()

    ## WHEN INPUT `RECALIBRATE` = `TRUE`
    # # %%
    if recalibrate is True:
        # add 'date_observed_pre' column based on 'within_tolerance' values
        # if the first value of 'within_tolerance' is `False` force the first value of 'date_observed_pre' to 'datetime_skeleton'
        df3["date_observed_pre"] = np.nan

        # NOTE: when executing git commit, it shows error and ask to change `==` to `is`
        if df3.at[0, "within_tolerance"] is False:
            df3.at[0, "date_observed_pre"] = datetime_start

        df3.loc[df3.within_tolerance is True, "date_observed_pre"] = df3.loc[
            df3.within_tolerance is True, "date_observed"
        ]

        df3["date_observed_pre"] = df3["date_observed_pre"].ffill()

        # sort by datetime_skeleton in descending order and add date_observed_pre' column based on 'within_tolerance' values
        df3 = df3.sort_values(by="datetime_skeleton", ascending=False)

        # add date_observed_post' column based on 'within_tolerance' values
        # if the last value of 'within_tolerance' is `False` force the last value of 'date_observed_pre' to corresponding value from 'datetime_skeleton'
        df3["date_observed_post"] = np.nan

        if df3.iat[0, 0] is False:
            df3.iat[0, -1] = df3.iat[0, -3]

        df3.loc[df3.within_tolerance is True, "date_observed_post"] = df3.loc[
            df3.within_tolerance is True, "date_observed"
        ]

        df3["date_observed_post"] = df3["date_observed_post"].ffill()

        # sort by "dt_skeleton"
        df3 = df3.sort_values(by="datetime_skeleton")

        df4 = df3.copy()

        # add two columns to df3 `n_dates_split` and `idx` based on the duplicate value
        df4["idx"] = df4.groupby(
            (df4["date_observed_pre"] != df4["date_observed_pre"].shift(1)).cumsum()
        ).cumcount()
        df4["n_dates_split"] = np.where(
            (df4["idx"] != 0),
            ((df4.groupby(["date_observed_pre"]).transform("size")) - 1),
            0,
        )

        # %% Recalibration function
        def recalibration(
            pre_date: datetime, post_date: datetime, n_dates_split: int, idx: int
        ) -> DataFrame:
            """
            pre_date: a left end of the date for recalibration, where 'true_value' is available
            post_date: a right end of the date for recalibration, where 'true_value' is available
            n_dates_split: number of dates between pre_date and post_date as result recalibration
            idx: index value for the dates obtained from n_dates_split
            """
            part_time_len = (post_date - pre_date) / (n_dates_split + 1)
            parts = []
            marker = pre_date

            for _ in range(n_dates_split):
                part = [marker + part_time_len]
                marker += part_time_len
                parts.append(part)

            recal_dates_ls = parts[idx - 1]
            recal_dates_ls = recal_dates_ls[0]
            return recal_dates_ls

        # this will overwrite the existing column `datetime_skeleton` when recalibrate is True
        df4["datetime_skeleton"] = df4.apply(
            lambda x: x["datetime_skeleton"]
            if x["idx"] == 0
            else recalibration(
                x["date_observed_pre"],
                x["date_observed_post"],
                x["n_dates_split"],
                x["idx"],
            ),
            axis=1,
        )

        df6 = df4[
            ["within_tolerance", col_datetime, "datetime_skeleton", col_value]
        ].copy()

    if recalibrate is True:
        return df6
    else:
        return df5


## need to fix the following things in this function??
# 4) Enforce that df_merged['datetime_skeleton'].max() >= datetime_end and
# df_merged['datetime_skeleton'].min() <= datetime_start.

## TODO: Need fixed
# The last date is missing as it is not able to create an addition row to plug in values in that row
