from datetime import datetime, timedelta

import numpy as np
from numpy import ceil
from pandas import DataFrame, Timedelta
from pandas import concat as pd_concat
from pandas import merge_asof, read_csv

# from demeter_utils.interpolate._interpolate import find_fill_in_dates

# %%
# load the true data
# TODO: read data from cloud
# data available at sharepoint:https://sentera.sharepoint.com/:x:/s/demeter/ESR0PKnkjQBIkYDkT9NBVS8B1h5kJHTbJE2tCLgM7QWP7A?e=ioUoFB

df_true_data = read_csv(
    "/root/git/test/df_drone_imagery1.csv",
    parse_dates=["date_observed", "last_updated"],
)

datetime_start = datetime(2022, 5, 6)
datetime_end = datetime(2022, 10, 1)
temporal_resolution = timedelta(days=10)
tolerance_alpha = 0.5
col_datetime = "date_observed"
col_value = "value_observed"
recalibrate = True


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

    ## We need to fix the following things in this function:

    # 1) After doing the merge, add back the rows from `df_in` that were not matched.
    # For example, in the sample data that you are using, the row from 6-30-2022 is not given in
    # `df_merged`.

    # 2) Implement the `recalibrate` argument which, if True, will recalibrate the values of
    # `datetime_proposed` column according to the two closest observed dates. This step will
    # require that we have all of the original data in `df_merged`.

    # 3) Add `datetime_skeleton` column which will consist of the observed datetime values and the
    # datetime values where will we need to make inferences

    # 4) Enforce that df_merged['datetime_skeleton'].max() >= datetime_end and
    # df_merged['datetime_skeleton'].min() <= datetime_start.

    # rename the data frame
    df_in = df_true_data.copy()

    # determine "length_out" based on temporal resolution
    length_out = int(ceil((datetime_end - datetime_start) / temporal_resolution))

    # create an empty dataframe `df_join` and outline the time windows that need to be represented`
    # TODO: Question: Do we want to retain other columns in the original dataframe or not???

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

    # %%
    # TODO: Delete this chunk later [Reseting the first and last values of within_tolerance as True]
    df_merged2.at[0, "within_tolerance"] = True
    df_merged2.at[0, "date_observed"] = df_merged2.at[0, "datetime_proposed"]

    # %%

    # create column `datetime_skeleton` whose values are the same as `col_datetime`
    # where `within_tolerance`=True and otherwise, are the same as `datetime_proposed`

    df3 = df_merged2.copy()

    # create dt_skeleton with considering the recalibration
    # [Should this be executed only if `recalibrate = False` or is fine in any case]

    df3["dt_skeleton"] = df_merged2.apply(
        lambda x: x[col_datetime]
        if x["within_tolerance"] is True
        else x["datetime_proposed"],
        axis=1,
    )

    # sort by "dt_skeleton"
    df3 = df3.sort_values(by="dt_skeleton")

    # add date_observed_last' column based on 'within_tolerance' values
    df3["date_observed_last"] = np.nan
    df3.loc[df3.within_tolerance is True, "date_observed_last"] = df3.loc[
        df3.within_tolerance is True, "date_observed"
    ]
    df3["date_observed_last"] = df3["date_observed_last"].ffill()

    # TODO: set the NaT values in 'date_observed_last' column with date from the first row of 'dt_skeleton'
    # df3["date_observed_last"].fillna((df3.at[0,"dt_skeleton"]))

    # sort by dt_skeleton in descending order and add date_observed_last' column based on 'within_tolerance' values
    df3 = df3.sort_values(by="dt_skeleton", ascending=False)

    # add date_observed_ahead' column based on 'within_tolerance' values
    df3["date_observed_ahead"] = np.nan
    df3.loc[df3.within_tolerance is True, "date_observed_ahead"] = df3.loc[
        df3.within_tolerance is True, "date_observed"
    ]
    df3["date_observed_ahead"] = df3["date_observed_ahead"].ffill()

    # TODO: set the NaT values 'date_observed_ahead' column with date from the last row of 'dt_skeleton"

    # sort by "dt_skeleton"
    df3 = df3.sort_values(by="dt_skeleton")

    # %% Recalibration function
    def recalibration(
        pre_date: datetime, post_date: datetime, splits_num: int
    ) -> DataFrame:
        """
        pre_date: a left end of the date for recalibration, where 'true_value' is available
        post_date: a right end of the date for recalibration, where 'true_value' is available
        splits_num: number of split section desired
        """
        part_time_len = (post_date - pre_date) / splits_num
        parts = []
        marker = pre_date

        for _ in range(splits_num):
            part = [marker + part_time_len]
            marker += part_time_len
            parts.append(part)

        recal_dates_ls = parts[:-1]

        return recal_dates_ls

    if recalibrate is True:
        df3["datetime_skeleton"] = df3.apply(
            lambda x: x[col_datetime]
            if x["within_tolerance"] is True
            else recalibration(x["date_observed_last"], x["date_observed_ahead"], 2),
            axis=1,
        )

    # # Example use of recalibration function
    # recal_dates_ls = recalibration(
    #     pre_date= datetime(2021,3,15),
    #     post_date= datetime(2021,3,25),
    #     splits_num= 3,
    #     )

    # # append the `recal_dates_ls` to column `dt_skeleton`
    # df3 = df3.append(pd.DataFrame(recal_dates_ls, columns=["dt_skeleton"]), ignore_index = True)

    # TODO: Relabel first and last value of 'within_tolerance' as True for ease of operating
    # with NaT values in 'date_observed_last'/ 'date_observed_ahead' / date_observed

    # %%
    # PROBLEM 2 fix: if recalibrate is True, re-define the values of
    # `datetime_skeleton` where `within_tolerance`= False so that the datetime values are
    # equally spaced between the two nearest observed values. If these missing values are
    # at the start or end of the time series, then don't change them.

    # PROBLEM 4 fix: check the conditions and add rows as needed so that the start and end datetimes
    # are accounted for when making predictions

    return df3
