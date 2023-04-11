from datetime import datetime, timedelta

from numpy import ceil
from pandas import DataFrame, Timedelta, merge_asof


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

    # PROBLEM 1 fix: look for rows in `df_true_data` that were not included in
    # df_merged)

    # mark which days are available
    df_merged.loc[df_merged[col_value].notna(), "within_tolerance"] = True

    # PROBLEM 3 fix: create column `datetime_skeleton` whose values are the same as `col_datetime`
    # where `within_tolerance`=True and otherwise, are the same as `datetime_proposed`

    # PROBLEM 2 fix: if recalibrate is True, re-define the values of
    # `datetime_skeleton` where `within_tolerance`=False so that the datetime values are
    # equally spaced between the two nearest observed values. If these missing values are
    # at the start or end of the time series, then don't change them.

    # PROBLEM 4 fix: check the conditions and add rows as needed so that the start and end datetimes
    # are accounted for when making predictions

    return df_merged
