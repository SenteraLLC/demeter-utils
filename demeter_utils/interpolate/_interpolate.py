from datetime import datetime, timedelta

from numpy import ceil
from pandas import DataFrame, Timedelta, merge_asof


def find_fill_in_dates(
    df_true_data: DataFrame,
    datetime_start: datetime,
    datetime_end: datetime,
    temporal_resolution: timedelta,
    col_datetime: str = "date_observed",
    col_value: str = "value_observed",
) -> DataFrame:
    """Given a time series dataframe, determines needed data points to achieve specific temporal resolution.

    Args:
        df_true_data (`DataFrame`): Input ("true") data that is available for a time series.
        datetime_start (`datetime.datetime`): Starting datetime for needed time series.
        datetime_end (`datetime.datetime`): Ending datetime for needed time series.
        temporal_resolution (`datetime.timedelta`): The minimum temporal resolution desired for the output time series.
        col_datetime (`str`): Column name for column in `df_true_data` that holds time series temporal data.
        col_value (`str`): Column name for column in `df_true_data` that holds time series value data.

    Returns:
        `DataFrame` containing desired time series data with date information held in `datetime_need`
        where `available` column indicates whether a value is available (True) or needs to be predicted.
    """
    # msg = "You must specify `temporal_resolution` or `length_out` to determine fill-in cadence."
    # assert (temporal_resolution is not None) or (length_out is not None), msg

    # rename the data frame
    df_in = df_true_data.copy()

    # determine "length_out" based on temporal resolution
    length_out = int(ceil((datetime_end - datetime_start) / temporal_resolution))

    # create an empty dataframe `df_join` and outline the time windows that need to be represented`
    df_join = DataFrame(data=[], columns=["available"])
    list_rq_datetime = [
        datetime_start + (temporal_resolution * x) for x in range(length_out + 1)
    ]
    df_join["datetime_need"] = list_rq_datetime
    df_join["available"] = False

    # do fuzzy match on `col_datetime` based on temporal resolution
    df_merged = merge_asof(
        df_join,
        df_in[[col_datetime, col_value]],
        left_on="datetime_need",
        right_on=col_datetime,
        tolerance=Timedelta(temporal_resolution / 2),
        direction="nearest",
    )

    # mark which days are available
    df_merged.loc[df_merged[col_value].notna(), "available"] = True

    return df_merged
