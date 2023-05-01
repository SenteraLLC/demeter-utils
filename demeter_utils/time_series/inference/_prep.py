from datetime import datetime, timedelta

from numpy import ceil
from numpy import nan as np_nan
from pandas import DataFrame, NaT, Timedelta
from pandas import concat as pd_concat
from pandas import merge_asof

from demeter_utils.time_series.inference._utils import (
    _get_df_skeleton_row_template,
    _maybe_fix_duplicate_matches,
)


def _add_missing_rows(
    df: DataFrame,
    df_merged: DataFrame,
    col_datetime: str,
    col_value: str,
    temporal_resolution_min: Timedelta,
    tolerance_alpha: float,
    datetime_start: datetime,
    datetime_end: datetime,
) -> DataFrame:
    """Add the rows from `df` that were not included in `df_merged` (unless outside desired date range)."""
    # Add rows...
    idx_missing = ~df[col_datetime].isin(df_merged[col_datetime])
    df_missing = df.loc[idx_missing][[col_datetime, col_value]]
    df_missing.insert(0, "datetime_proposed", NaT)

    # ...unless they are outside of the desired date range
    tolerance = tolerance_alpha * temporal_resolution_min
    df_missing = df_missing.loc[df_missing[col_datetime] >= datetime_start - tolerance]
    df_missing = df_missing.loc[df_missing[col_datetime] <= datetime_end + tolerance]
    return pd_concat([df_merged, df_missing], axis=0, ignore_index=True)


def _create_df_proposed(
    datetime_start: datetime, datetime_end: datetime, temporal_resolution_min: Timedelta
):
    """Creates df_proposed based on temporal extent and resolution."""
    # determine "length_out" based on temporal resolution
    length_out = int(ceil((datetime_end - datetime_start) / temporal_resolution_min))

    # create an empty dataframe `df_proposed` and outline the time windows that need to be represented
    df_proposed = DataFrame(data=[], columns=["within_tolerance"])
    list_rq_datetime = [
        datetime_start + (temporal_resolution_min * x) for x in range(length_out + 1)
    ]
    # ensure last value of rq_datetime is datetime_end
    list_rq_datetime[-1] = datetime_end

    df_proposed["datetime_proposed"] = list_rq_datetime
    df_proposed["within_tolerance"] = False
    return df_proposed


def _map_observed_datetimes(
    df: DataFrame, col_value: str, col_datetime: str
) -> DataFrame:
    """Adds "within tolerance" and "datetime_skeleton" columns to input DataFrame."""
    # indicate where data is available
    df.loc[df[col_value].notna(), "within_tolerance"] = True
    # create column `datetime_skeleton` whose values are the same as `col_datetime`
    # where `within_tolerance`=True and otherwise, are the same as `datetime_proposed`
    df["datetime_skeleton"] = df.apply(
        lambda row: row[col_datetime]
        if row["within_tolerance"] is True
        else row["datetime_proposed"],
        axis=1,
    )
    row_template = _get_df_skeleton_row_template(col_value)
    cols_keep = list(row_template.keys())
    return df.sort_values(by="datetime_skeleton").reset_index(drop=False)[cols_keep]


def _ensure_full_temporal_extent(
    df: DataFrame, col_value: str, datetime_start: datetime, datetime_end: datetime
):
    """Ensure the full time range is covered."""
    df_out = df.copy()
    row_template = _get_df_skeleton_row_template(col_value)
    if df["datetime_skeleton"].min() > datetime_start:
        first_row = row_template.copy()
        first_row["datetime_skeleton"] = [datetime_start]
        df_out = pd_concat([DataFrame(first_row), df_out], axis=0)

    if df["datetime_skeleton"].max() < datetime_end:
        last_row = row_template.copy()
        last_row["datetime_skeleton"] = [datetime_end]
        df_out = pd_concat([df_out, DataFrame(last_row)], axis=0)
    return df_out


def _recalibrate_date_split(
    datetime_pre: datetime,
    datetime_post: datetime,
    n_dates_split: int,
    idx: int,
) -> datetime:
    """
    Gets `idx` of `n_dates_split` evenly-spaced split points between `datetime_pre` and `datetime_post`.

    Args:
         datetime_pre (datetime): Lower bound of the date range to be split.
         datetime_post (datetime): Upper bound of the date range to be split.
         n_dates_split (int): Number of evenly-spaced split points within date range.
         idx (int): Indicates which split index (starting from 1) should be returned.
    """
    datetime_delta = (datetime_post - datetime_pre) / (n_dates_split + 1)
    list_recalibrated_splits = [
        datetime_pre + (datetime_delta * x) for x in range(1, n_dates_split + 1)
    ]
    return list_recalibrated_splits[idx - 1]


def _recalibrate_datetime_skeleton(df: DataFrame) -> DataFrame:
    """Recalibrate `datetime_skeleton` so missing time points are evenly-spaced between observed time points.
    Args:
    df (DataFrame): Input dataframe to recalibrate based on "datetime_skeleton" and "within_tolerance"
    """
    df_recal = df.copy()
    df_recal.sort_values(by="datetime_skeleton", inplace=True)
    idx_within_tolerance = df_recal["within_tolerance"]

    # create columns to indicate last and next available "observed" dates for each row
    df_recal["datetime_pre"] = np_nan
    df_recal["datetime_post"] = np_nan

    # force ends (if unavailable) to remain as `datetime_start` and `datetime_end`
    if df_recal.at[0, "within_tolerance"] is False:
        df_recal.loc[0, ["datetime_pre", "datetime_post"]] = df_recal.at[
            0, "datetime_skeleton"
        ]

    idx_last = len(df_recal) - 1
    if df_recal.at[idx_last, "within_tolerance"] is False:
        df_recal.loc[idx_last, ["datetime_pre", "datetime_post"]] = df_recal.at[
            idx_last, "datetime_skeleton"
        ]

    # forward fill all NA values in `datetime_pre` with most recent observed date
    df_recal.loc[idx_within_tolerance, "datetime_pre"] = df_recal.loc[
        idx_within_tolerance, "datetime_skeleton"
    ]
    df_recal["datetime_pre"].ffill(inplace=True)

    # do same process for `datetime_post`
    df_recal.sort_values(by="datetime_skeleton", ascending=False, inplace=True)

    df_recal.loc[idx_within_tolerance, "datetime_post"] = df_recal.loc[
        idx_within_tolerance, "datetime_skeleton"
    ]
    df_recal["datetime_post"].ffill(inplace=True)

    # sort once more
    df_recal.sort_values(by="datetime_skeleton", inplace=True)

    # add two columns to indicate num splits (`n_dates_split`) and split index (`idx`)
    df_recal["idx"] = (
        df_recal.groupby(by=["datetime_pre", "datetime_post"]).cumcount() + 1
    )
    df_recal.loc[
        df_recal["datetime_post"] == df_recal["datetime_pre"], "idx"
    ] = 0  # change idx to 0 for matched rows
    df_recal["n_dates_split"] = df_recal.groupby(by=["datetime_pre", "datetime_post"])[
        "idx"
    ].transform("max")
    df_recal["datetime_skeleton"] = df_recal.apply(
        lambda row: row["datetime_skeleton"]
        if row["idx"] == 0
        else _recalibrate_date_split(
            row["datetime_pre"],
            row["datetime_post"],
            row["n_dates_split"],
            row["idx"],
        ),
        axis=1,
    )
    df_recal.drop(
        columns=["datetime_pre", "datetime_post", "n_dates_split", "idx"], inplace=True
    )
    return df_recal


def get_datetime_skeleton(
    df_true: DataFrame,
    datetime_start: datetime,
    datetime_end: datetime,
    temporal_resolution_min: timedelta,
    col_datetime: str = "date_observed",
    col_value: str = "value_observed",
    tolerance_alpha: float = 0.5,
    recalibrate: bool = True,
) -> DataFrame:
    """
    Given a time series dataframe, determines needed "fill-in" time points to achieve specific temporal resolution.

    Args:
        df_true (DataFrame): Input ("true") data that is available for a time series.
        datetime_start (datetime): Starting datetime for needed time series.
        datetime_end (datetime): Ending datetime for needed time series.
        col_datetime (str, optional): Column name for column in `df_true` that holds time series temporal data. Defaults
        to "date_observed".
        col_value (str, optional): Column name for column in `df_true` that holds time series value data. Defaults to
        "value_observed".
        temporal_resolution_min (timedelta, optional): The minimum temporal resolution desired for the output time
        series; if None, the temporal resolution of `col_datetime` in `df_true` is calculated and used as
        `temporal_resolution_min`. Defaults to None.
        tolerance_alpha (float, optional): Proportion of `temporal_resolution` to use as `tolerance` when performing
        fuzzy merge on proposed and observed datetime values. Defaults to 0.5.
        recalibrate (bool, optional): Whether `date_skeleton` dates should be adjusted so they are evenly spaced between
        the previous and subsequent observed dates. Defaults to True. Defaults to True.

    Returns:
        DataFrame: With "datetime_skeleton" column (contains the proposed time series dates) and "within_tolerance"
        column (contains boolean values indicating whether an observed value was available for a given time point).
    """

    df = df_true.copy()
    df_proposed = _create_df_proposed(
        datetime_start, datetime_end, temporal_resolution_min
    )

    # do fuzzy match on `col_datetime` based on temporal resolution
    df_merged = merge_asof(
        df_proposed,
        df[[col_datetime, col_value]],
        left_on="datetime_proposed",
        right_on=col_datetime,
        tolerance=Timedelta(temporal_resolution_min * tolerance_alpha),
        direction="nearest",
    )
    # ensure no values matched twice (happens if date falls along halfway point)
    df_merged = _maybe_fix_duplicate_matches(
        df_merged, col_datetime=col_datetime, col_value=col_value
    )

    df_full = _add_missing_rows(
        df,
        df_merged,
        col_datetime,
        col_value,
        temporal_resolution_min,
        tolerance_alpha,
        datetime_start,
        datetime_end,
    )
    df_describe = _map_observed_datetimes(df_full, col_value, col_datetime)
    if recalibrate:
        df_describe = _recalibrate_datetime_skeleton(df_describe)
    df_out = _ensure_full_temporal_extent(
        df_describe, col_value, datetime_start, datetime_end
    )
    return df_out
