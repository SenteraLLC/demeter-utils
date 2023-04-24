from typing import Dict

from numpy import nan as np_nan
from pandas import DataFrame, NaT, Timedelta


def get_mean_temporal_resolution(
    df: DataFrame,
    col_subset: str = "true_data",
    col_date="datetime_skeleton",
    subset: bool = None,
) -> Timedelta:
    """
    Calculates mean temporal resolution, with the option to calculate for a bool subset column.

    Args:
        df (DataFrame): Input dataframe.
        col_subset (str, optional): Name of column used for subsetting data (ignored if `subset=None`). Defaults to
        "true_data".
        col_date (str, optional): Name of column that contains datetime information. Defaults to "datetime_skeleton".
        subset (bool, optional): Whether to subset `df` before calculating temporal resolution. If `None`, temporal
        resolution is summarized across all rows. Defaults to None.

    Returns:
        Timedelta: Mean temporal resolution.
    """
    msg = f"{[col_subset, col_date]} must be present in `df`"
    assert set([col_subset, col_date]).issubset(set(df.columns)), msg
    df_true = (
        df[df[col_subset] == subset].copy() if subset in [True, False] else df.copy()
    )
    df_col_date = (
        df_true.groupby([col_subset])[col_date]
        if subset in [True, False]
        else df_true[col_date]
    )
    df_true["timedelta"] = df_true[col_date] - df_col_date.shift()
    df_true["timedelta"] = df_true["timedelta"].apply(lambda x: Timedelta(x))
    return df_true["timedelta"].mean()


def _get_row_template(col_value: str) -> Dict:
    return {
        "within_tolerance": [False],
        "datetime_skeleton": [NaT],
        "datetime_proposed": [NaT],
        col_value: [np_nan],
    }


def _maybe_fix_duplicate_matches(
    df_merged: DataFrame, col_datetime: str, col_value: str
):
    """If an observed value matched more than once to a "proposed" datetime, undo the later match."""
    matched_rows = df_merged[col_datetime].notna()
    duplicated = df_merged.duplicated([col_datetime, col_value], keep="first")
    if any(duplicated & matched_rows):
        df_merged.loc[duplicated & matched_rows, col_datetime] = NaT
        df_merged.loc[duplicated & matched_rows, col_value] = np_nan

    return df_merged
