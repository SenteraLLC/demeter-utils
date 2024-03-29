from typing import Callable, Dict

from numpy import nan as np_nan
from pandas import DataFrame, NaT, to_numeric
from pandas.api.types import is_numeric_dtype


def _get_df_skeleton_row_template(col_value: str) -> Dict:
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


def populate_fill_in_values(
    df_skeleton: DataFrame,
    infer_function: Callable,
    col_value: str = "sample_value",
    col_datetime: str = "datetime_skeleton",
) -> DataFrame:
    """
    Generate a dataframe with predicted values given `df_skeleton` and `infer_function`.

    The `infer_function` should be created such that it can create reasonable predictions for the
    spatiotemporal AOI relevant to `df_skeleton`. This is especially important to keep in mind with regard to
    the date range that `infer_function` has knowledge of. For example, if one wants to train a function on
    2020 data to make 2021 inferences, the inference time series should be adjusted artificially in order to
    match the trained date range/growing season (or vice versa).

    Args:
        df_skeleton (`DataFrame`): Output dataframe from "get_df_skeleton" function
        infer_function (`Callable`): Function that takes a `datetime` value and returns an inferred value of
        interest for missing values in `df_skeleton`.

    Returns:
        DataFrame:  Replaces NaN values in `col_value` column with inferences from `infer_function` arg.
    """
    df_skeleton_in = df_skeleton.copy()

    if not is_numeric_dtype(df_skeleton_in[col_datetime]):
        df_skeleton_in["t"] = to_numeric(df_skeleton_in[col_datetime])
    else:
        df_skeleton_in["t"] = df_skeleton_in[col_datetime]

    # replace the NaN values in `sample_value` column with values in `inference_value` column
    df_skeleton_in[col_value] = df_skeleton_in.apply(
        lambda row: row[col_value]
        if row["within_tolerance"] is True
        else infer_function(row["t"]),
        axis=1,
    )

    df_skeleton_in.drop(columns=["t"], inplace=True)

    # Rename "within_tolerance" and filter columns
    df_skeleton_out = df_skeleton_in.rename(columns={"within_tolerance": "true_data"})[
        ["true_data", col_datetime, col_value]
    ]
    return df_skeleton_out
