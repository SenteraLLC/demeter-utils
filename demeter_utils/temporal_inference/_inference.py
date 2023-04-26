from typing import Callable

from pandas import DataFrame, Series, to_numeric


def populate_fill_in_values(
    df_skeleton: DataFrame,
    infer_function: Callable,
    col_value: str = "sample_value",
    col_datetime: str = "datetime_skeleton",
) -> DataFrame:
    """
    Generate a dataframe with predicted values given a `df_skeleton` and `infer_function`.

    The `infer_function` should be created such that it can create reasonable predictions for the
    spatiotemporal AOI relevant to `df_skeleton`. This is especially important to keep in mind with regard to
    the date range that `infer_function` has knowledge of. For example, if one wants to train a function on
    2020 data to make 2021 inferences, the inference time series should be adjusted artificially in order to
    match the trained date range/growing season.

    Args:
        df_skeleton (`DataFrame`): Output dataframe from "get_datetime_skeleton_for_ts" function
        infer_function (`Callable`): Function that takes a `datetime` value and returns an inferred value of
        interest for missing values in `df_skeleton`.

    Returns:
        DataFrame:  Replaces NaN values in `col_value` column with inferences from `infer_function` arg.
    """
    df_skeleton_in = df_skeleton.copy()

    # replace the NaN values in `sample_value` column with values in `inference_value` column
    df_skeleton_in[col_value] = df_skeleton_in.apply(
        lambda row: row[col_value]
        if row["within_tolerance"] is True
        else infer_function(to_numeric(Series([row.datetime_skeleton])))[0],
        axis=1,
    )

    # Rename "within_tolerance" and filter columns
    df_skeleton_out = df_skeleton_in.rename(columns={"within_tolerance": "true_data"})[
        ["true_data", col_datetime, col_value]
    ]
    return df_skeleton_out


# NOTE: This is for testing double_sigmoid function. Can be deleted when `double_sigmoid_function` can take `datetime` as input
def populate_fill_in_values_1(
    df_skeleton: DataFrame,
    infer_function: Callable,
    col_value: str = "sample_value",
    col_datetime: str = "datetime_skeleton",
) -> DataFrame:
    """
    Generate a dataframe with predicted values given a `df_skeleton` and `infer_function`.

    The `infer_function` should be created such that it can create reasonable predictions for the
    spatiotemporal AOI relevant to `df_skeleton`. This is especially important to keep in mind with regard to
    the date range that `infer_function` has knowledge of. For example, if one wants to train a function on
    2020 data to make 2021 inferences, the inference time series should be adjusted artificially in order to
    match the trained date range/growing season.

    Args:
        df_skeleton (`DataFrame`): Output dataframe from "get_datetime_skeleton_for_ts" function
        infer_function (`Callable`): Function that takes a `datetime` value and returns an inferred value of
        interest for missing values in `df_skeleton`.

    Returns:
        DataFrame:  Replaces NaN values in `col_value` column with inferences from `infer_function` arg.
    """
    df_skeleton_in = df_skeleton.copy()

    # replace the NaN values in `sample_value` column with values in `inference_value` column
    df_skeleton_in[col_value] = df_skeleton_in.apply(
        lambda row: row[col_value]
        if row["within_tolerance"] is True
        else infer_function(([row.doy_obs]))[0],
        axis=1,
    )

    # Rename "within_tolerance" and filter columns
    df_skeleton_out = df_skeleton_in.rename(columns={"within_tolerance": "true_data"})[
        ["true_data", col_datetime, col_value]
    ]
    return df_skeleton_out
