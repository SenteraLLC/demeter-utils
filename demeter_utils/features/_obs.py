from datetime import datetime
from typing import Any

from numpy import nan
from pandas import DataFrame, Timedelta, merge_asof

from ..query import basic_demeter_query, get_obs_type_and_unit_colname


def get_observation_type_by_date(
    cursor: Any,
    field_id: int,
    reference_date: datetime,
    obs_type_id: int,
    unit_type_id: int,
    date_tol: int = 4,
    include_date: bool = True,
    direction: str = "nearest",
) -> Any:
    """Get observation of `obs_type_id` and `unit_type_id` for `field_id` that is within `date_tol` days of `reference date`.

    If `include_date` is True, include the actual observation date of the observation as a
    column in a two-column Series. Otherwise, only return the observed value as a float.
    If no data matches constraints, then return NA values.

    Args:
        cursor: Connection to Demeter database.
        field_id (int): Field ID to look for observations.
        reference_date (datetime): The reference date to match to observations within `date_tol`
        obs_type_id (int): Observation type ID to find data for.
        unit_type_id (int): Unit type ID to find data for.

        date_tol (tol): Maximum difference between observation date and the reference date to
            consider for the fuzzy match; measured in days and defaults to 4 days.

        include_date (bool): If True, the observation date of the matched observation will be included
            as a column in a Series. Otherwise, only the value is returned. Defaults to True.

        direction (str): Direct reference to `direction` argument in pandas.merge_asof(); from docs:
            "Whether to search for prior ('backward'), subsequent ('forward'), or closest ('nearest')
            matches." Defaults to 'nearest'.
    """

    msg = "`direction` must be 'backward', 'forward', or 'nearest'"
    assert direction in [
        "backward",
        "forward",
        "nearest",
    ], msg

    # figure out column names for final dataframe
    formatted_colname = get_obs_type_and_unit_colname(
        cursor, observation_type_id=obs_type_id, unit_type_id=unit_type_id
    )
    cols_keep = (
        ["date_observed", formatted_colname] if include_date else formatted_colname
    )

    # create dataframe with data constraints
    df_to_match = DataFrame(
        {"field_id": [field_id], "reference_date": [reference_date]}
    )

    # get all observations of this type for this field
    obs_cols = [
        "observation_type_id",
        "unit_type_id",
        "field_id",
        "date_observed",
        "value_observed",
    ]
    df_obs = basic_demeter_query(
        cursor,
        table="observation",
        cols=obs_cols,
        conditions={
            "observation_type_id": obs_type_id,
            "unit_type_id": unit_type_id,
            "field_id": field_id,
        },
    )

    # handle lack of data
    if df_obs is None:
        df_null = DataFrame(nan, index=[0], columns=cols_keep)
        return df_null.loc[0]

    # do fuzzy merge based on given date tolerance
    df_matched = merge_asof(
        df_to_match,
        df_obs.sort_values(["date_observed"]),
        by=["field_id"],
        left_on="reference_date",
        right_on="date_observed",
        tolerance=Timedelta(value=date_tol, unit="days"),
        direction=direction,
    )

    df_matched.rename(columns={"value_observed": formatted_colname}, inplace=True)

    return df_matched.loc[0, cols_keep]
