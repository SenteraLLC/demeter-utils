"""Feature functions that operate at the row level within a feature matrix dataframe.

Current inventory:
- get_applied_fertilizer_until_date_for_field_id()
- get_days_after_planting()
- get_observation_type_by_date()
- get_interpolated_observation_value_at_dap()
- get_area_under_curve_for_observation()
- get_field_group_name()
- get_yield_response_to_application()

What will we need for Phase IV for ABI?
- get_max_ndvi_for_field()
- estimate_growth_stage_on_ref_date()
- get_summarized_weather_variable_by_growth_stage()
- get_summarized_weather_variable_for_season()
- get_soil_for_field_id()
"""

from datetime import datetime
from typing import Any, List, Union

from numpy import linspace, nan, trapz
from pandas import NA, DataFrame, Series, Timedelta, merge_asof
from scipy.interpolate import interp1d

from ..query import basic_demeter_query, get_as_applied, get_obs_type_and_unit_colname
from ._utils import add_feature


def get_applied_fertilizer_until_date_for_field_id(
    cursor: Any,
    field_id: int,
    date_limit: datetime,
    nutrient: str,
    method: Union[str, None] = None,
) -> float:
    """For a given field ID, gets sum of fertilizer applied for a specified nutrient and, if given, method up until a specified date.

    Args:
        cursor: Connection to Demeter database.
        field_id (int): Demeter field ID to find application data for.
        date_limit (datetime): Reference date to sum application rate information up until to.
        nutrient (str): Applied nutrient to consider.
        method (str): Application method to consider; if not given, all methods are considered.
    """
    # get all fertilizer activity for this `field_id`
    df_applied_full = get_as_applied(
        cursor, field_id=field_id, colname_date="date_performed"
    )

    # reduce based on end date
    df_apps = df_applied_full.loc[df_applied_full["date_performed"] < date_limit]

    # if no rows left, then return 0
    if len(df_apps) == 0:
        return 0

    # filter on nutrient and method (if available)
    df_apps = df_apps.loc[df_apps["nutrient"] == nutrient]
    if method is not None:
        df_apps = df_apps.loc[df_apps["method"] == method]

    return float(df_apps["rate_lbs_acre"].sum())


def get_days_after_planting(
    cursor: Any, field_id: int, reference_date: datetime, keep: str = "first"
) -> Union[float, int]:
    """Gets number of days between `reference_date` and planting date for a given field ID.

    If more than one planting date is available for a field ID, the following
    logic is used to select the used planting date based on ``keep``.

    'first': The first planting date available for this field ID is used.
    'last': The last available planting date for this field ID is used.

    Args:
        cursor (Any): Connection to Demeter database.
        field_id (int): Field ID to determine planting date.
        reference_date (datetime): The date to compare to the planting date of the field.
        keep (str): Logic to handle more than one planting date for a given field ID. Default is "first".
    """
    assert keep in ["first", "last"], "`keep` must be 'first' or 'last'"

    df_planted = basic_demeter_query(
        cursor,
        table="act",
        cols=["date_performed"],
        conditions={"act_type": "plant", "field_id": field_id},
    ).sort_values("date_performed")

    if len(df_planted) == 0:
        return nan

    elif (len(df_planted) == 1) or keep == "first":
        ind = 0
    else:
        ind = len(df_planted) - 1

    planting_date = df_planted.iloc[ind, 0]
    assert isinstance(planting_date, datetime)
    diff = reference_date - planting_date

    return diff.days


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


def get_interpolated_observation_value_at_dap(
    cursor: Any,
    field_id: int,
    obs_type_id: int,
    unit_type_id: int,
    target_dap: Union[List[float], float],
    kind: str = "linear",
) -> Series:
    """For given DAP values, interpolate values of an observation type for field ID.

    If passed DAP values in `target_dap` are outside of the interpolation bounds (i.e., before or
    after all available observations), an NA value is returned.

    Args:
        cursor: Connection to Demeter database
        field_id (int): Field ID to find observations for
        obs_type_id (int): Observation type ID to find observations for
        unit_type_id (int): Unit type ID to find observations for
        target_dap (list(float) or float): Values of days after planting on which to interpolate values

        kind (str): Direct reference to `kind` argument in scipy.interpolate.interp1d which
            dictates interpolation method
    """
    if isinstance(target_dap, list) is False:
        target_dap = [target_dap]  # make integers into lists

    # get observation data for field ID
    df_obs = basic_demeter_query(
        cursor,
        table="observation",
        cols=["date_observed", "value_observed"],
        conditions={
            "field_id": field_id,
            "observation_type_id": obs_type_id,
            "unit_type_id": unit_type_id,
        },
    )

    # add "days after planting"
    df_obs["dap"] = add_feature(
        df=df_obs.copy(),
        fx=get_days_after_planting,
        cols_to_args={"reference_date": "date_observed"},
        constant_args={"cursor": cursor, "field_id": field_id},
    )

    # fit interpolation function
    fx_interpolated = interp1d(df_obs["dap"], df_obs["value_observed"], kind=kind)
    df_inter = DataFrame(data={"dap": target_dap})
    df_inter["value"] = [
        float(fx_interpolated(val))
        if df_obs["dap"].min() <= val <= df_obs["dap"].max()
        else NA
        for val in target_dap
    ]

    # interpolate for desire DAP values
    df_inter["index"] = df_inter["dap"].map(lambda val: f"value_{val}")
    df_inter.set_index("index", inplace=True)
    df_inter.drop(columns="dap", inplace=True)

    # organize into series
    df_final = df_inter.transpose().reset_index(drop=True)
    df_final.columns.name = None
    df_final.index.name = None

    return df_final.loc[0]


def get_area_under_curve_for_observation(
    cursor: Any,
    field_id: int,
    obs_type_id: int,
    unit_type_id: int,
    interpolate: bool = False,
    dap_bounds: List[float] = None,
    kind: str = "linear",
    n_dx: int = 1000,
) -> Series:
    """Calculate the area under the curve for observation type for field ID.

    If `interpolate` is True, an interpolation function of kind `kind` is fitted first. Then, using `n_dx`
    subdivisions from the minimum available DAP to last available DAP, the trapezoidal method is used
    to calculate the area under the curve.

    If `interpolate` is False, the trapezoidal method is used to measure the area under the curve
    where the subdivisions are determined by available observations.

    Args:
        cursor: Connection to Demeter database
        field_id (int): Field ID to find observations for
        obs_type_id (int): Observation type ID to find observations for
        unit_type_id (int): Unit type ID to find observations for

        interpolate (bool): If `True`, interpolation of available observations is completed before
            integration; default is `False`.

        dap_bounds (list of float): Enforced DAP bounds for interpolation and calculation of AUC if interpolate is `True`

        kind (str): Direct reference to `kind` argument in scipy.interpolate.interp1d which
            dictates interpolation method and integrated function if `interpolate` is `True`.

        n_dx (int): Number of subdivisions to interpolate for the observations if `interpolate` is `True`.

    Returns pandas.Series with the following columns:
    -- "auc_value": estimated area under the curve
    -- "auc_dap_min": minimum DAP value with available observation
    -- "auc_dap_max": maximum DAP value wtih available observation
    -- "min_value": minimum observed value of `obs_type_id`
    -- "max_value": maximum observed value of `obs_type_id`
    """

    if dap_bounds:
        assert isinstance(dap_bounds, list), "`dap_bounds` must be passed as a list."
        assert len(dap_bounds, 2), "`dap_bounds` must have length of two."
        assert (
            dap_bounds[0] < dap_bounds[-1]
        ), "The first item of `dap_bounds` must be less than the second."

    # get observation data for field ID
    df_obs = basic_demeter_query(
        cursor,
        table="observation",
        cols=["date_observed", "value_observed"],
        conditions={
            "field_id": field_id,
            "observation_type_id": obs_type_id,
            "unit_type_id": unit_type_id,
        },
    )

    # add "days after planting"
    df_obs["dap"] = add_feature(
        df=df_obs.copy(),
        fx=get_days_after_planting,
        cols_to_args={"reference_date": "date_observed"},
        constant_args={"cursor": cursor, "field_id": field_id},
    )
    df_obs.sort_values("dap", inplace=True)

    # maybe fit interpolation function and then integrate
    if interpolate:
        fx_interpolated = interp1d(df_obs["dap"], df_obs["value_observed"], kind=kind)
        if dap_bounds is None:
            xx = linspace(start=df_obs["dap"].min(), stop=df_obs["dap"].max(), num=n_dx)
        else:
            xx = linspace(start=dap_bounds[0], stop=dap_bounds[1], num=n_dx)
        yy = fx_interpolated(xx)
        auc = trapz(y=yy, x=xx)
    else:
        auc = trapz(y=df_obs["value_observed"].to_list(), x=df_obs["dap"].to_list())

    df_final = DataFrame(
        data={
            "auc_value": [auc],
            "auc_dap_min": [df_obs["dap"].min()],
            "auc_dap_max": [df_obs["dap"].max()],
            "min_value": [df_obs["value_observed"].min()],
            "max_value": [df_obs["value_observed"].max()],
        }
    )
    return df_final.loc[0]


def get_field_group_name(cursor: Any, field_group_id: int) -> str:
    """Gets name of field group name based on ID."""
    return basic_demeter_query(
        cursor,
        table="field_group",
        cols=["name"],
        conditions={"field_group_id": field_group_id},
    )["name"].item()


def get_yield_response_to_application(
    control_yield: float,
    treatment_yield: float,
    treatment_application_rate: float,
    control_application_rate: float = 0,
) -> float:
    """Calculates yield response to an application rate relative to a `control_yield`.

    If difference in application rate between treatment and control is 0, then NA is returned.

    Args:
        control_yield (float): Reference value of yield to which `treatment_yield` should be compared (often the mean control yield for a site);
            should be given in units of mass or volume per acre (e.g., bu/acre).

        treatment_yield (float): Value of yield measured for treatment area, where response should be measured; should be given in units of mass
            or volume per acre (e.g., bu/acre).

        treatment_application_rate (float): Application rate of product applied to treatment area; should be given in units normalized by acre

        control_application_rate (float): Application rate of product applied to control area; should be given in the same units as `treatment_application_rate`.
            Value defaults to 0.
    """
    rate_diff = treatment_application_rate - control_application_rate
    if rate_diff > 0:
        return (treatment_yield - control_yield) / rate_diff
    else:
        return nan
