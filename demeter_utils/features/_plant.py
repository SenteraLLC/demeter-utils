from datetime import datetime
from typing import Any, List, Union

from numpy import nan, trapz
from pandas import NA, DataFrame, Series
from scipy.interpolate import interp1d

from ..query.demeter._core import basic_demeter_query
from ._utils import add_feature, interpolate_time_series


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
        xx, yy = interpolate_time_series(
            x=df_obs["dap"].to_list(),
            y=df_obs["value_observed"].to_list(),
            kind=kind,
            x_bounds=dap_bounds,
            n_dx=n_dx,
        )
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
