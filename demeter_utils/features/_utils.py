from typing import Any, List, Mapping, Union

from numpy import linspace, ndarray
from pandas import DataFrame, Series
from scipy.interpolate import interp1d

from ..query.demeter._core import basic_demeter_query


def interpolate_time_series(
    x: Series,
    y: Series,
    kind: str = "linear",
    x_bounds: List[float] = None,
    n_dx: int = 1000,
) -> tuple[ndarray, ndarray]:
    """Interpolates `y` over `x` using `scipy.interpolate` at `n_dx` intervals.

    Interpolation bounds are imposed by `x_bounds` if given. Otherwise, `y` is interpolated
    between the minimum and maximum values of `x`.

    Args:
        x (list): X values
        y (list): Y values where Y is f(x)

        kind (str): Direct reference to `kind` argument in scipy.interpolate.interp1d which
            dictates interpolation method and integrated function. Defaults to "linear".

        x_bounds (list of float): Enforced interpolation bounds for values of `x`
        n_dx (int): Number of subdivisions to interpolate for the observations.

    Returns tuple of 2 arrays of length `n_dx` where the first is the array of `x` values where `y` was
    interpolated and the second is the estimated values of `y`.
    """
    if x_bounds:
        assert isinstance(x_bounds, list), "`x_bounds` must be passed as a list."
        assert len(x_bounds, 2), "`x_bounds` must have length of two."
        assert (
            x_bounds[0] < x_bounds[-1]
        ), "The first item of `x_bounds` must be less than the second."

    fx_interpolated = interp1d(x, y, kind=kind)
    if x_bounds is None:
        xx = linspace(start=min(x), stop=max(x), num=n_dx)
    else:
        xx = linspace(start=x_bounds[0], stop=x_bounds[1], num=n_dx)
    yy = fx_interpolated(xx)

    return xx, yy


# TODO: Could (should) this function be performed in place?
def add_feature(
    df: DataFrame,
    fx: Any,
    cols_to_args: Union[None, Mapping[str, str]] = None,
    constant_args: Union[None, Mapping[str, Any]] = None,
) -> Union[Any, None]:
    """
    Takes a row-level function with arguments and performs the function on each row of `df`.

    Function arguments are populated either by columns of `df` (mapped by `cols_to_args`) or
    specified constants (mapped by `constant_args`).

    Args:
        df (pandas.DataFrame): Dataframe to perform row-level action on
        fx: Row-level function to perform
        cols_to_args (dict): Dictionary mapping fx argument names to `df` columns
        constant_args (dict): Dictionary mapping fx argument names to desired constants
    """

    args: Mapping[str, Any] = {} if constant_args is None else constant_args

    if cols_to_args is None:
        cols_to_args = {}

    def generalized_fx(
        row: Series,  # type: ignore
        fx: Any,
        cols_to_args: Mapping[str, str],
    ) -> Any:
        if len(cols_to_args) > 0:
            for key in cols_to_args.keys():
                args[key] = row[cols_to_args[key]]

        return fx(**args)

    return df.apply(func=lambda row: generalized_fx(row, fx, cols_to_args), axis=1)


def get_field_group_name(cursor: Any, field_group_id: int) -> str:
    """Gets name of field group name based on ID."""
    return basic_demeter_query(
        cursor,
        table="field_group",
        cols=["name"],
        conditions={"field_group_id": field_group_id},
    )["name"].item()
