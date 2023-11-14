from typing import Callable, Iterable, Union

from numpy import arange, array, exp, polyfit
from pandas import DataFrame, Series


def _cubic_poly_predict(coef: array, t: float) -> float:
    """Estimates f(t) where f is a fitted cubic polynomial with coefficients `coef`."""
    return (coef[0] * t**3) + (coef[1] * t**2) + (coef[2] * t) + coef[3]


def _cubic_poly_slope(coef: array, t: float) -> float:
    """Estimates f'(t) where f is a fitted cubic polynomial with coefficients `coef`."""
    return (3 * coef[0] * (t**2)) + (2 * coef[1] * t) + coef[2]


def _estimate_inflection_rate(
    coef: array, t_intersect: float, ymax: float, ymin: float
) -> float:
    """Estimates inflection rate by setting the derivative of the double logistic at `t_intersect`
    to f'(t_intersect) and solving for rate parameter, given `ymin` and `ymax`.
    """
    slope = _cubic_poly_slope(coef=coef, t=t_intersect)
    rate = (4 * slope) / (ymax - ymin)
    return abs(rate)


def approximate_inflection_with_cubic_poly(
    t: Series, y: Series, ymin: float, ymax: float
) -> dict:
    """Estimating initial values for inflection point and rate based on a one-sided cubic polynomial curve."""

    # fit a cubic polynomial f(t)
    coef = polyfit(x=t, y=y, deg=3)

    t_fit = arange(start=min(t), stop=max(t), step=0.05)
    df_fit = DataFrame(data={"t": t_fit, "value": _cubic_poly_predict(coef, t_fit)})

    # determine t where f(t) = midpoint
    midpoint = (ymax + ymin) / 2
    intersect_diff_value = abs(df_fit["value"] - midpoint)
    intersect = min(intersect_diff_value)
    intersect_ind = df_fit.loc[intersect_diff_value == intersect].index.values[0]
    t_intersect = df_fit.at[intersect_ind, "t"]

    # estimate inflection rate at intersection point
    rate = _estimate_inflection_rate(
        coef=coef, t_intersect=t_intersect, ymax=ymax, ymin=ymin
    )

    return {"t": t_intersect, "rate": rate}


def double_logistic(
    t: Union[float, Iterable[float]],
    ymin: float,
    ymax: float,
    rate_incr: float,
    rate_decr: float,
    t_incr: float,
    t_decr: float,
) -> Callable:
    """Estimate F(t) where F is double logistic function with given parameters.

    Args:
        t (float):
        ymin (float): minimum y value (e.g. winter NDVI)
        ymax (float): maximum y value (e.g. peak NDVI)
        t_incr (float): t value at first inflection point
        t_decr (float): t value at second inflection point
        rate_incr (float): Rate of increase at t = `t_incr`
        rate_decr (float): Rate of decrease at t = `t_decr`

    Returns F(t).
    """

    fraction1 = 1.0 / (1 + exp(-rate_decr * (t - t_decr)))
    fraction2 = 1.0 / (1 + exp(-rate_incr * (t - t_incr)))
    y = ymin + ((ymax - ymin) * (fraction1 - fraction2))
    return y
