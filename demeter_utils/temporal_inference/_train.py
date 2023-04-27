from functools import partial
from typing import Callable, Iterable, Tuple, Union

from numpy import datetime64, exp, log, polyfit
from pandas import DataFrame
from pandas.api.types import is_numeric_dtype
from scipy.optimize import minimize


def train_inference_from_df(
    df_reference: DataFrame,
    interp_type: Callable,
    col_datetime: str = "date_start",
    col_value: str = "sample_value",
) -> Callable:
    """
    # Generate a inference function from a reference data based on interpolation type

    Args:
        df_reference (`DataFrame`): Standard ("reference") data.
        interp_type (`Callable`): Function (a model type for interpolation ["CubicSpline", "Akima1DInterpolator" or "PchipInterpolator"]) that takes `datetime` and `sample_value`
        from reference data and `datetime` for desired output data and returns `interpolated sample_value`.

        col_datetime (`str`): Column name for column in `df_reference` that holds time series temporal data.
        col_value (`str`): Column name for column in `df_reference` that holds time series value data.

    Returns the fitted interpolation function
    """

    # copy the `df_reference_ndvi` as `df_forinterp`
    df_forinterp = df_reference.copy()

    # Remove row with NA values in the 'sample_value' column from the data 'df_forinterp'
    df_forinterp = df_forinterp[df_forinterp[col_value].notna()]

    # Convert the 'date_start' column to a datetime.datetime() object
    df_forinterp[col_datetime] = (df_forinterp[col_datetime]).astype(datetime64)

    # assign `x_obs` and `y_obs` values
    x_obs = df_forinterp[col_datetime]
    y_obs = df_forinterp[col_value]

    # define the function
    fx = interp_type(x=x_obs, y=y_obs)

    return fx


def double_sigmoid(
    t: Union[float, Iterable[float]],
    ymin: float,
    ymax: float,
    rate_incr: float,
    rate_decr: float,
    t_incr: float,
    t_decr: float,
) -> Callable:
    """Estimate F(t) where F is double sigmoid function with given parameters.

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

    sigma1 = 1.0 / (1 + exp(-rate_decr * (t - t_decr)))
    sigma2 = 1.0 / (1 + exp(rate_incr * (t - t_incr)))
    y = ymin - (ymax - ymin) * (sigma1 + sigma2 - 1)
    return y


def _estimate_inflection_pt(
    df: DataFrame, col_t: str, col_value: str, increasing: bool, threshold: float = 0.50
):
    ymin = df[col_value].min()

    flag = df[col_value] > (ymin * (1 + threshold))
    df_critical = df.loc[flag].sort_values(by=[col_t])
    t_critical = df_critical[col_t].to_list()

    if increasing:
        return t_critical[0]
    else:
        return t_critical[-1]


def _fit_exponential_rate(x: Iterable, y: Iterable) -> float:
    pars = polyfit(x, log(y), 1)
    return pars[0]


def _estimate_inflection_rate(
    df: DataFrame, col_t: str, col_value: str, increasing: bool
) -> float:
    ymax = df[col_value].max()
    tmax = df.loc[df[col_value] == ymax, col_t].reset_index(drop=True)[0]
    if increasing:
        # get data before `t_max`
        df_subset = df.loc[df[col_t] < tmax]
        tmin = df_subset[col_t].min()
        df_subset["new_t"] = df_subset[col_t] - tmin

        # fit to exponential
        return _fit_exponential_rate(df_subset["new_t"], df_subset[col_value])
    else:
        # get data after `t_max`
        df_subset = df.loc[df[col_t] > tmax]
        tmin = df_subset[col_t].min()
        df_subset["new_t"] = df_subset[col_t] - tmin

        # fit to exponential
        return abs(_fit_exponential_rate(df_subset["new_t"], df_subset[col_value]))


def fit_double_sigmoid(
    df: DataFrame,
    col_t: str,
    col_value: str,
    guess: dict = None,
    inflection_pt_threshold: float = 0.5,
) -> Tuple[dict, Callable]:
    """
    Fit double sigmoid function to estimate `col_value` with respect to `col_datetime` from `df`` and returns the fitted function.

    Args:
        df (`DataFrame): A dataframe with at least two columns `col_datetime` and `col_value`
        col_t (`str`): Column name for column in `df` that holds time series temporal data.
        col_value (`str`): Column name for column in `df` that holds time series value data.

        guess (dict): Optionl dictionary of initial parameter values for `double_sigmoid()`; if None,
            initial values are estimated based on the passed data. See `double_sigmoid()` function arguments
            for more information.

    Returns a tuple which includes a dictionary of the fitted parameters and the fitted double sigmoid function F(T).
    """
    msg = "Datatype of `col_t` is not numeric. Please convert temporal component to a numeric datatype."
    assert is_numeric_dtype(df[col_t]), msg

    msg = "Datatype of `col_value` is not numeric. Double logistic function can only be used to estimate numeric values."
    assert is_numeric_dtype(df[col_value]), msg

    # define x and y for the input dataframe
    x = df[col_t].astype(float)
    y = df[col_value].astype(float)

    # If no initial parameter values are specified, make an educated guess based on the data
    # TODO: Estimate initial values of rates based on data
    if guess is None:
        guess = {
            "ymin": min(y),
            "ymax": max(y),
            "t_incr": _estimate_inflection_pt(
                df,
                col_t,
                col_value,
                increasing=True,
                threshold=inflection_pt_threshold,
            ),
            "t_decr": _estimate_inflection_pt(
                df,
                col_t,
                col_value,
                increasing=False,
                threshold=inflection_pt_threshold,
            ),
            "rate_incr": _estimate_inflection_rate(
                df, col_t, col_value, increasing=True
            ),
            "rate_decr": _estimate_inflection_rate(
                df, col_t, col_value, increasing=False
            ),
        }
        guess_values = [*guess.values()]

    def _cost_function(p, t, y):
        partial_fx = partial(
            double_sigmoid,
            ymin=p[0],
            ymax=p[1],
            t_incr=p[2],
            t_decr=p[3],
            rate_incr=p[4],
            rate_decr=p[5],
        )
        y_pred = partial_fx(t)
        se = (y_pred - y) ** 2
        return se.sum()

    # Minimize cost function
    opt = minimize(_cost_function, guess_values, args=(x, y))
    popt = opt.x

    pars = {
        "ymin": popt[0],
        "ymax": popt[1],
        "t_incr": popt[2],
        "t_decr": popt[3],
        "rate_incr": popt[4],
        "rate_decr": popt[5],
    }

    # Create partial `double_sigmoid` function so it just takes `t`
    partial_fx = partial(
        double_sigmoid,
        ymin=pars["ymin"],
        ymax=pars["ymax"],
        t_incr=pars["t_incr"],
        t_decr=pars["t_decr"],
        rate_incr=pars["rate_incr"],
        rate_decr=pars["rate_decr"],
    )

    return popt, partial_fx, guess_values
