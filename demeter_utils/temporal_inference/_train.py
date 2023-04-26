from datetime import datetime
from typing import Callable

from numpy import around, array, datetime64, exp, percentile
from pandas import DataFrame
from scipy.optimize import curve_fit


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


# double sigmoid function [CASE 1: Generate y_pred based on input model parameters]]
def dbl_sigmoid_function_1(params: list, t: datetime):
    """The double sigmoid function defined over t (where t is an array).
    params = list of 6 parameters
    where, p0 = value_observed at planting/emergence,
    p1 = max value_observed at peak growing season
    p2 = rate of increase of value_observed at p3
    p3 = x-value (doy_obs) for which p2 is calculated
    p4 = rate of decrease of value_observed at p5
    p5 = x-value (doy_obs) for which p4 is calcualted

    t = array of datetime #TODO: Curently `doy` is used instead of `datetime`. Need to make it work with datetime

    Returns an array of predicted/fitted values
    """

    sigma1 = 1.0 / (1 + exp(params[2] * (t - params[3])))
    sigma2 = 1.0 / (1 + exp(-params[4] * (t - params[5])))
    y = params[0] - params[1] * (sigma1 + sigma2 - 1)
    return y


# double sigmoid function [Case 2: Generate model parameters from given standard data and plug in those parameters to generate a refined function]
def dbl_sigmoid_function_2(
    df=DataFrame,
    col_datetime=str,
    col_value=str,
    guess=None,
) -> Callable:
    """
    Perform double logistic regression of `col_value` with respect to `col_datetime` from given dataframe and retun a fitted function.

    Parameters:
    df (`DataFrame): A dataframe with at least two columns `col_datetime` and `col_value`
    col_datetime (`str`): Column name for column in `df` that holds time series temporal data.
    col_value (`str`): Column name for column in `df` that holds time series value data.

    guess (dict): dictionary of initial parameter values (optional)
    where,
    p0 = value_observed at planting/emergence [min value on left tail]
    p1 = max value_observed at peak growing season
    p2 = rate of increase of value_observed at p3
    p3 = x-value (doy_obs) for which p2 is calculated
    p4 = rate of decrease of value_observed at p5
    p5 = x-value (doy_obs) for which p4 is calcualted

    Returns:
    A fitted double logistic regression function
    """
    # TODO: The col_datetime currently only takes 'DOY' values. Some work is needed to make the code work to take `datetime` as input

    # define x and y for the input dataframe
    x = df[col_datetime].astype(float)
    y = df[col_value]

    # Define the double logistic function
    def func(x, p0, p1, p2, p3, p4, p5):
        y = p0 - p1 * (
            (1.0 / (1 + exp(p2 * (x - p3)))) + (1.0 / (1 + exp(-p4 * (x - p5)))) - 1
        )
        return y

    # If no initial parameter values are specified, make an educated guess based on the data
    if guess is None:
        guess = {
            "p0": min(y),
            "p1": max(y),
            "p2": 0.035,
            "p3": percentile(x, 25),
            "p4": 0.035,
            "p5": percentile(x, 75),
        }

    guess = [*guess.values()]

    print("guess paramters:", list(around(array(guess), 4)))

    # Fit the curve to the data
    popt, pcov = curve_fit(
        func,
        xdata=x,
        ydata=y,
        p0=[guess],
    )

    # return an array of the fitted parameters
    popt
    print("refined paramters:", list(around(array(popt), 4)))

    # Refine the double sigmoid function based on the above curve fitting
    def fitted_dbl_sigmoid_func(x):
        p = popt
        y = p[0] - p[1] * (
            (1.0 / (1 + exp(p[2] * (x - p[3]))))
            + (1.0 / (1 + exp(-p[4] * (x - p[5]))))
            - 1
        )
        return y

    # return the refined function
    return fitted_dbl_sigmoid_func
