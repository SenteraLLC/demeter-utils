from datetime import timedelta
from typing import Dict

from numpy import average, exp, power
from pandas import DataFrame, Series, Timedelta

from demeter_utils.time import convert_dt_to_unix
from demeter_utils.time_series.utils import get_datetime_skeleton_time_series


def assign_group_weights(
    groups: Series,
    group_weights: Dict,
) -> Series:
    """Creates a Series containing weights that correspond to the passed group weights."""
    return groups.map(group_weights)


def _gaussian(x, mu, sig):
    """Gaussian probability density function with mean `mu` and standard deviation `sig`."""
    return exp(-power(x - mu, 2.0) / (2 * power(sig, 2.0)))


def _gaussian_kernel(
    t_unix: Series,
    t_mean: float,
    t_sigma: float,
) -> Series:
    """
    Calculate the moving window weights for the passed unix time series based on a Gaussian kernel with
    mean `t_mu` and standard deviation `t_simga`.

    Args:
        t_unix (Series): The input unix time series for which to apply the gaussian kernel.
        t_mean (int): The center of the gaussian distribution.
        t_sigma (float): The standard deviation of the gaussian distribution.

    Returns:
        Series: The moving window distance-based weights.
    """
    gaussian_wts = t_unix.apply(lambda t: _gaussian(t, mu=t_mean, sig=t_sigma))
    return Series(gaussian_wts)


def weighted_moving_average(
    t: Series,
    y: Series,
    step_size: timedelta,
    window_size: timedelta,
    weights: Series = None,
    include_bounds: bool = False,
    col_datetime: str = "date",
    col_value: str = "ndvi",
) -> Series:
    """
    Calculates a weighted moving average of the passed values for a given step size and window size.

    Only a Gaussian kernel is implemented here. 1/2 of the `window_size` is considered the standard deviation
    of the kernel.

    Args:
        t (Series): Input datetime values; must be dtype=datetime.
        y (Series): Input time series values observed at values of `t` (i.e., f(`t`) = `y`).

        step_size (timedelta): Step size used to create the weighted mean time series or, in other
            words, the temporal resolution of `t_hat`, where `t_hat[idx] = t.min() + (step_size * idx)`.

        window_size (timedelta): The standard deviation of the Gaussian kernel used to determine the weight
            of each point based on its distance from a given value of `t_hat`.

        weights (Series): Input weights for each value of `y`; defaults to array of 1s of len(`y`).

    Returns:
        Dataframe: Dataframe containing weighted moving average time series for input dataset with columns
            "t" and "y" for the temporal and value components, respectively.
    """
    if weights is None:
        weights = Series([1] * len(y))

    # get time points at which to estimate weighted mean
    bins_dt = get_datetime_skeleton_time_series(
        start=t.min(), end=t.max(), step_size=step_size, include_bounds=include_bounds
    )
    bins_unix = convert_dt_to_unix(bins_dt, relative_epoch=t.min())

    # convert everything to unix
    t_unix = convert_dt_to_unix(t, relative_epoch=t.min())
    window_size_unix = window_size // Timedelta("1s")

    # The following line performs these steps at each value of `t_hat` in `bins_unix`:
    # 1. Calculates moving window weights for `y` values based on distance between measured timepoint and `t_hat` given a Gaussian kernel.
    # 2. Multiplies each valueo of `weights` by the corresponding distance-based weight from (1) to calculate full contributing weight of each data point.
    # 3. Calculates the weighted average at `t_hat`.

    weighted_mean = bins_unix.apply(
        lambda mu: average(
            y.values,
            weights=_gaussian_kernel(
                t_unix, t_mean=mu, t_sigma=window_size_unix / 2
            ).values
            * weights,
        )
    )

    return DataFrame(data={col_datetime: bins_dt, col_value: weighted_mean})
