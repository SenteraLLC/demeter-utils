from datetime import timedelta
from typing import Dict

from numpy import average, ceil, exp, max, min, power
from pandas import Series, Timedelta

from demeter_utils.temporal_inference import convert_dt_to_unix, convert_unix_to_dt


def assign_group_weights(
    groups: Series,
    group_weights: Dict,
) -> Series:
    """Creates a Series containing weights that corresponde to the passed group weights."""
    return groups.map(group_weights)


def moving_average_weights(
    bin_dt: Series, weights: Series, step_size: timedelta, window_size: timedelta
) -> Series:
    """
    Adjusts weights for datetime bins based on a moving average.

    Args:
        bin_dt (Series): The datetime bins to adjust weights for.
        weights (Series): The weights to adjust.
        step_size (timedelta): The step size to use for the moving average.
        window_size (timedelta): The window size to use for the moving average.

    Returns:
        Series: The adjusted weights.
    """
    bin_unix = convert_dt_to_unix(bin_dt, relative_epoch=bin_dt.min())
    bin_dt_reassigned = reassign_datetime_bins(bin_dt, step_size=step_size)
    bin_unix_reassigned = convert_dt_to_unix(
        bin_dt_reassigned, relative_epoch=bin_dt_reassigned.min()
    )
    window_size_sec = window_size // Timedelta("1s")

    sum_weights = Series([0.0] * len(bin_unix))
    for _, mu in enumerate(bin_unix_reassigned):
        combined_weights = _moving_window_weighted_gaussian(
            bin_unix, weights, window_size_sec, mu
        )
        sum_weights += combined_weights.apply(lambda x: x / sum(combined_weights))
    return bin_dt.to_frame(name="datetime").join(
        sum_weights.to_frame(name="weights_moving_avg")
    )


def reassign_datetime_bins(dt_bins: Series, step_size: timedelta):
    """
    Re-assigns the passed datetime bins into bins of the passed step size.

    Args:
        dt_bins (Series): Input datetime values; must be dtype=datetime.
        step_size (timedelta): The size of the bins to re-assign the passed datetime values into.
    """
    step_size_sec = step_size // Timedelta("1s")
    range_sec = (max(dt_bins) - min(dt_bins)) // Timedelta("1s")
    num_steps = ceil(range_sec / step_size_sec)
    bin_centers = [
        convert_unix_to_dt((step_size_sec * idx), relative_epoch=min(dt_bins))
        for idx in range(int(num_steps))
    ]
    return Series(bin_centers)


def weighted_moving_average(
    bin_dt: Series,
    values: Series,
    weights: Series,
    step_size: timedelta,
    window_size: timedelta,
) -> Series:
    """
    Calculates the weighted moving average of the passed values for a given step size and window size.

    Args:
        bin_dt (Series): Input datetime values; must be dtype=datetime.
        values (Series): Input values to calculate the weighted moving average for.
        weights (Series): Input weights.
        step_size (timedelta): Step size (used to determine the new/re-assigned bins).
        window_size (timedelta): Window size (passed into a gaussian function).

    Returns:
        Series: The weighted moving average.
    """
    bin_unix = convert_dt_to_unix(bin_dt, relative_epoch=bin_dt.min())
    bin_dt_reassigned = reassign_datetime_bins(bin_dt, step_size=step_size)
    bin_unix_reassigned = convert_dt_to_unix(
        bin_dt_reassigned, relative_epoch=bin_dt_reassigned.min()
    )
    window_size_sec = window_size // Timedelta("1s")

    # The following line performs these steps:
    # 1. Calculates moving window weightings for the input bins based on their distance from each reassigned bin.
    # 2. Takes the passed weights and multiplies them by the gaussian weights to adjust the weight even further.
    # 3. Calculates the weighted average for each reassigned bin.
    weighted_mean = bin_unix_reassigned.apply(
        lambda mu: average(
            values.to_numpy(),
            weights=_moving_window_weighted_gaussian(
                bin_unix, weights, window_size_sec, mu
            ).to_numpy(),
        )
    )
    # weighted_mean = Series([0.0] * len(bin_unix))
    # for i, mu in enumerate(bin_unix_reassigned):
    #     combined_weights = _moving_window_weighted_gaussian(bin_unix, weights, window_size_sec, mu)
    #     weighted_mean[i] = average(values.to_numpy(), weights=combined_weights)
    return bin_dt_reassigned.to_frame(name="datetime").join(
        weighted_mean.to_frame(name="weighted_mean")
    )


def _gaussian(x, mu, sig):
    return exp(-power(x - mu, 2.0) / (2 * power(sig, 2.0)))


def _moving_window_weighted_gaussian(
    bin_unix: Series, weights: Series, window_size_sec: int, mu: int
) -> Series:
    """
    Calculate the moving window weights for the passed unix time series for the given weights, window size, and mu.

    Args:
        bin_unix (Series): The unix time series to calculate the moving window weights for.
        mu (int): The center of the gaussian distribution.
        window_size_sec (int): The size of the window (in seconds).
        weights (Series): The weights to apply to the gaussian distribution.

    Returns:
        Series: The moving window weights.
    """
    gaussian_wts = bin_unix.apply(
        lambda x: _gaussian(x, mu=mu, sig=window_size_sec / 2)
    )
    return Series(gaussian_wts * weights)
