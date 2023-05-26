from datetime import datetime, timedelta

from numpy import ceil as np_ceil
from pandas import Series


def get_datetime_skeleton_time_series(
    start: datetime, end: datetime, step_size: timedelta, include_bounds: bool = False
):
    """Generates datetime time series skeleton (t_hat) based on starting point, end point, and step size.

    Args:
        start (datetime): First date of desired time series skeleton.
        end (datetime): Last date of desired time series skeleton.
        step_size (timedelta): Step size between each time point in the final time series.

        include_bounds (bool): Whether the function should enforce that the first and last value
            should be `start` and `end`, respectively.
    """
    timerange = end - start
    num_steps = np_ceil(timerange / step_size)
    bin_centers = [start + (step_size * idx) for idx in range(int(num_steps))]

    if include_bounds:
        bin_centers[0] = start
        bin_centers[-1] = end

    return Series(bin_centers)
