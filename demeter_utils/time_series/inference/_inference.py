from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import cached_property, partial
from typing import Callable

from pandas import DataFrame
from scipy.interpolate import UnivariateSpline
from scipy.optimize import minimize

from demeter_utils.time import convert_dt_to_unix
from demeter_utils.time_series.inference._double_logistic import (
    approximate_inflection_with_cubic_poly,
    double_logistic,
)
from demeter_utils.time_series.interpolate import weighted_moving_average


@dataclass
class TimeSeriesFitter:
    """
    Class to facilitate fitting of generic time-series data using different fitting methods.

    Initialization steps:
        1. Uses `params.wt_mapping` to weight each grouping of `params.col_mapping_group` column in `params.df`.
        2. Perform weighted moving average using Gaussian kernel with `params.step_size` and `params.window_size`.

    Fitting steps:
        1. Choose your desired fitting function (one of the methods of the `TimeSeriesFitter` class).
        2. Fit the desired fitting function to weighted moving average time series (created in __init__) to generate a
            Callable based on unix input.
        3. [Optional] Convert that Callable to take `datetime` input, converting to unix under the hood.

    Args:
        df (DataFrame): Dataframe containing `col_mapping_group`, `col_datetime`, and `col_value` columns.

        step_size (timedelta): Direct pass to `step_size` argument for `weighted_moving_average()`; corresponds to
            temporal resolution of weighted moving average time series.

        window_size (timedelta): Direct pass to `window_size` argument for `weighted_moving_average()`; corresponds to
            moving window sensitivity to distance when computing average (i.e., 2x the standard deviation of the
            Gaussian kernel).

        wt_mapping (dict[str, float]): Dictionary mapping values of the unique values in th `col_mapping_group` column
            to the relative weights defined by `wt_mapping` (float).

        col_datetime (str): Name of column in `df` containing datetime values.
        col_mapping_group (str): Name of column in `df` containing values to be mapped to weights in `wt_mapping`.
        col_value (str): Name of column in `df` containing values to be smoothed.

    """

    df: DataFrame
    step_size: timedelta
    window_size: timedelta
    wt_mapping: dict[str, float] = field(
        default_factory=lambda: {"drone": 10, "satellite": 1}
    )
    col_datetime: str = field(default="date")
    col_mapping_group: str = field(default="source")
    col_value: str = field(default="ndvi")

    @cached_property
    def df_daily_weighted_moving_avg(
        self,
    ) -> DataFrame:
        """
        Map the weights from `wt_mapping` and calculate the wighted moving average DataFrame.

        Returns:
            STAC_crop: Response from the titiler stac statistics endpoint.
        """
        self.df.sort_values(by=[self.col_datetime], inplace=True)
        wts = self.df[self.col_mapping_group].map(self.wt_mapping)
        return weighted_moving_average(
            t=self.df[self.col_datetime],
            y=self.df[self.col_value],
            step_size=self.step_size,
            window_size=self.window_size,
            weights=wts,
            include_bounds=True,
            col_datetime=self.col_datetime,
            col_value=self.col_value,
        )

    def cubic_spline(
        self, s: float = None, callable_unit_datetime: bool = True
    ) -> Callable:
        """
        Fits 3rd degree UnivariateSpline (cubic) function to the smoothed and weighted data values.

        See https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.UnivariateSpline.html

        Args:
            s (float, optional): The smooting factor. Defaults to 5% of the mean data value if not specified.

            callable_unit_datetime (bool, optional): If True, the returned function takes a `datetime` value as an arg;
                if false, it takes a relative epoch value (i.e., `int` dtype). Defaults to True.

        Returns:
            Callable: Takes a `datetime` value as an argument and returns the estimated data value based on the fitted
                smoothing spline.

        """
        # Set the smoothing factor (i.e., upper limit for sum of squared errors) for the UnivariateSpline function
        # Let's use 5% of mean value if not specified by user
        s = self.df[self.col_value].mean() * 0.05 if s is None else s

        # UnivariateSpline must take `int`` dtype (i.e., unix) for `x`
        xt = convert_dt_to_unix(
            self.df_daily_weighted_moving_avg[self.col_datetime],
            relative_epoch=self.df_daily_weighted_moving_avg[self.col_datetime].min(),
        )

        # Fit cubic spline to smoothed weighted mean curve data
        get_value_from_relative_epoch_fx = UnivariateSpline(
            x=xt, y=self.df_daily_weighted_moving_avg[self.col_value], k=3, s=s
        )

        # Return the callable
        if callable_unit_datetime:

            def get_value_from_datetime(dt: datetime) -> float:
                t = convert_dt_to_unix(
                    dt,
                    relative_epoch=self.df_daily_weighted_moving_avg[
                        self.col_datetime
                    ].min(),
                )
                return get_value_from_relative_epoch_fx(t)

            return get_value_from_datetime
        else:
            return get_value_from_relative_epoch_fx

    def double_logistic(self, callable_unit_datetime: bool = True) -> Callable:
        """
        Fits double logistic function to the smoothed and weighted data values.

        Args:
            callable_unit_datetime (bool, optional): If True, the returned function takes a `datetime` value as an arg;
                if false, it takes a relative epoch value (i.e., `int` dtype). Defaults to True.

        Returns:
            Callable: Takes a `datetime` value as an argument and returns the estimated data value based on the fitted
                smoothing spline.
        """
        # TODO: Allow user to choose whether to apply a weighted moving average
        # if self.apply_weighting is True:
        #     df_timeseries = self.df_daily_weighted_moving_avg.copy()
        # else:
        #     df_timeseries = self.df.copy()

        # Define the datetime to unix conversion to embed into get_value_from_datetime()
        def dt_transformation(dt: datetime) -> float:
            """Transform and standardize temporal dimension to improve convergence."""
            unix = convert_dt_to_unix(
                dt,
                relative_epoch=self.df_daily_weighted_moving_avg[
                    self.col_datetime
                ].min(),
            )  # convert to psuedo-unix
            return (unix - t_mean) / t_sd  # scale

        # TODO: Refactor this function outside of double_logistic(), and have users pass their own guess based on their data
        def _guess_starting_params():
            # Determine left and right side of curve
            max_threshold = 0.1
            max_bound = y.max() * (1 - max_threshold)
            ind_max = self.df_daily_weighted_moving_avg.loc[
                self.df_daily_weighted_moving_avg[self.col_value] >= max_bound
            ].index.values

            # Approximate inflection points
            df_left = self.df_daily_weighted_moving_avg.iloc[: min(ind_max) + 1, :]
            left_params = approximate_inflection_with_cubic_poly(
                t=df_left[self.col_datetime].apply(lambda dt: dt_transformation(dt)),
                y=df_left[self.col_value],
                ymin=y.min(),
                ymax=y.max(),
            )
            df_right = self.df_daily_weighted_moving_avg.iloc[max(ind_max) :, :]
            right_params = approximate_inflection_with_cubic_poly(
                t=df_right[self.col_datetime].apply(lambda dt: dt_transformation(dt)),
                y=df_right[self.col_value],
                ymin=y.min(),
                ymax=y.max(),
            )
            guess = {
                "ymin": y.min(),
                "ymax": y.max(),
                "t_incr": left_params["t"],
                "t_decr": right_params["t"],
                "rate_incr": left_params["rate"],
                "rate_decr": right_params["rate"],
            }
            return guess

        # Define cost function
        def _cost_function(p, t, y):
            partial_fx = partial(
                double_logistic,
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

        # Standardize to reduce scale (mean = 0, sd = 1)
        date_min = self.df[self.col_datetime].min()
        s_unix = self.df[self.col_datetime].map(
            lambda dt: convert_dt_to_unix(dt, relative_epoch=date_min)
        )
        t_mean = s_unix.mean()
        t_sd = s_unix.std()

        # Partial double logistic function must take scaled `float` dtype for `t`
        t = self.df_daily_weighted_moving_avg[self.col_datetime].apply(
            lambda dt: dt_transformation(dt)
        )
        y = self.df_daily_weighted_moving_avg[self.col_value].astype(float)

        guess_values = [*_guess_starting_params().values()]

        # Minimize cost function with initial values
        opt = minimize(_cost_function, guess_values, args=(t, y))
        popt = opt.x
        pars = {
            "ymin": popt[0],
            "ymax": popt[1],
            "t_incr": popt[2],
            "t_decr": popt[3],
            "rate_incr": popt[4],
            "rate_decr": popt[5],
        }

        # TODO: Figure out how to attach the `pars` dict to the returned function (e.g., as a class object)
        get_value_from_relative_epoch_fx = partial(
            double_logistic,
            ymin=pars["ymin"],
            ymax=pars["ymax"],
            t_incr=pars["t_incr"],
            t_decr=pars["t_decr"],
            rate_incr=pars["rate_incr"],
            rate_decr=pars["rate_decr"],
        )

        # Return the callable
        if callable_unit_datetime:
            # Create partial function that takes `datetime`, transforms it appropriately, and estimates value
            def get_value_from_datetime(dt: datetime) -> float:
                get_value_from_relative_epoch_fx = partial(
                    double_logistic,
                    ymin=pars["ymin"],
                    ymax=pars["ymax"],
                    t_incr=pars["t_incr"],
                    t_decr=pars["t_decr"],
                    rate_incr=pars["rate_incr"],
                    rate_decr=pars["rate_decr"],
                )
                return get_value_from_relative_epoch_fx(dt_transformation(dt))

            return get_value_from_datetime
        else:
            return get_value_from_relative_epoch_fx
