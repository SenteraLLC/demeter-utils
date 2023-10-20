from datetime import date, timedelta
from functools import cached_property
from typing import Callable

from marshmallow import ValidationError, fields, validates
from marshmallow_dataclass import dataclass
from pandas import DataFrame
from scipy.interpolate import UnivariateSpline

from demeter_utils.time import convert_dt_to_unix
from demeter_utils.time_series.interpolate import weighted_moving_average


@dataclass
class TimeSeriesParams:
    """
    Parameters needed for the `TimeSeriesFitter` class.

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
    wt_mapping: dict[str, float] = fields.Dict(
        dump_default={"drone": 10, "satellite": 1}
    )
    col_datetime: str = fields.Str(dump_default="date")
    col_mapping_group: str = fields.Str(dump_default="source")
    col_value: str = fields.Str(dump_default="ndvi")

    @validates(field_name="df")
    def validate_dataframe_columns(
        self, df, col_datetime, col_mapping_group, col_value
    ):
        for col in [col_datetime, col_mapping_group, col_value]:
            try:
                _ = df[col]
            except KeyError as e:
                raise ValidationError(e)

    @validates(field_name="step_size")
    def validate_step_size(self, step_size):
        if not isinstance(step_size, timedelta):
            raise ValidationError("`step_size` must be `timedelta` type.")

    @validates(field_name="window_size")
    def validate_window_size(self, window_size):
        if not isinstance(window_size, timedelta):
            raise ValidationError("`window_size` must be `timedelta` type.")

    # TODO: Validate wt_mapping for keys in col_mapping_group
    # TODO: Validate that dtype(df[col_datetime]) == datetime


class TimeSeriesFitter:
    """
    Class to faciilitate fitting of generic time-series data using different fitting methods.

    Initialization steps:
        1. Uses `params.wt_mapping` to weight each grouping of `params.col_mapping_group` column in `params.df`.
        2. Perform weighted moving average using Gaussian kernel with `params.step_size` and `params.window_size`.

    Fitting steps:
        1. Choose your desired fitting function (one of the methods of the `TimeSeriesFitter` class).
        2. Fit the desired fitting function to weighted moving average time series (created in __init__) to generate a
            Callable based on unix input.
        3. [Optional] Convert that Callable to take `datetime` input, converting to unix under the hood.

    Args:
        fitter_params (TimeSeriesParams): Validated TimeSeriesParams needed to fit time-series data.

    """

    def __init__(
        self,
        params: TimeSeriesParams,
    ):
        serialized_params = TimeSeriesParams.Schema().dump(
            params
        )  # Use Schema(only=["df", "step_size", "window_size"]) to filter

        errors = TimeSeriesParams.Schema().validate(serialized_params)
        if errors:
            raise ValidationError(errors)

        self.df = serialized_params["df"]
        self.step_size = serialized_params["step_size"]
        self.window_size = serialized_params["window_size"]
        self.wt_mapping = serialized_params["wt_mapping"]
        self.col_datetime = serialized_params["col_datetime"]
        self.col_mapping_group = serialized_params["col_mapping_group"]
        self.col_value = serialized_params["col_value"]

        self.df_daily_weighted_moving_avg

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
        )

    def cubic_spline(
        self, s: float = None, callable_unit_datetime: bool = True
    ) -> Callable:
        """
        Fits 3rd degree UnivariateSpline (cubic) function to the smoothed and weighted data values.

        See https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.UnivariateSpline.html

        Args:
            s (float, optional): The smooting factor. Defaults to 5% of the mean data value if not specified.

        Returns:
            Callable: Takes a `datetime` value as an argument and returns the estimated data value based on the fitted
                smoothing spline.

        """
        # Set the smoothing factor (i.e., upper limit for sum of squared errors) for the UnivariateSpline function
        # Let's use 5% of mean value if not specified by user
        s = self.df[self.col_value].mean() * 0.05 if s is None else s

        # UnivariateSpline must take `int`` dtype (i.e., unix) for `x`
        xt = convert_dt_to_unix(
            self.df_daily_weighted_moving_avg["t"],
            relative_epoch=self.df_daily_weighted_moving_avg["t"].min(),
        )

        # Fit cubic spline to smoothed weighted mean curve data
        get_value_from_relative_epoch_fx = UnivariateSpline(
            x=xt, y=self.df_daily_weighted_moving_avg["y"], k=3, s=s
        )

        # Return the callable
        if callable_unit_datetime:

            def get_value_from_datetime(dt: date) -> float:
                t = convert_dt_to_unix(
                    dt, relative_epoch=self.df_daily_weighted_moving_avg["t"].min()
                )
                return get_value_from_relative_epoch_fx(t)

            return get_value_from_datetime(get_value_from_relative_epoch_fx)
        else:
            return get_value_from_relative_epoch_fx

    def double_logistic(
        self,
    ) -> Callable:
        """
        _summary_

        Returns:
            Callable: Takes a `datetime` value as an argument and returns the estimated data value based on the fitted
                smoothing spline.
        """
        # TODO: Implement double logistic fitting method
        pass
