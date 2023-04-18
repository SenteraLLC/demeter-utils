from datetime import datetime, timedelta
from typing import Dict

from numpy import arange, ceil, datetime64
from numpy import nan as np_nan
from pandas import DataFrame, NaT, Timedelta
from pandas import concat as pd_concat
from pandas import merge, merge_asof


def _get_row_template(col_value: str) -> Dict:
    return {
        "within_tolerance": [False],
        "datetime_skeleton": [NaT],
        "datetime_proposed": [NaT],
        col_value: [np_nan],
    }


def _recalibrate_date_split(
    datetime_pre: datetime,
    datetime_post: datetime,
    n_dates_split: int,
    idx: int,
) -> datetime:
    """
    Gets `idx` of `n_dates_split` evenly-spaced split points between `datetime_pre` and `datetime_post`.

    Args:
         datetime_pre (datetime): Lower bound of the date range to be split.
         datetime_post (datetime): Upper bound of the date range to be split.
         n_dates_split (int): Number of evenly-spaced split points within date range.
         idx (int): Indicates which split index (starting from 1) should be returned.
    """
    datetime_delta = (datetime_post - datetime_pre) / (n_dates_split + 1)

    list_recalibrated_splits = [
        datetime_pre + (datetime_delta * x) for x in range(1, n_dates_split + 1)
    ]

    return list_recalibrated_splits[idx - 1]


def _recalibrate_datetime_skeleton(df: DataFrame) -> DataFrame:
    """Recalibrate `datetime_skeleton` so missing time points are evenly-spaced between observed time points.
    Args:
    df (DataFrame): Input dataframe to recalibrate based on "datetime_skeleton" and "within_tolerance"
    """
    df_out = df.copy()

    df_out.sort_values(by="datetime_skeleton", inplace=True)

    idx_within_tolerance = df_out["within_tolerance"]

    # create columns to indicate last and next available "observed" dates for each row
    df_out["datetime_pre"] = np_nan
    df_out["datetime_post"] = np_nan

    # force ends (if unavailable) to remain as `datetime_start` and `datetime_end`
    if df_out.at[0, "within_tolerance"] is False:
        df_out.loc[0, ["datetime_pre", "datetime_post"]] = df_out.at[
            0, "datetime_skeleton"
        ]

    idx_last = len(df_out) - 1
    if df_out.at[idx_last, "within_tolerance"] is False:
        df_out.loc[idx_last, ["datetime_pre", "datetime_post"]] = df_out.at[
            idx_last, "datetime_skeleton"
        ]

    # forward fill all NA values in `datetime_pre` with most recent observed date
    df_out.loc[idx_within_tolerance, "datetime_pre"] = df_out.loc[
        idx_within_tolerance, "datetime_skeleton"
    ]
    df_out["datetime_pre"].ffill(inplace=True)

    # do same process for `datetime_post`
    df_out.sort_values(by="datetime_skeleton", ascending=False, inplace=True)

    df_out.loc[idx_within_tolerance, "datetime_post"] = df_out.loc[
        idx_within_tolerance, "datetime_skeleton"
    ]
    df_out["datetime_post"].ffill(inplace=True)

    # sort once more
    df_out.sort_values(by="datetime_skeleton", inplace=True)

    # add two columns to indicate num splits (`n_dates_split`) and split index (`idx`)
    df_out["idx"] = df_out.groupby(by=["datetime_pre", "datetime_post"]).cumcount() + 1
    df_out.loc[
        df_out["datetime_post"] == df_out["datetime_pre"], "idx"
    ] = 0  # change idx to 0 for matched rows

    df_out["n_dates_split"] = df_out.groupby(by=["datetime_pre", "datetime_post"])[
        "idx"
    ].transform("max")

    df_out["datetime_skeleton"] = df_out.apply(
        lambda row: row["datetime_skeleton"]
        if row["idx"] == 0
        else _recalibrate_date_split(
            row["datetime_pre"],
            row["datetime_post"],
            row["n_dates_split"],
            row["idx"],
        ),
        axis=1,
    )

    df_out.drop(
        columns=["datetime_pre", "datetime_post", "n_dates_split", "idx"], inplace=True
    )

    return df_out


def _maybe_fix_duplicate_matches(
    df_merged: DataFrame, col_datetime: str, col_value: str
):
    """If an observed value matched more than once to a "proposed" datetime, undo the later match."""
    matched_rows = df_merged[col_datetime].notna()
    duplicated = df_merged.duplicated([col_datetime, col_value], keep="first")
    if any(duplicated & matched_rows):
        df_merged.loc[duplicated & matched_rows, col_datetime] = NaT
        df_merged.loc[duplicated & matched_rows, col_value] = np_nan

    return df_merged


def get_datetime_skeleton_for_ts(
    df_true_data: DataFrame,
    datetime_start: datetime,
    datetime_end: datetime,
    temporal_resolution: timedelta,
    tolerance_alpha: float = 0.5,
    col_datetime: str = "date_observed",
    col_value: str = "value_observed",
    recalibrate: bool = True,
) -> DataFrame:
    """Given a time series dataframe, determines needed "fill-in" time points to achieve specific temporal resolution.

    Args:
        df_true_data (`DataFrame`): Input ("true") data that is available for a time series.
        datetime_start (`datetime.datetime`): Starting datetime for needed time series.
        datetime_end (`datetime.datetime`): Ending datetime for needed time series.
        temporal_resolution (`datetime.timedelta`): The minimum temporal resolution desired for the output time series.

        tolerance_alpha (`float`): Proportion of `temporal_resolution` to use as `tolerance` when
            performing fuzzy merge on proposed and observed datetime values.

        col_datetime (`str`): Column name for column in `df_true_data` that holds time series temporal data.
        col_value (`str`): Column name for column in `df_true_data` that holds time series value data.

        recalibrate (`bool`): Whether `date_skeleton` dates should be adjusted so they are evenly spaced between the
            previous and subsequent observed dates. Defaults to True.

    Returns:
        `DataFrame` containing desired time series data with date information held in `datetime_skeleton`
        where `within_tolerance` column indicates whether an observed value was available (True) for a given
        time point or still needs to be predicted.
    """
    df_in = df_true_data.copy()

    # determine "length_out" based on temporal resolution
    length_out = int(ceil((datetime_end - datetime_start) / temporal_resolution))

    # create an empty dataframe `df_join` and outline the time windows that need to be represented
    df_join = DataFrame(data=[], columns=["within_tolerance"])
    list_rq_datetime = [
        datetime_start + (temporal_resolution * x) for x in range(length_out + 1)
    ]
    # ensure last value of rq_datetime is datetime_end
    list_rq_datetime[-1] = datetime_end

    df_join["datetime_proposed"] = list_rq_datetime
    df_join["within_tolerance"] = False

    # do fuzzy match on `col_datetime` based on temporal resolution
    df_merged = merge_asof(
        df_join,
        df_in[[col_datetime, col_value]],
        left_on="datetime_proposed",
        right_on=col_datetime,
        tolerance=Timedelta(temporal_resolution * tolerance_alpha),
        direction="nearest",
    )

    # ensure no values matched twice (happens if date falls along halfway point)
    df_merged = _maybe_fix_duplicate_matches(
        df_merged, col_datetime=col_datetime, col_value=col_value
    )

    # add the rows from `df_in` that were not included in `df_merged`...
    idx_missing = ~df_in[col_datetime].isin(df_merged[col_datetime])
    df_missing = df_in.loc[idx_missing][[col_datetime, col_value]]
    df_missing.insert(0, "datetime_proposed", NaT)

    # ... unless they are outside of the desired date range
    tolerance = tolerance_alpha * temporal_resolution
    df_missing = df_missing.loc[df_missing[col_datetime] >= datetime_start - tolerance]
    df_missing = df_missing.loc[df_missing[col_datetime] <= datetime_end + tolerance]
    df = pd_concat([df_merged, df_missing], axis=0, ignore_index=True)

    # indicate where data is available
    df.loc[df[col_value].notna(), "within_tolerance"] = True

    # create column `datetime_skeleton` whose values are the same as `col_datetime`
    # where `within_tolerance`=True and otherwise, are the same as `datetime_proposed`
    df["datetime_skeleton"] = df.apply(
        lambda row: row[col_datetime]
        if row["within_tolerance"] is True
        else row["datetime_proposed"],
        axis=1,
    )

    row_template = _get_row_template(col_value)
    cols_keep = list(row_template.keys())
    df = df.sort_values(by="datetime_skeleton").reset_index(drop=False)[cols_keep]

    if recalibrate:
        df = _recalibrate_datetime_skeleton(df)

    # check that the full time range is covered
    if df["datetime_skeleton"].min() > datetime_start:
        first_row = row_template.copy()
        first_row["datetime_skeleton"] = [datetime_start]
        df = pd_concat([DataFrame(first_row), df], axis=0)

    if df["datetime_skeleton"].max() < datetime_end:
        last_row = row_template.copy()
        last_row["datetime_skeleton"] = [datetime_end]
        df = pd_concat([df, DataFrame(last_row)], axis=0)

    return df


# %%
def generate_fill_in_values(
    df_reference_ndvi: DataFrame,
    datetime_start: datetime,
    datetime_end: datetime,
    temporal_resolution: timedelta,
    interp_function: str,
) -> DataFrame:
    """
    # Generate a dataframe with interpolated values given a standard/reference data `df_reference_ndvi' and `interp_function`
     The input dataset should have at least `date_start` and `sample_value` columns.

    Args:
        df_reference_ndvi (`DataFrame`): Input ("reference") data.
        datetime_start (`datetime.datetime`): Starting datetime for interpolation of reference data.
        datetime_end (`datetime.datetime`): Ending datetime for interpolation of reference data.
        temporal_resolution (`datetime.timedelta`): The minimum temporal resolution desired for the output interpolation of reference data.
        interp_function (`str`): Model type for interpolation, "CubicSpline" for cubic spline, "Akima1DInterpolator" for akima1DInterpolator, "PchipInterpolator" for pchip_interpolator

    Returns `DataFrame` wtih following columns:
        "datetime_interp": Datetime for the value observed/interpolated
        "doy_interp": Day of the year for the corresponding `datetime_interp`
        "model_type": Interpolation model/function used
        "ndvi_interp": Value of NDVI observed/interpolated

    """

    # copy the `df_reference_ndvi` as `df_forinterp`
    df_forinterp = df_reference_ndvi.copy()

    # Remove NA values from the 'sample_value' column in the data 'df_forinterp'
    df_forinterp = df_forinterp[df_forinterp["sample_value"].notna()]

    # Convert the 'date_start' column to a datetime.datetime() object
    df_forinterp["date_start"] = (df_forinterp["date_start"]).astype(datetime64)

    # Extract the day of year from the 'date_start' column and store it in a new column 'doy_obs'
    df_forinterp["doy_obs"] = df_forinterp["date_start"].apply(
        lambda x: x.timetuple().tm_yday
    )

    # create an arrary of `datetime_interp` values for interpolation
    datetime_interp = arange(datetime_start, datetime_end, temporal_resolution)

    # create a temp df `df_forinterp_temp` to store `datetime_interp` and `doy_interp`
    df_forinterp_temp = DataFrame(datetime_interp, columns=["datetime_interp"]).astype(
        datetime64
    )

    # convert the `datetime_interp` to `doy_interp` and add the values to a new column
    df_forinterp_temp["doy_interp"] = df_forinterp_temp["datetime_interp"].apply(
        lambda x: x.timetuple().tm_yday
    )

    doy_interp = df_forinterp_temp["doy_interp"]

    # assign `x_interp`, `x_obs` and `y_obs` values
    x_interp = doy_interp  # Or, datetime_interp: The code does not work for datetime_interp as of now
    x_obs = df_forinterp["doy_obs"]
    y_obs = df_forinterp["sample_value"]

    # create a new data frame 'df_reference_interp' to store the interpolated values.
    df_reference_interp = DataFrame(columns=["model_type", "doy_interp", "ndvi_interp"])

    # generate interpolated ndvi values using the interp_function specified in function
    ndvi_interp = interp_function(x=x_obs, y=y_obs)(x_interp)

    data = {
        "model_type": str(interp_function),
        "doy_interp": x_interp,
        "ndvi_interp": ndvi_interp,
    }
    df_temp = DataFrame(data=data)

    # create a df `df_reference_interp` with concat of `df_reference_interp` and `df_temp`
    df_reference_interp = pd_concat([df_reference_interp, df_temp], axis=0)

    # create a df `df_reference_interp_merged` by merging `df_reference_interp` and `df_forinterp_temp` based on 'doy_interp' column
    df_reference_interp_merged = merge(
        df_forinterp_temp, df_reference_interp, on="doy_interp"
    )

    # remove NA values from the dataframe
    df_reference_interp_merged = df_reference_interp_merged[
        df_reference_interp_merged["ndvi_interp"].notna()
    ]
    return df_reference_interp_merged


# df_interp = generate_fill_in_values(
#     df_reference_ndvi = df_gimms_ndvi,
#     datetime_start = datetime(2019, 5, 6),
#     datetime_end = datetime(2019, 10, 1),
#     temporal_resolution = timedelta(days=1),
#     interp_function = CubicSpline,
#     )

# %% Example:
#     True data:
#               START DATE         SAMPLE VALUE
#               2022-05-01             0.4
#               2022-05-15             0.5
#               2022-05-25             0.6

#     Desired output for:
#         df_interp = generate_fill_in_values(
#     df_reference_ndvi = df_gimms_ndvi,
#     datetime_start = datetime(2019, 5, 6),
#     datetime_end = datetime(2019, 10, 1),
#     temporal_resolution = timedelta(days=1),
#     interp_function = CubicSpline,
#     )
#
#               model_type         doy_interp        ndvi_interp
#           Akima1DInterpolator         1               0.265
#           Akima1DInterpolator         2               0.275


# %% TODO: Delete if `doy_interp` is used instead of `datetime_interp`
# convert `doy_interp` to float
# def dt64_to_float(dt64):
#     Converts numpy.datetime64 to year as float.

#     Rounded to days

#     Parameters
#     ----------
#     dt64 : np.datetime64 or np.ndarray(dtype='datetime64[X]')
#         date data

#     Returns
#     -------
#     float or np.ndarray(dtype=float)
#         Year in floating point representation
#     """

#     year = dt64.astype('M8[Y]')
#     days = (dt64 - year).astype('timedelta64[D]')
#     year_next = year + np.timedelta64(1, 'Y')
#     days_of_year = (year_next.astype('M8[D]') - year.astype('M8[D]')
#                     ).astype('timedelta64[D]')
#     dt_float = 1970 + year.astype(float) + days / (days_of_year)
#     return dt_float

# doy_interp_float = dt64_to_float(doy_interp)


# TODO: Delete this if not required
# datetime_obs_array = df_forinterp["date_start"].to_numpy()
# datetime_obs_float = dt64_to_float(datetime_obs_array)
# %%
