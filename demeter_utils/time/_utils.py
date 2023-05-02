from datetime import datetime, tzinfo

from pandas import NA, Timedelta, isna, to_datetime


def make_date_tzaware(d: datetime, tz: tzinfo) -> datetime:
    """Sets a time zone for an already created datetime object."""
    return d.replace(tzinfo=tz)


def get_timedelta_days(date_start: datetime, date_end: datetime):
    """Get the number of days (rounded) between two dates, where `date_start` - `date_end`.

    If either one of the dates is NA, return NA.
    """
    diff_days = date_start - date_end

    if isna(diff_days):
        return NA
    else:
        days = diff_days.round("d")
        return int(days.days)


def convert_dt_to_unix(
    dt: datetime, relative_epoch: datetime = datetime.utcfromtimestamp(0)
) -> int:
    """Converts a datetime to unix time, optionally adjusting relative to a defined epoch.

    Args:
        dt (datetime): Datetime to convert to unix time.

        relative_epoch (datetime, optional): Datetime value to use as Unix origin point
            (i.e., t = 0) for dt conversion; defaults to 1970-01-01 (or
            datetime.utcfromtimestamp(0)) which is the canonical Unix epoch.
    """
    return (to_datetime(dt) - to_datetime(relative_epoch)) // Timedelta("1s")


def convert_unix_to_dt(
    unix: int, relative_epoch: datetime = datetime.utcfromtimestamp(0)
) -> datetime:
    """Converts unix time to a datetime, optionally adjusting relative to a defined epoch.

    Args:
        unix (int): Unix time to convert to a datetime.

        relative_epoch (datetime, optional): Datetime value to use as Unix origin point
            (i.e., t = 0) for dt conversion; defaults to 1970-01-01 (or
            datetime.utcfromtimestamp(0)) which is the canonical Unix epoch.
    """
    tdelta = unix * Timedelta("1s")
    dt = to_datetime(relative_epoch) + tdelta
    return dt.to_pydatetime()
