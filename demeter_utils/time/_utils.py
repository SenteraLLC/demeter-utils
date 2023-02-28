from datetime import datetime, tzinfo

from pandas import NA, isna


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
