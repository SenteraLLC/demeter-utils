from datetime import datetime

from pandas import Timedelta, Timestamp, to_datetime


def get_unix_from_datetime(dt: datetime) -> int:
    """Convert dt `datetime` to UNIX timestamp following pandas recommended method.

    See https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#from-timestamps-to-epoch.
    """
    date = to_datetime(dt)
    start_epoch = Timestamp("1970-01-01")
    unix = (date - start_epoch) // Timedelta("1s")
    return unix
