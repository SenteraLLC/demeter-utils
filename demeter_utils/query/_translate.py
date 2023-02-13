"""Util functions to translate demeter objects into data-science friendly Python formats."""

from demeter.data import Field
from pandas import DataFrame


def field_to_dataframe(field: Field) -> DataFrame:
    """Translates demeter.data.Field object into a pandas.DataFrame row."""
    data = {}
    for n in field.names():
        data[n] = [getattr(field, n)]

    df = DataFrame(data)

    for var in ["date_start", "date_end", "created", "last_updated"]:
        df[var] = df[var].dt.to_pydatetime()

    return df
