"""Util functions to translate demeter objects into data-science friendly Python formats."""

from re import sub

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


def camel_to_snake(string: str) -> str:
    name = sub("(.)([A-Z][a-z]+)", r"\1_\2", string)
    return sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()
