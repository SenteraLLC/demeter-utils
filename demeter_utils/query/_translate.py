"""Util functions to translate demeter objects into data-science friendly Python formats."""

from re import sub

from demeter.data import Field
from pandas import DataFrame, Series, concat


def field_to_dataframe(field: Field) -> DataFrame:
    """Translates demeter.data.Field object into a pandas.DataFrame row."""
    data = {}
    for n in field.names():
        data[n] = [getattr(field, n)]

    df = DataFrame(data)

    for var in ["date_start", "date_end", "created", "last_updated"]:
        if var in df.columns:
            df[var] = df[var].dt.to_pydatetime()

    return df


def camel_to_snake(string: str) -> str:
    name = sub("(.)([A-Z][a-z]+)", r"\1_\2", string)
    return sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def explode_details(df: DataFrame, col_details: str = "details") -> DataFrame:
    """Explodes the "details" column (`dict` type) as separate columns and concats them to end of `df`."""
    # TODO: What if `df` already has a column name that is in `df[col_details]`?
    return concat(
        [df.drop([col_details], axis=1), df[col_details].apply(Series)], axis=1
    )
