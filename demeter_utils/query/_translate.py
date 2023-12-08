"""Util functions to translate demeter objects into data-science friendly Python formats."""

from re import sub

from demeter.data import Field
from pandas import DataFrame, concat


def camel_to_snake(string: str) -> str:
    name = sub("(.)([A-Z][a-z]+)", r"\1_\2", string)
    return sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def explode_details(df: DataFrame, col_details: str = "details") -> DataFrame:
    """Explodes the "details" column (`dict` type) as separate columns and concats them to end of `df`."""
    # TODO: What if `df` already has a column name that is in `df[col_details]`?
    df.reset_index(drop=True, inplace=True)
    df_details = DataFrame(df[col_details].values.tolist())

    # Get index/column name for reordering later
    col_to_insert_after = df.columns[df.columns.tolist().index(col_details) - 1]
    cols_to_reorder = df_details.columns

    # Concat details into a single dataframe
    df_out = concat(
        [df.drop(columns=[col_details]), df_details],
        axis=1,
    )

    # Reorder so exploded columns are located at the position of the col_details column
    return reorder_dataframe_columns(
        df_out, col_to_insert_after=col_to_insert_after, cols_to_reorder=cols_to_reorder
    )


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


def reorder_dataframe_columns(
    df: DataFrame, col_to_insert_after: str, cols_to_reorder: list[str]
) -> DataFrame:
    for col in cols_to_reorder:
        if col not in df.columns:
            raise ValueError(f"Column {col} is not present in DataFrame.")
    crop_type_idx = df.columns.tolist().index(col_to_insert_after)
    for col in reversed(list(cols_to_reorder)):
        df.insert(crop_type_idx + 1, col, df.pop(col))
    return df
