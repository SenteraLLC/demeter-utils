from typing import Any, Mapping, Union

from pandas import DataFrame, Series


# TODO: Could (should) this function be performed in place?
def add_feature(
    df: DataFrame,
    fx: Any,
    cols_to_args: Union[None, Mapping[str, str]] = None,
    constant_args: Union[None, Mapping[str, Any]] = None,
) -> Union[Any, None]:
    """
    Takes a row-level function with arguments and performs the function on each row of `df`.

    Function arguments are populated either by columns of `df` (mapped by `cols_to_args`) or
    specified constants (mapped by `constant_args`).

    Args:
        df (pandas.DataFrame): Dataframe to perform row-level action on
        fx: Row-level function to perform
        cols_to_args (dict): Dictionary mapping fx argument names to `df` columns
        constant_args (dict): Dictionary mapping fx argument names to desired constants
    """

    args: Mapping[str, Any] = {} if constant_args is None else constant_args

    if cols_to_args is None:
        cols_to_args = {}

    def generalized_fx(
        row: Series,  # type: ignore
        fx: Any,
        cols_to_args: Mapping[str, str],
    ) -> Any:
        if len(cols_to_args) > 0:
            for key in cols_to_args.keys():
                args[key] = row[cols_to_args[key]]

        return fx(**args)

    return df.apply(func=lambda row: generalized_fx(row, fx, cols_to_args), axis=1)
