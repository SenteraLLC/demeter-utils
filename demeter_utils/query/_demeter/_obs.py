"""Util functions for querying and translating Demeter data."""
from typing import Any

from demeter_utils.query._demeter._core import basic_demeter_query


def get_obs_type_and_unit_colname(
    cursor: Any, observation_type_id: int, unit_type_id: int
) -> str:
    """Formats a feature column name for a given observation type and unit type in demeter.

    Takes observation type ID and unit type ID and returns a column name
    for that observation, that includes both observation type name and unit name
    separated with underscores.

    Args:
        cursor: Connection to Demeter database

        observation_type_id (int): Observation type ID to look at in Demeter where `type_name` is
            used to create column name.

        unit_type_id (int): Unit type ID to look at in Demeter where name is appended to column
            name.
    """
    df_type = basic_demeter_query(
        cursor=cursor,
        table="observation_type",
        cols="type_name",
        conditions={"observation_type_id": observation_type_id},
    )

    type_name = str(df_type.iloc[0, 0])

    if " - " in type_name:
        type_name = type_name.replace(" - ", "_")
    # some names have hyphens and we should replace those with underscores, too

    joined_type_name = type_name.replace(" ", "_")

    df_unit = basic_demeter_query(
        cursor=cursor,
        table="unit_type",
        cols="unit_name",
        conditions={"unit_type_id": unit_type_id},
    )

    unit_name = str(df_unit.iloc[0, 0])
    if "/" in unit_name:
        unit_name = unit_name.replace("/", "_")
    # if there is a forward slash in units, replace with underscore

    if unit_name == "unitless":
        return joined_type_name
    else:
        return f"{joined_type_name}_{unit_name}"
