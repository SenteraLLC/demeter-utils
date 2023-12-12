from typing import Any

from demeter import db
from demeter.data import Field, FieldTrial, Grouper, Plot
from pandas import DataFrame
from pandas import concat as pd_concat
from psycopg2.extensions import AsIs
from psycopg2.sql import Identifier

from demeter_utils.query._translate import camel_to_snake
from demeter_utils.query.demeter._core import basic_demeter_query


def get_grouper_ancestors(
    cursor: Any,
    grouper_id: db.TableId,
) -> DataFrame:
    """Gets all Grouper ancestors for a given Grouper ID (sorted by distance from the given child)."""
    stmt = """
    with recursive ancestry as (
      select root.*,
             0 as distance
      from grouper root
      where root.grouper_id = %(grouper_id)s
      UNION ALL
      select ancestor.*,
             distance + 1
      from ancestry descendant, grouper ancestor
      where descendant.parent_grouper_id = ancestor.grouper_id
    )
    select * from ancestry
    order by distance
    """
    params = {"grouper_id": AsIs(grouper_id)}
    cursor.execute(stmt, params)
    # cursor.execute(stmt, {"grouper_id": grouper_id})
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(f"Failed to get grouper ancestors for: {grouper_id}")

    return _grouper_query_to_df(
        DataFrame(results), demeter_table=Grouper, pop_keys=["distance"]
    )


def get_grouper_descendants(
    cursor: Any,
    grouper_id: db.TableId,
) -> DataFrame:
    """Gets all Grouper descendants for a given Grouper ID (sorted by distance from the given parent)."""
    stmt = """
    with recursive descendants as (
      select root.*,
             0 as distance
      from grouper root
      where root.grouper_id = %(grouper_id)s
      UNION ALL
      select descendant.*,
             distance + 1
      from descendants ancestor, grouper descendant
      where ancestor.grouper_id = descendant.parent_grouper_id
    )
    select * from descendants
    order by distance
    """

    params = {"grouper_id": AsIs(grouper_id)}
    cursor.execute(stmt, params)
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(f"Failed to get grouper descendants for: {grouper_id}")

    return _grouper_query_to_df(
        DataFrame(results), demeter_table=Grouper, pop_keys=["distance"]
    )


def get_demeter_object_by_grouper(
    cursor: Any,
    demeter_table: db.Table,
    organization_id: db.TableId,
    grouper_id: db.TableId,
    include_descendants: bool = True,
) -> DataFrame:
    """Gets Fields, FieldTrials, or Plots that belong to a Grouper.

    Args:
        cursor: A psycopg2 cursor object.
        table: The table to query for Grouper ID. Must be one of [Field, FieldTrial, Plot].
        grouper_id: The ID of the Grouper to query.

        include_descendants: Whether to include descendants. If `include_descendants` is False, include all
            Fields/FieldTrials/Plots that belong to `grouper_id` DIRECTLY. If `include_descendants` is True, include all
            Fields/FieldTrials/Plots that belong to `grouper_id` either directly OR any of its children Groupers.

    Returns:
        DataFrame: A DataFrame of the Fields/FieldTrials/Plots objects that belong to `grouper_id`.
    """
    # if table not in ["field", "field_trial", "plot"]:
    table_name = camel_to_snake(demeter_table.__name__)
    if demeter_table not in [Field, FieldTrial, Plot]:
        raise ValueError(f'Groupers are not supported by table "{table_name}".')

    stmt_descendants_true = """
    with recursive descendants as (
      select root.*
      from grouper root
      where root.grouper_id = %(grouper_id)s
      UNION ALL
      select descendant.*
      from descendants ancestor, grouper descendant
      where ancestor.grouper_id = descendant.parent_grouper_id
    )
    select * from {table}
    where organization_id = %(organization_id)s and grouper_id in (select grouper_id from descendants)
    """

    stmt_descendants_false = """
    select * from {table}
    where organization_id = %(organization_id)s and grouper_id = %(grouper_id)s
    """

    stmt = (
        db.doPgFormat(stmt_descendants_true, table=Identifier(table_name))
        if include_descendants
        else db.doPgFormat(stmt_descendants_false, table=Identifier(table_name))
    )
    params = {"organization_id": AsIs(organization_id), "grouper_id": AsIs(grouper_id)}
    cursor.execute(stmt, params)
    results = cursor.fetchall()

    if len(results) < 1:
        raise Exception(
            f"Failed to get {table_name}s that belong to `grouper_id`: {grouper_id}"
        )
    return _grouper_query_to_df(DataFrame(results), demeter_table=demeter_table)


def get_grouper_id_by_name(cursor: Any, grouper_name: str) -> int:
    """Gets the Grouper ID for a given Grouper name."""
    df_grouper = basic_demeter_query(
        cursor, table="grouper", conditions={"name": grouper_name}
    )
    if len(df_grouper) > 1:
        raise ValueError(
            f'Multiple grouper names found for {grouper_name}: {df_grouper["name"].to_list()}'
        )
    if len(df_grouper) == 0:
        raise ValueError(f"No grouper names found for {grouper_name}")
    return int(df_grouper.iloc[0]["grouper_id"])


def _grouper_query_to_df(
    df_results: DataFrame, demeter_table: db.TableId, pop_keys: list[str] = []
) -> DataFrame:
    table_name = camel_to_snake(demeter_table.__name__)
    df_demeter_objects = DataFrame()
    for _, row in df_results.iterrows():
        pop_data = {k: row.pop(item=k) for k in pop_keys}
        demeter_object_ = demeter_table(**row.drop(labels=table_name + "_id").to_dict())
        data = dict(
            {
                "table": table_name,
                "table_id": [row[table_name + "_id"]],
                "demeter_object": [demeter_object_],
            },
            **pop_data,
        )
        df_demeter_objects = pd_concat(
            [df_demeter_objects, DataFrame(data)], ignore_index=True, axis=0
        )
    return df_demeter_objects


# def searchGrouper(
#     cursor: Any,
#     grouper_name: str,
#     parent_grouper_id: Optional[db.TableId] = None,
#     ancestor_grouper_id: Optional[db.TableId] = None,
#     do_fuzzy_search: bool = False,
# ) -> Optional[Tuple[db.TableId, Grouper]]:
#     search_part = "where name = %(name)s"
#     if do_fuzzy_search:
#         search_part = "where name like concat('%', %(name)s, '%')"

#     stmt = f"""
#     with candidate as (
#       select *
#       from grouper
#       {search_part}

#     ) select * from candidate;
#     """
#     args: Dict[str, Any] = {"name": grouper_name}
#     cursor.execute(stmt, args)
#     results = cursor.fetchall()

#     maybe_result: Optional[Tuple[db.TableId, Grouper]] = None
#     for r in results:
#         _id = r["grouper_id"]
#         f = Grouper(
#             grouper_id=r["grouper_id"],
#             parent_grouper_id=r["parent_grouper_id"],
#             name=r["name"],
#             details=r["details"],
#             last_updated=r["last_updated"],
#         )

#         if (p_id := parent_grouper_id) or (a_id := ancestor_grouper_id):
#             ancestors = get_grouper_ancestors(cursor, _id)
#             ancestor_ids = [a[0] for a in ancestors]
#             if p_id is not None:
#                 if p_id != ancestor_ids[0]:
#                     continue
#             if a_id is not None:
#                 if a_id not in ancestor_ids:
#                     continue

#         if maybe_result is not None:
#             raise Exception(
#                 f"Ambiguous field group search: {grouper_name},{p_id},{a_id}"
#             )

#         _id = r["grouper_id"]
#         maybe_result = (_id, f)

#     return maybe_result
