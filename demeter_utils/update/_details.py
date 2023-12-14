from json import dumps
from typing import Any

from demeter.db import Table, TableId
from psycopg2.extensions import AsIs
from psycopg2.sql import Identifier

from demeter_utils.query import camel_to_snake


def update_details(conn: Any, demeter_table: Table, table_id: TableId, details: dict):
    """Updates the details jsonb data for the given table_id in the given demeter_table."""
    table_name = camel_to_snake(demeter_table.__name__)
    table_name_id = table_name + "_id"
    # Update record with act
    stmt = """
    update {table}
    set details = '%(details)s'::jsonb
    WHERE %(table_name_id)s = %(table_id)s;
    """
    args = {
        "table": Identifier(table_name),
        "details": AsIs(dumps(details)),
        "table_name_id": AsIs(table_name_id),
        "table_id": AsIs(table_id),
    }

    with conn.begin():
        with conn.connection.cursor() as cursor:
            cursor.execute(stmt, vars=args)
