import logging

from demeter.data import Organization, insertOrGetOrganization
from pandas import DataFrame
from psycopg2.extras import NamedTupleCursor


def insert_organization(cursor: NamedTupleCursor, organization_name: str) -> int:
    """Insert Organization."""
    logging.info("  Inserting Organization: %s", organization_name)
    organization = Organization(name=organization_name)
    organization_id = insertOrGetOrganization(cursor, organization)
    return DataFrame(
        data=[
            {
                "organization_name": organization_name,
                "organization_id": organization_id,
            }
        ]
    )
