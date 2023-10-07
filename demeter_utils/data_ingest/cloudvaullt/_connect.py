from os import getenv
from typing import Tuple

from gql.client import Client
from gql.dsl import DSLSchema
from gql_utils.api import get_client, graphql_token


def get_cv_connection(env: str = "prod") -> Tuple[Client, DSLSchema]:
    """Create a session with CloudVault's API using credentials in .env file.

    Args:
        env (str): CloudVault enviroment to connect to; can be "prod" or "staging"
    """
    # load environment variables
    email = getenv("SENTERA_EMAIL")
    if not email:
        raise Exception(
            "You must provide your CloudVault email as environment variable 'SENTERA_EMAIL'"
        )
    password = getenv(f"SENTERA_{env.upper()}_PW")
    if not password:
        raise Exception(
            "You must provide your CloudVault password as environment variable 'SENTERA_<ENV>_PW'"
        )

    sentera_api_url = getenv(f"SENTERA_API_{env.upper()}_URL")
    if not sentera_api_url:
        raise Exception(
            "You must provide the API url as environment variable 'SENTERA_API_<ENV>_URL'"
        )

    token = graphql_token(
        email,
        password,
        url_auth=f"{sentera_api_url}/v1/sessions",
    )
    client, ds = get_client(token, sentera_api_url)

    return client, ds
