"""CLI entrypoints through ``click`` bindings."""

import logging

import click

from <pkg_name> import __version__


@click.group()
@click.option(
    "--log_level",
    type=click.Choice(["DEBUG", "INFO", "WARNING"]),
    default="INFO",
    help="Set logging level for both console and file",
)
@click.version_option(__version__)
def cli(log_level):
    """CLI entrypoint."""
    logging.basicConfig(level=log_level)
