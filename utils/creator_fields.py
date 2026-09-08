"""Separate creator names from HTTP authority identifiers in Dublin Core."""

from utils.linked_data import split_http_identifiers


def split_creator_names_and_ids(values: list[str]) -> tuple[list[str], list[str]]:
    """Retain creator names and route authority URLs to a separate value list."""
    return split_http_identifiers(values)
