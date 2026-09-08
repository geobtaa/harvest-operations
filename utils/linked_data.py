"""Separate text labels from HTTP linked-data identifiers."""

import re


HTTP_IDENTIFIER = re.compile(r"https?://[^\s|<>;]+", re.IGNORECASE)


def split_http_identifiers(values: list[str]) -> tuple[list[str], list[str]]:
    """Keep text labels and extract standalone or label-adjacent HTTP URLs.

    DC exports may combine multiple names with semicolons and append several
    authority URLs to each name. Keep identifiers as separate repeatable values;
    no positional one-to-one association with the names is implied.
    """
    names, identifiers = [], []
    for value in values:
        for match in HTTP_IDENTIFIER.finditer(value):
            identifiers.append(match.group())
        name_text = HTTP_IDENTIFIER.sub("", value)
        for part in re.split(r"[;|]", name_text):
            name = re.sub(r"\s+", " ", part).strip()
            if name:
                names.append(name)
    return list(dict.fromkeys(names)), list(dict.fromkeys(identifiers))
