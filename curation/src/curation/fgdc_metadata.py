"""Parse the FGDC CSDGM fields used by local curation pipelines.

The parser deliberately retains publication, content, and metadata dates as
separate values.  They describe different events and must not be collapsed
before a curator reviews the record.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable


def local_name(tag: str) -> str:
    """Return a lower-case XML local name, ignoring namespaces."""
    return tag.rsplit("}", 1)[-1].casefold()


def clean_text(value: str | None) -> str:
    """Collapse whitespace in an XML text value."""
    return " ".join((value or "").split())


class _PlainTextParser(HTMLParser):
    """Collect readable text while discarding HTML markup."""

    BLOCK_TAGS = {
        "br",
        "div",
        "li",
        "ol",
        "p",
        "table",
        "td",
        "th",
        "tr",
        "ul",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() in self.BLOCK_TAGS:
            self.parts.append(" ")

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() in self.BLOCK_TAGS:
            self.parts.append(" ")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def strip_html(value: str | None) -> str:
    """Convert literal or XML-escaped HTML fragments to normalized plain text."""
    parser = _PlainTextParser()
    parser.feed(unescape(value or ""))
    parser.close()
    return clean_text("".join(parser.parts))


def path_elements(root: ET.Element, path: Iterable[str]) -> list[ET.Element]:
    """Find direct-descendant XML elements using namespace-agnostic names."""
    nodes = [root]
    for expected_name in path:
        expected = expected_name.casefold()
        nodes = [
            child
            for node in nodes
            for child in node
            if local_name(child.tag) == expected
        ]
    return nodes


def path_values(root: ET.Element, path: Iterable[str]) -> list[str]:
    """Return nonblank text values at one namespace-agnostic XML path."""
    values = []
    for element in path_elements(root, path):
        value = clean_text(" ".join(element.itertext()))
        if value:
            values.append(value)
    return values


def first_path_value(
    root: ET.Element,
    paths: Iterable[Iterable[str]],
) -> str:
    """Return the first nonblank value found among candidate paths."""
    for path in paths:
        values = path_values(root, path)
        if values:
            return values[0]
    return ""


def unique_path_values(root: ET.Element, path: Iterable[str]) -> tuple[str, ...]:
    """Return unique values at a path while preserving document order."""
    return tuple(dict.fromkeys(path_values(root, path)))


def normalize_fgdc_date(value: str) -> str:
    """Convert common compact FGDC dates to ISO form.

    Values such as ``Unknown``, ``Present``, and malformed dates are retained
    so that the manual-review report does not discard source evidence.
    """
    value = clean_text(value)
    formats = {
        r"\d{8}": ("%Y%m%d", "%Y-%m-%d"),
        r"\d{6}": ("%Y%m", "%Y-%m"),
        r"\d{4}": ("%Y", "%Y"),
    }
    for pattern, (input_format, output_format) in formats.items():
        if not re.fullmatch(pattern, value):
            continue
        try:
            return datetime.strptime(value, input_format).strftime(output_format)
        except ValueError:
            return value
    return value


def _first_year(value: str) -> str:
    match = re.search(r"\b(?:18|19|20)\d{2}\b", value)
    return match.group(0) if match else ""


@dataclass(frozen=True)
class FgdcAttribute:
    """One FGDC entity attribute definition."""

    label: str
    definition: str
    definition_source: str
    domain: str


@dataclass(frozen=True)
class FgdcMetadata:
    """Selected, losslessly separated FGDC metadata values."""

    source_path: Path
    title: str = ""
    alternative_title: str = ""
    abstract: str = ""
    purpose: str = ""
    originators: tuple[str, ...] = ()
    publisher: str = ""
    publication_date: str = ""
    calendar_dates: tuple[str, ...] = ()
    begin_date: str = ""
    end_date: str = ""
    currentness_reference: str = ""
    metadata_date: str = ""
    theme_keywords: tuple[str, ...] = ()
    place_keywords: tuple[str, ...] = ()
    access_constraints: str = ""
    use_constraints: str = ""
    west: str = ""
    east: str = ""
    south: str = ""
    north: str = ""
    spatial_reference: str = ""
    entity_attribute_detail: str = ""
    attributes: tuple[FgdcAttribute, ...] = ()

    @property
    def creator(self) -> str:
        return "|".join(self.originators)

    @property
    def description(self) -> str:
        pieces = [self.abstract]
        if self.purpose and self.purpose != self.abstract:
            pieces.append(f"Purpose: {self.purpose}")
        return "|".join(piece for piece in pieces if piece)

    @property
    def temporal_coverage(self) -> str:
        if self.begin_date and self.end_date:
            if self.begin_date == self.end_date:
                return self.begin_date
            return f"{self.begin_date} to {self.end_date}"
        if self.begin_date:
            return self.begin_date
        if self.end_date:
            return self.end_date
        if self.calendar_dates:
            return "|".join(self.calendar_dates)
        return self.publication_date

    @property
    def temporal_source(self) -> str:
        if self.begin_date or self.end_date:
            return "FGDC range dates"
        if self.calendar_dates:
            return "FGDC calendar date"
        if self.publication_date:
            return "FGDC publication date fallback"
        return ""

    @property
    def temporal_year(self) -> str:
        return _first_year(self.temporal_coverage)

    @property
    def date_range(self) -> str:
        start_year = _first_year(self.begin_date)
        end_year = _first_year(self.end_date)
        if start_year and end_year:
            return f"{start_year}-{end_year}"
        years = [_first_year(value) for value in self.calendar_dates]
        years = [year for year in years if year]
        if years:
            return f"{min(years)}-{max(years)}"
        publication_year = _first_year(self.publication_date)
        return f"{publication_year}-{publication_year}" if publication_year else ""

    @property
    def bounding_box(self) -> str:
        if all((self.west, self.south, self.east, self.north)):
            return f"{self.west},{self.south},{self.east},{self.north}"
        return ""

    @property
    def geometry(self) -> str:
        if not self.bounding_box:
            return ""
        return (
            f"POLYGON(({self.west} {self.north}, {self.east} {self.north}, "
            f"{self.east} {self.south}, {self.west} {self.south}, "
            f"{self.west} {self.north}))"
        )

    @property
    def centroid(self) -> str:
        if not self.bounding_box:
            return ""
        try:
            latitude = (float(self.south) + float(self.north)) / 2
            longitude = (float(self.west) + float(self.east)) / 2
        except ValueError:
            return ""
        return f"{latitude},{longitude}"


def _domain_text(attribute: ET.Element) -> str:
    domains = path_elements(attribute, ("attrdomv",))
    if not domains:
        return ""
    pieces = []
    for node in domains[0].iter():
        if node is domains[0]:
            continue
        value = clean_text(node.text)
        if value:
            pieces.append(value)
    return " | ".join(dict.fromkeys(pieces))


def _attributes(root: ET.Element) -> tuple[FgdcAttribute, ...]:
    rows = []
    for attribute in root.iter():
        if local_name(attribute.tag) != "attr":
            continue
        label = first_path_value(attribute, (("attrlabl",),))
        definition = first_path_value(attribute, (("attrdef",),))
        definition_source = first_path_value(attribute, (("attrdefs",),))
        domain = _domain_text(attribute)
        if any((label, definition, definition_source, domain)):
            rows.append(
                FgdcAttribute(
                    label=label,
                    definition=definition,
                    definition_source=definition_source,
                    domain=domain,
                )
            )
    if rows:
        return tuple(rows)

    # Some Minnesota Geographic Metadata Guidelines records put the entire
    # data dictionary in free text under ``eaover`` instead of using FGDC
    # ``attr`` elements. Retain simple ``FIELD = definition`` and
    # ``FIELD - definition`` entries so these legacy descriptions are not
    # lost when an access data dictionary is generated.
    entry_pattern = re.compile(
        r"^\s*([A-Za-z][A-Za-z0-9_.]*)\s+(?:=|-)\s+(.+?)\s*$"
    )
    for overview in root.iter():
        if local_name(overview.tag) != "eaover":
            continue
        for line in "".join(overview.itertext()).splitlines():
            match = entry_pattern.match(line)
            if not match:
                continue
            rows.append(
                FgdcAttribute(
                    label=match.group(1),
                    definition=clean_text(match.group(2)),
                    definition_source="FGDC entity and attribute overview",
                    domain="",
                )
            )
    return tuple(rows)


def _spatial_reference(root: ET.Element) -> str:
    epsg = first_path_value(
        root,
        (
            ("refsysinfo", "refsystem", "refsysID", "identCode"),
            ("spref", "horizsys", "cordsysn", "projcsn"),
        ),
    )
    if re.fullmatch(r"\d{4,6}", epsg):
        return f"https://spatialreference.org/ref/epsg/{epsg}/"
    projected_name = first_path_value(
        root,
        (
            ("spref", "horizsys", "planar", "mapproj", "mapprojn"),
            ("spref", "horizsys", "cordsysn", "projcsn"),
        ),
    )
    datum = first_path_value(
        root,
        (("spref", "horizsys", "geodetic", "horizdn"),),
    )
    units = first_path_value(
        root,
        (("spref", "horizsys", "planar", "planci", "plandu"),),
    )
    pieces = []
    for label, value in (
        ("Projected CRS", projected_name),
        ("Datum", datum),
        ("Units", units),
    ):
        if value:
            pieces.append(f"{label}: {value}")
    return "; ".join(pieces)


def parse_fgdc_metadata(path: Path) -> FgdcMetadata:
    """Parse selected FGDC CSDGM fields from ``path``."""
    path = Path(path)
    root = ET.parse(path).getroot()

    publication_date = normalize_fgdc_date(
        first_path_value(
            root,
            (("idinfo", "citation", "citeinfo", "pubdate"),),
        )
    )
    calendar_dates = tuple(
        normalize_fgdc_date(value)
        for value in unique_path_values(
            root,
            ("idinfo", "timeperd", "timeinfo", "sngdate", "caldate"),
        )
    )
    if not calendar_dates:
        calendar_dates = tuple(
            normalize_fgdc_date(value)
            for value in unique_path_values(
                root, ("idinfo", "timeperd", "timeinfo", "caldate")
            )
        )

    return FgdcMetadata(
        source_path=path,
        title=first_path_value(
            root,
            (("idinfo", "citation", "citeinfo", "title"),),
        ),
        alternative_title=first_path_value(
            root,
            (("idinfo", "citation", "citeinfo", "edition"),),
        ),
        abstract=strip_html(
            first_path_value(root, (("idinfo", "descript", "abstract"),))
        ),
        purpose=strip_html(
            first_path_value(root, (("idinfo", "descript", "purpose"),))
        ),
        originators=unique_path_values(
            root,
            ("idinfo", "citation", "citeinfo", "origin"),
        ),
        publisher=first_path_value(
            root,
            (("idinfo", "citation", "citeinfo", "pubinfo", "publish"),),
        ),
        publication_date=publication_date,
        calendar_dates=calendar_dates,
        begin_date=normalize_fgdc_date(
            first_path_value(
                root,
                (("idinfo", "timeperd", "timeinfo", "rngdates", "begdate"),),
            )
        ),
        end_date=normalize_fgdc_date(
            first_path_value(
                root,
                (("idinfo", "timeperd", "timeinfo", "rngdates", "enddate"),),
            )
        ),
        currentness_reference=first_path_value(
            root,
            (("idinfo", "timeperd", "current"),),
        ),
        metadata_date=normalize_fgdc_date(
            first_path_value(root, (("metainfo", "metd"),))
        ),
        theme_keywords=unique_path_values(
            root,
            ("idinfo", "keywords", "theme", "themekey"),
        ),
        place_keywords=unique_path_values(
            root,
            ("idinfo", "keywords", "place", "placekey"),
        ),
        access_constraints=first_path_value(root, (("idinfo", "accconst"),)),
        use_constraints=first_path_value(root, (("idinfo", "useconst"),)),
        west=first_path_value(root, (("idinfo", "spdom", "bounding", "westbc"),)),
        east=first_path_value(root, (("idinfo", "spdom", "bounding", "eastbc"),)),
        south=first_path_value(root, (("idinfo", "spdom", "bounding", "southbc"),)),
        north=first_path_value(root, (("idinfo", "spdom", "bounding", "northbc"),)),
        spatial_reference=_spatial_reference(root),
        entity_attribute_detail=first_path_value(
            root,
            (("eainfo", "overview", "eadetcit"),),
        ),
        attributes=_attributes(root),
    )
