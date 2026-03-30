import csv
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table


class OaiQdcHarvester(BaseHarvester):
    """
    Harvester for qualified Dublin Core OAI-PMH records that have already been
    downloaded to local XML files.
    """

    OAI_NS = {
        "oai": "http://www.openarchives.org/OAI/2.0/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dcterms": "http://purl.org/dc/terms/",
        "oai_qdc": "http://worldcat.org/xmlschemas/qdc-1.0/",
    }
    NS_PREFIXES = {uri: prefix for prefix, uri in OAI_NS.items() if prefix != "oai"}

    EMPTY_PRIMARY_COLUMNS = [
        "ID",
        "Title",
        "Alternative Title",
        "Description",
        "Creator",
        "Publisher",
        "Provider",
        "Resource Type",
        "Keyword",
        "Subject",
        "Temporal Coverage",
        "Date Range",
        "Spatial Coverage",
        "Bounding Box",
        "Identifier",
        "Resource Class",
        "Access Rights",
    ]

    def __init__(self, config):
        super().__init__(config)
        self.oai_base_url = self.config["oai_base_url"]
        self.metadata_prefix = self.config.get(
            "metadata_prefix",
            self.config.get("feed_type", "oai_qdc"),
        )
        self.source_name = self.config.get("source_name", self.config.get("name", "the source"))
        self.source_id_prefix = self.config.get(
            "source_id_prefix",
            self.oai_slugify(self.config.get("name", "oai")).replace("-", "_"),
        )
        self.sets_csv = self.config["sets_csv"]
        self.set_column = self.config.get("sets_csv_set_column", "set")
        self.set_title_column = self.config.get("sets_csv_title_column", "title")
        self.download_dir = Path(
            self.config.get(
                "oai_download_dir",
                Path("inputs") / "oai-downloads" / self.config.get("name", "oai_qdc"),
            )
        )

    def load_reference_data(self):
        super().load_reference_data()

    def fetch(self):
        sets = self.oai_load_sets()
        print(
            f"[OAI_QDC] Loaded {len(sets)} set definitions from {self.sets_csv} "
            f"for {self.oai_base_url}"
        )
        if not sets:
            return []

        all_records = []
        missing_sets = []

        for set_row in sets:
            local_files = self.oai_local_xml_files(set_row["set_spec"])
            if not local_files:
                missing_sets.append(set_row["set_spec"])
                continue

            set_records = self.oai_load_local_records(set_row, local_files)
            all_records.extend(set_records)

        if missing_sets:
            missing_list = ", ".join(missing_sets)
            raise FileNotFoundError(
                "[OAI_QDC] Local XML files were not found for set(s): "
                f"{missing_list}. Expected files under {self.download_dir}/<set-spec>/ . "
                "Run scripts/oai_download.py first or set oai_download_dir in the job config."
            )

        print(f"[OAI_QDC] Prepared {len(all_records)} records across {len(sets)} sets.")
        return all_records

    def parse(self, raw_data):
        return raw_data

    def flatten(self, harvested_metadata):
        return harvested_metadata

    def build_dataframe(self, record_rows):
        if not record_rows:
            print("[OAI_QDC] No local OAI records found. Returning an empty dataframe.")
            return pd.DataFrame(columns=self.EMPTY_PRIMARY_COLUMNS)

        return pd.DataFrame(record_rows)

    def derive_fields(self, df):
        df = (
            df.pipe(self.oai_map_to_schema)
              .pipe(self.oai_ensure_required_columns)
        )
        df = super().derive_fields(df)
        return df

    def add_defaults(self, df):
        df = super().add_defaults(df)
        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")
        df["Website Platform"] = "OAI-PMH"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_oai_qdc"
        df["Endpoint Description"] = "OAI-PMH"
        df["Endpoint URL"] = self.oai_base_url

        harvest_statement = (
            f"The metadata for this resource was last retrieved from the "
            f"{self.source_name} on {today}."
        )
        if "Provenance" in df.columns:
            df["Provenance"] = df["Provenance"].apply(
                lambda value: f"{value}|{harvest_statement}" if value else harvest_statement
            )
        else:
            df["Provenance"] = harvest_statement

        return df

    def clean(self, df):
        df = super().clean(df)
        return df

    def validate(self, df):
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = self.oai_build_distributions(primary_df.copy())
        return super().write_outputs(primary_df, distributions_df)

# --- OAI -Specific Functions --- #

    def oai_load_sets(self):
        sets = []
        with open(self.sets_csv, newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                set_spec = str(row.get(self.set_column, "")).strip()
                set_title = str(row.get(self.set_title_column, "")).strip()
                if not set_spec:
                    continue
                sets.append({"set_spec": set_spec, "set_title": set_title})
        return sets

    def oai_build_distributions(self, df):
        landing_base = self.config.get("distribution_landing_base", "").rstrip("/")
        iiif_base = self.config.get("distribution_iiif_base", "").rstrip("/")

        if not landing_base or not iiif_base:
            return generate_secondary_table(df, self.distribution_types)

        rows = []
        for _, row in df.iterrows():
            friendlier_id = row.get("ID", "")
            set_slug, record_number = self.oai_distribution_parts(row)
            if not friendlier_id or not set_slug or not record_number:
                continue

            rows.append(
                {
                    "friendlier_id": friendlier_id,
                    "reference_type": "documentation_external",
                    "distribution_url": f"{landing_base}/{set_slug}/id/{record_number}",
                    "label": "",
                }
            )
            rows.append(
                {
                    "friendlier_id": friendlier_id,
                    "reference_type": "iiif_manifest",
                    "distribution_url": f"{iiif_base}/{set_slug}:{record_number}/manifest.json",
                    "label": "",
                }
            )

        return pd.DataFrame(
            rows,
            columns=["friendlier_id", "reference_type", "distribution_url", "label"],
        )

    def oai_local_xml_files(self, set_spec):
        set_dir = self.download_dir / self.oai_slugify(set_spec)
        if not set_dir.exists():
            return []
        return sorted(path for path in set_dir.glob("*.xml") if path.is_file())

    def oai_load_local_records(self, set_row, xml_files):
        set_records = []
        for xml_path in xml_files:
            xml_text = xml_path.read_text(encoding="utf-8")
            page_records = self.oai_parse_xml(xml_text, set_row)
            set_records.extend(page_records)

        print(
            f"[OAI_QDC] Loaded {len(set_records)} records for set "
            f"{set_row['set_spec']} from {len(xml_files)} local XML page(s)."
        )
        return set_records

    def oai_map_to_schema(self, df):
        if df.empty:
            return pd.DataFrame(columns=self.EMPTY_PRIMARY_COLUMNS)

        mapped_df = df.apply(self.oai_build_schema_row, axis=1, result_type="expand")
        print(
            f"[OAI_QDC] Crosswalked {len(mapped_df)} qualified Dublin Core records using "
            f"metadataPrefix={self.metadata_prefix}."
        )
        return mapped_df

    def oai_ensure_required_columns(self, df):
        for column in self.EMPTY_PRIMARY_COLUMNS:
            if column not in df.columns:
                df[column] = ""
        return df

    def oai_build_schema_row(self, row):
        record = row.to_dict()

        titles = self.oai_values(record, "dc:title", "dcterms:title")
        creators = self.oai_split_people(
            self.oai_values(record, "dc:creator", "dcterms:creator")
        )
        subjects = self.oai_values(record, "dc:subject", "dcterms:subject")
        types = self.oai_values(record, "dc:type", "dcterms:type")
        identifiers = self.oai_values(record, "dc:identifier", "dcterms:identifier")
        all_identifiers = self.oai_unique([record.get("oai_identifier", "")] + identifiers)
        landing_page = self.oai_select_landing_page(all_identifiers)
        dates = self.oai_values(record, "dc:date", "dcterms:date", "dcterms:created")
        temporal_values = self.oai_values(record, "dcterms:temporal")
        temporal_coverage = self.oai_build_temporal_coverage(temporal_values, dates)
        spatial_values = self.oai_values(record, "dcterms:spatial", "dc:coverage")
        spatial_coverage = self.oai_spatial_coverage(spatial_values)
        bounding_box = self.oai_extract_bbox_from_values(spatial_values)
        scale_values = [value for value in spatial_values if self.oai_looks_like_scale(value)]
        format_values = self.oai_values(record, "dc:format", "dcterms:format")
        publisher_values = self.oai_values(record, "dc:publisher", "dcterms:publisher")
        is_part_of = self.oai_values(record, "dcterms:isPartOf")
        local_collection = self.oai_local_collection(record, is_part_of)
        record_id = self.oai_build_id(record, landing_page)
        source_values = self.oai_values(record, "dc:source", "dcterms:source")

        return {
            "ID": record_id,
            "Title": titles[0] if titles else "",
            "Alternative Title": "|".join(titles[1:]),
            "Description": "|".join(
                self.oai_values(record, "dc:description", "dcterms:description")
            ),
            "Creator": "|".join(creators),
            "Publisher": "|".join(publisher_values),
            "Provider": self.config.get("provider", ""),
            "Resource Class": self.oai_resource_class(types, record.get("set_title", "")),
            "Resource Type": "|".join(types),
            "Subject": "|".join(subjects),
            "Keyword": "|".join(self.oai_keywords(subjects, record.get("set_title", ""))),
            "Local Collection": local_collection,
            "Temporal Coverage": temporal_coverage,
            "Date Issued": self.oai_date_issued(dates),
            "Date Range": self.oai_date_range(temporal_values, dates),
            "Spatial Coverage": "|".join(spatial_coverage),
            "Bounding Box": bounding_box,
            "Spatial Resolution as Text": "|".join(scale_values),
            "Extent": self.oai_extent(format_values),
            "Provenance": "|".join(self.oai_values(record, "dcterms:provenance")),
            "Identifier": "|".join(all_identifiers),
            "Rights": "|".join(self.oai_values(record, "dc:rights", "dcterms:rights")),
            "Format": self.oai_format(format_values, all_identifiers),
            "File Size": self.oai_file_size(format_values),
            "Source": "|".join(source_values),
            "Relation": "|".join(
                self.oai_values(record, "dc:relation", "dcterms:relation")
            ),
        }

    def oai_parse_xml(self, xml_text, set_row):
        root = ET.fromstring(xml_text)

        errors = []
        for error in root.findall(".//oai:error", self.OAI_NS):
            errors.append(
                f"{error.attrib.get('code', 'oai_error')}: {(error.text or '').strip()}"
            )
        if errors:
            raise ValueError("[OAI_QDC] OAI-PMH error(s): " + "; ".join(errors))

        record_rows = []
        for record_el in root.findall(".//oai:record", self.OAI_NS):
            parsed_record = self.oai_parse_record(record_el, set_row)
            if parsed_record is not None:
                record_rows.append(parsed_record)

        return record_rows

    def oai_parse_record(self, record_el, set_row):
        header_el = record_el.find("oai:header", self.OAI_NS)
        if header_el is not None and header_el.attrib.get("status") == "deleted":
            return None

        metadata_el = record_el.find("oai:metadata", self.OAI_NS)
        if metadata_el is None:
            return None

        qualifieddc_el = metadata_el.find("oai_qdc:qualifieddc", self.OAI_NS)
        if qualifieddc_el is None:
            return None

        fields = {}
        for child in list(qualifieddc_el):
            key = self.oai_tag_name(child.tag)
            if not key:
                continue
            text = self.oai_normalize_space("".join(child.itertext()))
            if not text:
                continue
            fields.setdefault(key, []).append(text)

        return {
            "oai_identifier": self.oai_text(header_el, "oai:identifier"),
            "datestamp": self.oai_text(header_el, "oai:datestamp"),
            "set_spec": set_row["set_spec"],
            "set_title": set_row.get("set_title", ""),
            "fields": fields,
        }

    def oai_text(self, parent, xpath):
        if parent is None:
            return ""
        child = parent.find(xpath, self.OAI_NS)
        if child is None or child.text is None:
            return ""
        return child.text.strip()

    def oai_tag_name(self, tag):
        if not tag.startswith("{"):
            return ""
        namespace, local_name = tag[1:].split("}", 1)
        prefix = self.NS_PREFIXES.get(namespace)
        if not prefix:
            return ""
        return f"{prefix}:{local_name}"

    def oai_values(self, record, *keys):
        raw_fields = record.get("fields", {})
        values = []
        for key in keys:
            values.extend(raw_fields.get(key, []))
        return self.oai_unique(values)

    def oai_unique(self, values):
        seen = set()
        unique_values = []
        for value in values:
            clean_value = self.oai_normalize_space(value)
            if not clean_value or clean_value in seen:
                continue
            seen.add(clean_value)
            unique_values.append(clean_value)
        return unique_values

    def oai_normalize_space(self, value):
        return re.sub(r"\s+", " ", str(value or "")).strip()

    def oai_split_people(self, values):
        split_values = []
        for value in values:
            parts = [part.strip(" .") for part in re.split(r"\s*;\s*", value) if part.strip()]
            split_values.extend(parts or [value])
        return self.oai_unique(split_values)

    def oai_select_landing_page(self, identifiers):
        http_identifiers = [
            value for value in identifiers if value.lower().startswith(("http://", "https://"))
        ]
        preferred_patterns = ("/cdm/ref/", "/node/")
        for pattern in preferred_patterns:
            for value in http_identifiers:
                if pattern in value:
                    return value
        for value in http_identifiers:
            if "_foxml" not in value.lower():
                return value
        return ""

    def oai_distribution_parts(self, row):
        identifier_values = str(row.get("Identifier", "")).split("|")
        for value in identifier_values:
            match = re.search(r"/collection/([^/]+)/id/([^/?#]+)", value)
            if match:
                return match.group(1), match.group(2)

        friendlier_id = str(row.get("ID", ""))
        prefix = f"{self.source_id_prefix}_"
        if friendlier_id.startswith(prefix):
            suffix = friendlier_id[len(prefix):]
            if "_" in suffix:
                set_slug, record_number = suffix.rsplit("_", 1)
                return set_slug, record_number

        return "", ""

    def oai_build_id(self, record, landing_page):
        set_part = self.oai_slugify(record.get("set_spec", "")).replace("-", "_")
        raw_identifier = (
            self.oai_extract_record_number(landing_page)
            or self.oai_extract_record_number(record.get("oai_identifier", ""))
            or self.oai_slugify(landing_page or record.get("oai_identifier", "")).replace("-", "_")
        )
        return f"{self.source_id_prefix}_{set_part}_{raw_identifier}".strip("_")

    def oai_extract_record_number(self, value):
        text = str(value or "").strip()
        if not text:
            return ""

        patterns = [
            r"/id/([A-Za-z0-9]+)\b",
            r"/node/([A-Za-z0-9-]+)\b",
            r":([A-Za-z]+-)?([A-Za-z0-9]+)$",
            r"\b([A-Za-z]+-)?([A-Za-z0-9]+)$",
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                candidate = match.group(match.lastindex)
                return self.oai_slugify(candidate).replace("-", "_")
        return ""

    def oai_date_issued(self, dates):
        return dates[0] if dates else ""

    def oai_build_temporal_coverage(self, temporal_values, dates):
        if temporal_values:
            return "|".join(temporal_values)

        years = self.oai_years_from_values(dates)
        if not years:
            return ""
        if len(years) == 1:
            return years[0]
        return f"{years[0]}-{years[-1]}"

    def oai_date_range(self, temporal_values, dates):
        source_values = temporal_values if temporal_values else dates
        years = self.oai_years_from_values(source_values)
        if not years:
            return ""
        return f"{years[0]}-{years[-1]}"

    def oai_years_from_values(self, values):
        years = set()
        for value in values:
            for match in re.findall(r"\b(1[6-9]\d{2}|20\d{2}|2100)\b", value):
                years.add(match)
        return sorted(years)

    def oai_spatial_coverage(self, spatial_values):
        coverage = []
        for value in spatial_values:
            if self.oai_looks_like_scale(value):
                continue
            if self.oai_extract_bbox(value):
                continue
            if re.fullmatch(r"[tr]\d+[nsew]", value.lower()):
                continue
            coverage.append(value)
        return self.oai_unique(coverage)

    def oai_extract_bbox_from_values(self, spatial_values):
        for value in spatial_values:
            bbox = self.oai_extract_bbox(value)
            if bbox:
                return bbox
        return ""

    def oai_extract_bbox(self, value):
        clean_value = self.oai_normalize_space(value)
        envelope_match = re.match(r"ENVELOPE\(([^)]+)\)", clean_value, flags=re.IGNORECASE)
        if envelope_match:
            coords = [part.strip() for part in envelope_match.group(1).split(",")]
            if len(coords) == 4:
                west, east, north, south = coords
                return f"{west},{south},{east},{north}"

        bbox_match = re.match(
            r"^\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*$",
            clean_value,
        )
        if not bbox_match:
            return ""

        west, south, east, north = [float(part) for part in bbox_match.groups()]
        if (
            -180 <= west <= 180
            and -90 <= south <= 90
            and -180 <= east <= 180
            and -90 <= north <= 90
        ):
            return f"{west},{south},{east},{north}"
        return ""

    def oai_looks_like_scale(self, value):
        return bool(re.search(r"\b1\s*:\s*\d", value))

    def oai_resource_class(self, types, set_title):
        type_text = " ".join(types).lower()
        set_text = self.oai_normalize_space(set_title).lower()

        if "collection" in type_text:
            return "Collections"
        if any(term in type_text for term in ["aerial", "orthophoto", "satellite", "imagery"]):
            return "Imagery"
        if any(term in type_text for term in ["map", "maps", "cartograph", "stillimage", "image"]):
            return "Maps"
        if any(term in type_text for term in ["dataset", "data set", "tabular"]):
            return "Datasets"
        if any(term in type_text for term in ["website", "interactive resource", "web site"]):
            return "Websites"
        if "collection" in set_text:
            return "Collections"
        return "Other"

    def oai_keywords(self, subjects, set_title):
        keywords = list(subjects)
        if set_title:
            keywords.append(set_title)
        return self.oai_unique(keywords)

    def oai_local_collection(self, record, is_part_of_values):
        if is_part_of_values:
            return "|".join(is_part_of_values)
        if record.get("set_title"):
            return record["set_title"]
        return ""

    def oai_format(self, format_values, identifiers):
        extension_map = {
            ".tif": "TIFF",
            ".tiff": "TIFF",
            ".jpg": "JPEG",
            ".jpeg": "JPEG",
            ".jp2": "JPEG2000",
            ".png": "PNG",
            ".pdf": "PDF",
            ".geojson": "GeoJSON",
            ".json": "GeoJSON",
            ".zip": "Files",
        }

        format_text = " ".join(format_values).lower()
        for extension, mapped_format in extension_map.items():
            for identifier in identifiers:
                if identifier.lower().endswith(extension):
                    return mapped_format

        if "paper map" in format_text:
            return "TIFF"
        if "pdf" in format_text:
            return "PDF"
        if "jpeg" in format_text:
            return "JPEG"
        if "tiff" in format_text or "tif" in format_text:
            return "TIFF"
        return ""

    def oai_file_size(self, format_values):
        for value in format_values:
            clean_value = self.oai_normalize_space(value)
            if clean_value.isdigit():
                return clean_value
        return ""

    def oai_extent(self, format_values):
        extent_values = []
        for value in format_values:
            clean_value = self.oai_normalize_space(value)
            if clean_value.isdigit():
                continue
            if re.search(r"\b(cm|mm|inches|inch|ft|feet|paper map)\b", clean_value, flags=re.IGNORECASE):
                extent_values.append(clean_value)
        return "|".join(self.oai_unique(extent_values))

    def oai_slugify(self, value):
        value = re.sub(r"[^A-Za-z0-9._-]+", "-", str(value or "").strip())
        value = re.sub(r"-{2,}", "-", value)
        return value.strip("-") or "unnamed"
