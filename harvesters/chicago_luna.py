import csv
import json
import hashlib
import os
import re
import time
from urllib.parse import urlparse

import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.temporal_fields import create_date_range


class ChicagoLunaHarvester(BaseHarvester):
    """
    Harvester for a University of Chicago LUNA IIIF collection.

    This currently maps collection item references into a minimal schema-compatible
    table. The upstream endpoint may return an access verification page instead of
    IIIF JSON, so this class handles that case by returning an empty collection.
    """

    EMPTY_PRIMARY_COLUMNS = [
        "ID",
        "Identifier",
        "Title",
        "Description",
        "Creator",
        "Keyword",
        "Access Rights",
        "Resource Class",
        "Resource Type",
        "Format",
        "Language",
        "Publisher",
        "Provider",
        "Temporal Coverage",
        "Date Issued",
        "Date Range",
        "Spatial Coverage",
        "Bounding Box",
        "Spatial Resolution as Text",
        "manifest",
        "iiif",
        "image",
        "thumbnail",
        "information",
    ]

    def __init__(self, config):
        super().__init__(config)
        self.collection_url = self.config["collection_url"]
        self.input_csv = self.config.get("input_csv", "")
        self.input_json = self.config.get("input_json", "inputs/chicago_luna.json")
        self.raw_canvas_df = pd.DataFrame()

    def load_reference_data(self):
        super().load_reference_data()

    def fetch(self):
        if self.input_csv and os.path.exists(self.input_csv):
            records = self.chicago_luna_load_raw_csv()
            print(
                f"[Chicago Luna] Loaded {len(records)} raw canvas records "
                f"from {self.input_csv}"
            )
            return records

        if not self.input_json or not os.path.exists(self.input_json):
            print(
                f"[Chicago Luna] Error: no local input found. "
                f"Tried input_csv={self.input_csv or '<unset>'} and input_json={self.input_json}"
            )
            return []

        bundle = self.chicago_luna_load_bundle()
        print(
            f"[Chicago Luna] Loaded {len(bundle.get('manifests', []))} manifests "
            f"from {self.input_json}"
        )
        return bundle

    def parse(self, raw_data):
        if isinstance(raw_data, list):
            return raw_data

        if not isinstance(raw_data, dict):
            return []

        return self.chicago_luna_parse_bundle(raw_data)

    def flatten(self, harvested_metadata):
        return harvested_metadata

    def build_dataframe(self, parsed_data):
        self.raw_canvas_df = pd.DataFrame(parsed_data)

        if not parsed_data:
            print("[Chicago Luna] No local manifest records found. Returning an empty dataframe.")
            return pd.DataFrame(columns=self.EMPTY_PRIMARY_COLUMNS)

        self.raw_canvas_df = self.chicago_luna_enrich_raw_dataframe(self.raw_canvas_df)
        mapped_rows = [
            self.chicago_luna_build_schema_row(record)
            for record in self.raw_canvas_df.to_dict(orient="records")
        ]
        return pd.DataFrame(mapped_rows).reindex(columns=self.EMPTY_PRIMARY_COLUMNS)

    def derive_fields(self, df):
        df = super().derive_fields(df)
        if "Temporal Coverage" not in df.columns:
            df["Temporal Coverage"] = ""
        if "Date Issued" not in df.columns:
            df["Date Issued"] = ""

        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1,
        )
        return df

    def add_defaults(self, df):
        df = super().add_defaults(df)
        df["Code"] = df.get("Code", "12d-00")
        df["Member Of"] = df.get("Member Of", "64bd8c4c-8e60-4956-b43d-bdc3f93db488")
        df["Is Part Of"] = df.get("Is Part Of", "")
        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")
        df["Website Platform"] = "LUNA"
        df["Accrual Method"] = "Automated retrieval"
        df["Harvest Workflow"] = "py_chicago_luna"
        df["Endpoint Description"] = "IIIF Presentation API"
        df["Endpoint URL"] = self.collection_url
        df["Provenance"] = (
            f"The metadata for this resource was last retrieved from the University of Chicago Libraries on {today}."
        )
        return df

    def clean(self, df):
        df = super().clean(df)
        return df

    def validate(self, df):
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        results = super().write_outputs(primary_df, distributions_df)

        raw_out = self.config.get("output_raw_csv")
        if raw_out:
            today = time.strftime("%Y-%m-%d")
            output_dir = "outputs"
            os.makedirs(output_dir, exist_ok=True)
            raw_filename = os.path.join(output_dir, f"{today}_{os.path.basename(raw_out)}")
            raw_df = self.raw_canvas_df.copy()
            raw_df.to_csv(raw_filename, index=False, encoding="utf-8")
            results["raw_csv"] = raw_filename

        return results

# --- Chicago Luna-Specific Functions --- #

    def chicago_luna_load_bundle(self):
        with open(self.input_json, encoding="utf-8") as handle:
            return json.load(handle)

    def chicago_luna_load_raw_csv(self):
        with open(self.input_csv, encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            return [row for row in reader]

    def chicago_luna_parse_bundle(self, bundle):
        collection_title = self._extract_label(bundle.get("label")) or self.config.get("name", "Chicago Luna")
        parsed_items = []

        for manifest_json in bundle.get("manifests", []):
            if not isinstance(manifest_json, dict):
                continue

            canvas_records = self.chicago_luna_extract_canvas_records(
                manifest_json=manifest_json,
                collection_title=collection_title,
            )
            parsed_items.extend(canvas_records)
            print(
                f"[Chicago Luna] Manifest {self._extract_resource_id(manifest_json)} "
                f"yielded {len(canvas_records)} canvas records."
            )

        print(f"[Chicago Luna] Parsed {len(parsed_items)} collection items.")
        return parsed_items

    def chicago_luna_extract_canvas_records(self, manifest_json, collection_title=""):
        manifest_id = self._extract_resource_id(manifest_json)
        manifest_label = self._extract_label(manifest_json.get("label"))
        manifest_related = self._extract_first_url(manifest_json.get("related"))
        manifest_service = self._extract_first_url(manifest_json.get("service"))
        manifest_attribution = self._extract_label(manifest_json.get("attribution"))

        records = []
        for sequence_index, sequence in enumerate(manifest_json.get("sequences", [])):
            if not isinstance(sequence, dict):
                continue

            for canvas_index, canvas in enumerate(sequence.get("canvases", [])):
                if not isinstance(canvas, dict):
                    continue

                metadata_map = self._metadata_to_multimap(canvas.get("metadata", []))
                image_resource = self._extract_first_image_resource(canvas.get("images", []))
                image_service = self._extract_first_url(image_resource.get("service"))
                image_url = self._extract_resource_id(image_resource) or self._extract_thumbnail(canvas.get("thumbnail"))
                canvas_descriptions = metadata_map.get("description", [])

                records.append(
                    {
                        "collection_title": collection_title,
                        "manifest_id": manifest_id,
                        "manifest_label": manifest_label,
                        "manifest_attribution": manifest_attribution,
                        "manifest_related": manifest_related,
                        "manifest_service": manifest_service,
                        "manifest_json": json.dumps(manifest_json, ensure_ascii=True),
                        "canvas_index": canvas_index,
                        "sequence_index": sequence_index,
                        "canvas_id": self._extract_resource_id(canvas),
                        "canvas_type": canvas.get("@type") or canvas.get("type", ""),
                        "canvas_label": self._extract_label(canvas.get("label")),
                        "canvas_description": self._extract_label(canvas.get("description")),
                        "canvas_metadata_json": json.dumps(canvas.get("metadata", []), ensure_ascii=True),
                        "canvas_images_json": json.dumps(canvas.get("images", []), ensure_ascii=True),
                        "canvas_json": json.dumps(canvas, ensure_ascii=True),
                        "canvas_thumbnail": self._extract_thumbnail(canvas.get("thumbnail")),
                        "canvas_width": canvas.get("width", ""),
                        "canvas_height": canvas.get("height", ""),
                        "call_number": "|".join(metadata_map.get("call number", [])),
                        "filename": "|".join(metadata_map.get("filename", [])),
                        "identifier_text": "|".join(metadata_map.get("identifier", [])),
                        "title_text": "|".join(metadata_map.get("title", [])),
                        "creator": "|".join(metadata_map.get("creator", [])),
                        "subject": "|".join(metadata_map.get("subject", [])),
                        "description_text": "|".join(canvas_descriptions),
                        "publisher_text": "|".join(metadata_map.get("publisher", [])),
                        "date_text": "|".join(metadata_map.get("date", [])),
                        "type_text": "|".join(metadata_map.get("type", [])),
                        "format_text": "|".join(metadata_map.get("format", [])),
                        "coverage_text": "|".join(metadata_map.get("coverage", [])),
                        "source_id": manifest_id,
                        "source_title": self._extract_label(canvas.get("label")) or manifest_label,
                        "source_summary": self._extract_label(canvas.get("description")) or "|".join(canvas_descriptions),
                        "manifest": manifest_id,
                        "iiif": image_service,
                        "image": image_url,
                        "thumbnail": self._extract_thumbnail(canvas.get("thumbnail")),
                        "information": manifest_related or manifest_id or self.collection_url,
                    }
                )

        return records

    def chicago_luna_build_schema_row(self, record):
        df = pd.DataFrame([record])
        row = {}
        row["ID"] = self._series_or_default(df, "canvas_id").replace("", pd.NA).fillna(
            self._series_or_default(df, "manifest_id")
        ).iloc[0]
        row["ID"] = self._build_record_id(row["ID"])
        row["Identifier"] = self._series_or_default(df, "identifier_text").replace("", pd.NA).fillna(
            self._series_or_default(df, "manifest_id")
        ).iloc[0]
        row["Title"] = self._series_or_default(df, "title_text").replace("", pd.NA).fillna(
            self._series_or_default(df, "canvas_label").replace("", pd.NA)
        ).fillna(row["ID"]).iloc[0]
        description_parts = [
            self._series_or_default(df, "description_text").iloc[0],
            self._series_or_default(df, "format_description_text").iloc[0],
            self._series_or_default(df, "canvas_description").iloc[0],
        ]
        row["Description"] = self._join_pipe_values(description_parts)
        row["Creator"] = self._series_or_default(df, "creator").iloc[0]
        row["Keyword"] = self._series_or_default(df, "subject").iloc[0]
        row["Access Rights"] = "Public"
        row["Resource Class"] = "Maps"
        row["Resource Type"] = self.chicago_luna_clean_resource_type(
            self._series_or_default(df, "type_text").iloc[0]
        )
        row["Format"] = "JPEG"
        row["Language"] = self.chicago_luna_detect_language(row["Title"])
        row["Publisher"] = self.chicago_luna_clean_publisher(
            self._series_or_default(df, "publisher_text").replace("", pd.NA).fillna("").iloc[0]
        )
        row["Provider"] = "University of Chicago"
        row["Temporal Coverage"] = self.chicago_luna_temporal_coverage(
            self._series_or_default(df, "date_text").iloc[0]
        )
        row["Date Issued"] = self.chicago_luna_date_issued(
            self._series_or_default(df, "date_text").iloc[0]
        )
        row["Date Range"] = ""
        row["Spatial Coverage"] = self.chicago_luna_clean_spatial_coverage(
            self._series_or_default(df, "coverage_text").iloc[0]
        )
        row["Bounding Box"] = self._series_or_default(df, "temp-Bbox").iloc[0]
        row["Spatial Resolution as Text"] = self._series_or_default(
            df, "Spatial Resolution as Text"
        ).iloc[0]
        row["manifest"] = self._series_or_default(df, "manifest_id").iloc[0]
        row["iiif"] = self._series_or_default(df, "iiif").iloc[0]
        row["image"] = self._series_or_default(df, "image").iloc[0]
        row["thumbnail"] = self._series_or_default(df, "thumbnail").iloc[0]
        row["information"] = self._series_or_default(df, "information", self.collection_url).iloc[0]
        return row

    def chicago_luna_enrich_raw_dataframe(self, df):
        format_parts = df.get("format_text", pd.Series("", index=df.index)).apply(
            self.chicago_luna_parse_format_text
        )
        format_df = pd.DataFrame(list(format_parts), index=df.index)
        return pd.concat([df, format_df], axis=1)

    def chicago_luna_parse_format_text(self, format_text):
        spatial_resolution_values = []
        temp_bbox_values = []
        description_values = []

        for part in str(format_text or "").split("|"):
            clean_part = part.strip()
            if not clean_part:
                continue

            bbox_matches = [match.strip() for match in re.findall(r"\(([^()]+)\)", clean_part) if match.strip()]
            temp_bbox_values.extend(bbox_matches)

            remainder = re.sub(r"\([^()]+\)", "", clean_part).strip(" ;,.")
            if remainder.lower().startswith("scale"):
                spatial_resolution_values.append(remainder.rstrip("."))
                continue

            if remainder:
                description_values.append(remainder)

        return {
            "Spatial Resolution as Text": self._join_pipe_values(spatial_resolution_values),
            "temp-Bbox": self._join_pipe_values(temp_bbox_values),
            "format_description_text": self._join_pipe_values(description_values),
        }

    def chicago_luna_temporal_coverage(self, date_text):
        text = str(date_text or "").strip()
        if not text:
            return ""

        years = re.findall(r"\b(1[5-9]\d{2}|20\d{2})\b", text)
        if not years:
            return ""
        if len(years) == 1:
            return years[0]
        return f"{years[0]}-{years[-1]}"

    def chicago_luna_date_issued(self, date_text):
        text = str(date_text or "").strip()
        if not text:
            return ""

        years = re.findall(r"\b(1[5-9]\d{2}|20\d{2})\b", text)
        return years[0] if years else ""

    def chicago_luna_clean_publisher(self, publisher_text):
        values = []
        for part in str(publisher_text or "").split("|"):
            clean_part = part.strip()
            if not clean_part:
                continue

            if ":" in clean_part:
                clean_part = clean_part.split(":", 1)[1].strip()

            clean_part = clean_part.strip(" ;,")
            if clean_part:
                values.append(clean_part)

        return self._join_pipe_values(values)

    def chicago_luna_clean_spatial_coverage(self, coverage_text):
        values = []
        for part in str(coverage_text or "").split("|"):
            clean_part = part.strip()
            if not clean_part:
                continue

            normalized = clean_part.replace("Unitd States", "United States")
            segments = [segment.strip(" .;,") for segment in normalized.split("--") if segment.strip(" .;,")]

            if segments and segments[0].lower() == "united states":
                segments = segments[1:]

            segments = [self.chicago_luna_normalize_place_segment(segment) for segment in segments]

            if segments == ["Illinois", "Cook County", "Chicago"]:
                clean_part = "Illinois--Cook County"
            else:
                clean_part = "--".join(segments)

            if clean_part:
                values.append(clean_part)

        return self._join_pipe_values(values)

    def chicago_luna_clean_resource_type(self, type_text):
        values = []
        for part in str(type_text or "").split("|"):
            clean_part = part.strip()
            if not clean_part:
                continue

            clean_part = clean_part.split("--", 1)[0].strip()
            clean_part = clean_part.replace(".", "").strip()
            if not clean_part:
                continue

            lowered = clean_part.lower()
            if lowered in {"cartographic", "maps"}:
                continue

            values.append(clean_part)

        return self._join_pipe_values(values)

    def chicago_luna_normalize_place_segment(self, segment):
        text = str(segment or "").strip()
        lowered = text.lower()
        if lowered == "hicago":
            return "Chicago"
        return text

    def chicago_luna_detect_language(self, title):
        text = str(title or "").strip().lower()
        if not text:
            return ""

        normalized_text = re.sub(r"\s+", " ", text)
        french_score = 0
        english_score = 0
        german_score = 0
        latin_score = 0

        if re.search(r"[àâçéèêëîïôùûüœæ]", normalized_text):
            french_score += 2
        if re.search(r"[äöüß]", normalized_text):
            german_score += 2

        french_patterns = [
            r"\bcarte\b",
            r"\bcartes\b",
            r"\bplan\b",
            r"\bplans\b",
            r"\bfeuille\b",
            r"\bfeuilles\b",
            r"\bdress[eé]\b",
            r"\bgrav[eé]\b",
            r"\bpubli[eé]\b",
            r"\bd[ée]partement\b",
            r"\barrondissement\b",
            r"\bcommune\b",
            r"\bchemin\b",
            r"\benvirons\b",
            r"\bg[eé]ographique\b",
            r"\b[eé]chelle\b",
        ]
        english_patterns = [
            r"\bchicago\b",
            r"\bmap\b",
            r"\bmaps\b",
            r"\batlas\b",
            r"\bplat\b",
            r"\bplats\b",
            r"\bchart\b",
            r"\bcensus\b",
            r"\bpostal\b",
            r"\binsurance\b",
            r"\bfire insurance\b",
            r"\bland use\b",
            r"\bdrainage\b",
            r"\bstreet\b",
            r"\bstreets\b",
            r"\bavenue\b",
            r"\bavenues\b",
            r"\broad\b",
            r"\broads\b",
            r"\bhighway\b",
            r"\bhighways\b",
            r"\bward\b",
            r"\bwards\b",
            r"\bprecinct\b",
            r"\bprecincts\b",
            r"\belection\b",
            r"\belections\b",
            r"\bsoil\b",
            r"\bgeologic\b",
            r"\bcounty\b",
            r"\bcounties\b",
            r"\bcity\b",
            r"\bcities\b",
            r"\btown\b",
            r"\btowns\b",
            r"\btownship\b",
            r"\btownships\b",
            r"\bsurvey\b",
            r"\bsurveys\b",
            r"\btopographic\b",
            r"\bsheet\b",
            r"\bsheets\b",
            r"\bquadrangle\b",
            r"\bquadrangles\b",
            r"\brailroad\b",
            r"\brailroads\b",
            r"\bharbor\b",
            r"\briver\b",
            r"\brivers\b",
            r"\blake\b",
            r"\blakes\b",
            r"\bterritory\b",
            r"\bstate\b",
            r"\bstates\b",
            r"\bgeological survey\b",
            r"\bunited states\b",
            r"\bdepartment\b",
            r"\bvicinity\b",
        ]
        german_patterns = [
            r"\bkarte\b",
            r"\bkarten\b",
            r"\bplan von\b",
            r"\bblatt\b",
            r"\bblätter\b",
            r"\bdeutschland\b",
            r"\bpreussen\b",
            r"\bgezeichnet\b",
            r"\bherausgegeben\b",
            r"\bfluss\b",
            r"\bgebiet\b",
        ]
        russian_patterns = [
            r"\bkarta\b",
            r"\brossi[ī]i\b",
            r"\brossiiskoi\b",
            r"\bevrop[eĭ]+\b",
            r"\bguberni[ī]\b",
            r"\bui[e͡]zdam\b",
            r"\boblast\b",
            r"\bselo\b",
            r"\bsel[ʹ']skikh\b",
            r"\buchilishch\b",
            r"\bnaseleni[ī]\b",
            r"\bvozrasta\b",
            r"\bgoda\b",
            r"\bsostavil[ai]?\b",
            r"\bstatisticheskago\b",
            r"\bkomiteta\b",
            r"\bpetrovski[ĭ]?\b",
            r"\bdubrovski[ĭ]?\b",
            r"\bt[s͡]entral[ʹ']nago\b",
        ]
        latin_patterns = [
            r"\btabula\b",
            r"\btabulae\b",
            r"\borbis\b",
            r"\bterrae\b",
            r"\bimperii\b",
            r"\bnova\b",
            r"\bnovum\b",
            r"\bamericae\b",
            r"\beuropae\b",
            r"\basiae\b",
            r"\bafricae\b",
            r"\bseptentrionalis\b",
            r"\bmeridionalis\b",
            r"\binsula\b",
            r"\binsulae\b",
            r"\bcosmographia\b",
        ]

        french_score += sum(1 for pattern in french_patterns if re.search(pattern, normalized_text))
        english_score += sum(1 for pattern in english_patterns if re.search(pattern, normalized_text))
        german_score += sum(1 for pattern in german_patterns if re.search(pattern, normalized_text))
        russian_score = sum(1 for pattern in russian_patterns if re.search(pattern, normalized_text))
        latin_score += sum(1 for pattern in latin_patterns if re.search(pattern, normalized_text))

        scores = {
            "fre": french_score,
            "eng": english_score,
            "ger": german_score,
            "rus": russian_score,
            "lat": latin_score,
        }
        best_code, best_score = max(scores.items(), key=lambda item: item[1])
        if best_score <= 0:
            return ""

        sorted_scores = sorted(scores.values(), reverse=True)
        if len(sorted_scores) > 1 and best_score == sorted_scores[1]:
            return ""

        if best_code == "fre":
            return "fre"
        if best_code == "eng":
            return "eng"
        if best_code == "ger":
            return "ger"
        if best_code == "rus":
            return "rus"
        if best_code == "lat":
            return "lat"
        return ""

    @staticmethod
    def _metadata_to_multimap(metadata_entries):
        metadata_map = {}
        if not isinstance(metadata_entries, list):
            return metadata_map

        for entry in metadata_entries:
            if not isinstance(entry, dict):
                continue

            label = str(entry.get("label", "")).strip().lower()
            value = ChicagoLunaHarvester._extract_label(entry.get("value"))
            if not label or not value:
                continue

            metadata_map.setdefault(label, []).append(value)

        return metadata_map

    @staticmethod
    def _extract_first_image_resource(images):
        if not isinstance(images, list):
            return {}

        for image in images:
            if not isinstance(image, dict):
                continue
            resource = image.get("resource")
            if isinstance(resource, dict):
                return resource

        return {}

    @staticmethod
    def _extract_label(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            return next((str(item).strip() for item in value if str(item).strip()), "")
        if isinstance(value, dict):
            for candidate in value.values():
                text = ChicagoLunaHarvester._extract_label(candidate)
                if text:
                    return text
        return ""

    @staticmethod
    def _extract_first_url(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            for item in value:
                url = ChicagoLunaHarvester._extract_first_url(item)
                if url:
                    return url
        if isinstance(value, dict):
            for key in ("id", "@id", "href"):
                if value.get(key):
                    return str(value[key]).strip()
        return ""

    @staticmethod
    def _extract_thumbnail(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, list):
            for item in value:
                thumb = ChicagoLunaHarvester._extract_thumbnail(item)
                if thumb:
                    return thumb
        if isinstance(value, dict):
            if value.get("id"):
                return str(value["id"]).strip()
            if value.get("@id"):
                return str(value["@id"]).strip()
        return ""

    @staticmethod
    def _extract_resource_id(value):
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, dict):
            if value.get("id"):
                return str(value["id"]).strip()
            if value.get("@id"):
                return str(value["@id"]).strip()
        return ""

    @staticmethod
    def _build_record_id(source_id):
        source_id = str(source_id or "").strip()
        if not source_id:
            return "unknown"

        path = urlparse(source_id).path.rstrip("/")
        parts = [part for part in path.split("/") if part]
        if parts[-2:] and len(parts) >= 2 and parts[-2] == "canvas":
            base = f"{parts[-3]}-{parts[-1]}" if len(parts) >= 3 else parts[-1]
        else:
            base = "-".join(parts[-2:]) if len(parts) >= 2 else (parts[-1] if parts else "")

        slug = re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")
        if slug:
            return slug

        digest = hashlib.sha1(source_id.encode("utf-8")).hexdigest()[:12]
        return digest

    @staticmethod
    def _series_or_default(df, column_name, default=""):
        if column_name in df.columns:
            return df[column_name].fillna(default)
        return pd.Series([default] * len(df), index=df.index, dtype="string")

    @staticmethod
    def _join_pipe_values(values):
        cleaned = []
        seen = set()
        for value in values:
            try:
                if pd.isna(value):
                    continue
            except TypeError:
                pass
            text = str(value or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            cleaned.append(text)
        return "|".join(cleaned)
