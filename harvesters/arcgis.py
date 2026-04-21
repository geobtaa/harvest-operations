import csv
import glob
import time
import os
import re
from urllib.parse import urlparse, parse_qs

import requests
import pandas as pd

from harvesters.base import BaseHarvester
from utils.distribution_writer import generate_secondary_table
from utils.resource_type_match import match_resource_type
from utils.temporal_fields import infer_temporal_coverage_from_title, create_date_range

class ArcGISHarvester(BaseHarvester):
    def __init__(self, config):
        # Initialize the ArcGIS harvester with the shared harvester configuration.
        super().__init__(config)
        self.workflow_input_path = self._resolve_workflow_input_path()
        self.hub_metadata_path = self._resolve_hub_metadata_path()
        self._hub_metadata_lookup = None

    def load_reference_data(self):
        # Load shared lookup data and reference tables required by downstream transforms.
        super().load_reference_data()
        self._hub_metadata_lookup = self._load_hub_metadata_lookup()

    def _resolve_workflow_input_path(self) -> str:
        # Resolve the dated ArcGIS workflow input CSV, falling back to the latest available run output.
        configured_path = str(
            self.config.get("input_csv", "inputs/{date}_harvest-workflow-inputs/py-arcgis-hub.csv")
        ).strip()
        if not configured_path:
            return ""

        dated_path = configured_path.replace("{date}", time.strftime("%Y-%m-%d"))
        if os.path.exists(dated_path) or "{date}" not in configured_path:
            return dated_path

        pattern = configured_path.replace("{date}", "*")
        matches = sorted(glob.glob(pattern))
        if matches:
            return matches[-1]

        return dated_path

    def _resolve_hub_metadata_path(self) -> str:
        # Resolve the ArcGIS hub metadata defaults CSV path from config or the repo default.
        return str(
            self.config.get("hub_metadata_csv", "reference_data/arcHub_metadata.csv")
        ).strip()

    def _read_csv_rows(self, csv_path: str) -> list[dict]:
        # Load a CSV file into memory as row dictionaries when the file exists.
        if not csv_path or not os.path.exists(csv_path):
            return []

        with open(csv_path, newline="", encoding="utf-8-sig") as handle:
            return list(csv.DictReader(handle))

    def _normalize_lookup_key(self, value: str) -> str:
        # Normalize row identifiers so workflow records and hub defaults can be matched reliably.
        return str(value or "").strip().lower()

    def _lookup_keys_for_row(self, row: dict) -> list[str]:
        # Generate candidate lookup keys from the row's code, identifier, and id values.
        keys = []
        for raw_value in (
            row.get("Code", ""),
            row.get("Identifier", ""),
            row.get("ID", ""),
        ):
            normalized = self._normalize_lookup_key(raw_value)
            if not normalized:
                continue
            keys.append(normalized)
            if normalized.startswith("harvest_"):
                keys.append(normalized.removeprefix("harvest_"))

        seen = set()
        ordered_keys = []
        for key in keys:
            if key not in seen:
                seen.add(key)
                ordered_keys.append(key)
        return ordered_keys

    def _load_hub_metadata_lookup(self) -> dict[str, dict]:
        # Build a lookup of ArcGIS hub metadata rows keyed by their known identifiers.
        lookup = {}
        for row in self._read_csv_rows(self.hub_metadata_path):
            for key in self._lookup_keys_for_row(row):
                lookup[key] = row
        return lookup

    def _match_hub_defaults(self, workflow_row: dict) -> dict:
        # Retrieve the metadata defaults row that corresponds to the current workflow record.
        if self._hub_metadata_lookup is None:
            self._hub_metadata_lookup = self._load_hub_metadata_lookup()

        for key in self._lookup_keys_for_row(workflow_row):
            matched_row = self._hub_metadata_lookup.get(key)
            if matched_row:
                return matched_row.copy()
        return {}

    def _first_non_empty(self, *values: str) -> str:
        # Return the first non-empty string from a list of candidate values.
        for value in values:
            cleaned = str(value or "").strip()
            if cleaned:
                return cleaned
        return ""

    def _normalize_workflow_title(self, title: str) -> str:
        # Strip the harvest-record prefix when a workflow title is used as a hub-title fallback.
        cleaned = str(title or "").strip()
        prefix = "Harvest record for "
        if cleaned.startswith(prefix):
            return cleaned[len(prefix):].strip()
        return cleaned

    def _build_parent_hub_rows(self, indexed_ids: set[str], today: str) -> pd.DataFrame:
        # Reconstruct ArcGIS hub parent rows from workflow lifecycle fields plus metadata defaults.
        parent_rows = []

        for workflow_row in self._read_csv_rows(self.workflow_input_path):
            hub_defaults = self._match_hub_defaults(workflow_row)
            parent_id = self._first_non_empty(
                hub_defaults.get("ID", ""),
                hub_defaults.get("Code", ""),
                workflow_row.get("Code", ""),
                workflow_row.get("Identifier", ""),
                workflow_row.get("ID", ""),
            )
            parent_code = self._first_non_empty(
                hub_defaults.get("Code", ""),
                hub_defaults.get("ID", ""),
                workflow_row.get("Code", ""),
                workflow_row.get("Identifier", ""),
                workflow_row.get("ID", ""),
            )

            parent_rows.append(
                {
                    "Title": self._first_non_empty(
                        hub_defaults.get("Title", ""),
                        self._normalize_workflow_title(workflow_row.get("Title", "")),
                    ),
                    "Creator": self._first_non_empty(
                        hub_defaults.get("Creator", ""),
                        workflow_row.get("Creator", ""),
                    ),
                    "Publisher": self._first_non_empty(
                        hub_defaults.get("Publisher", ""),
                        workflow_row.get("Publisher", ""),
                    ),
                    "Subject": self._first_non_empty(
                        hub_defaults.get("Subject", ""),
                        workflow_row.get("Subject", ""),
                    ),
                    "Language": self._first_non_empty(workflow_row.get("Language", ""), "eng"),
                    "Provenance": workflow_row.get("Provenance", ""),
                    "Publication State": self._first_non_empty(
                        workflow_row.get("Publication State", ""),
                        "published",
                    ),
                    "Website Platform": self._first_non_empty(
                        workflow_row.get("Website Platform", ""),
                        "ArcGIS Hub",
                    ),
                    "Endpoint URL": workflow_row.get("Endpoint URL", ""),
                    "Endpoint Description": self._first_non_empty(
                        workflow_row.get("Endpoint Description", ""),
                        "DCAT API",
                    ),
                    "Is Harvested": "True" if parent_code in indexed_ids else "False",
                    "Last Harvested": today,
                    "Accrual Method": self._first_non_empty(
                        workflow_row.get("Accrual Method", ""),
                        "Automated retrieval",
                    ),
                    "Accrual Periodicity": workflow_row.get("Accrual Periodicity", ""),
                    "Harvest Workflow": self._first_non_empty(
                        workflow_row.get("Harvest Workflow", ""),
                        "py_arcgis_hub",
                    ),
                    "Date Accessioned": today,
                    "Resource Class": self._first_non_empty(
                        workflow_row.get("Resource Class", ""),
                        "Websites",
                    ),
                    "Resource Type": self._first_non_empty(
                        workflow_row.get("Resource Type", ""),
                        "Data portals",
                    ),
                    "Member Of": self._first_non_empty(
                        hub_defaults.get("Member Of", ""),
                        workflow_row.get("Member Of", ""),
                    ),
                    "Rights": workflow_row.get("Rights", ""),
                    "Access Rights": self._first_non_empty(
                        workflow_row.get("Access Rights", ""),
                        "Public",
                    ),
                    "ID": parent_id,
                    "Identifier": self._first_non_empty(
                        hub_defaults.get("Identifier", ""),
                        workflow_row.get("Identifier", ""),
                    ),
                    "Code": parent_code,
                    "Spatial Coverage": self._first_non_empty(
                        hub_defaults.get("Spatial Coverage", ""),
                        workflow_row.get("Spatial Coverage", ""),
                    ),
                    "Bounding Box": self._first_non_empty(
                        hub_defaults.get("Bounding Box", ""),
                        workflow_row.get("Bounding Box", ""),
                    ),
                    "Tags": workflow_row.get("Tags", ""),
                    "Admin Note": workflow_row.get("Admin Note", ""),
                }
            )

        return pd.DataFrame(parent_rows)

    def fetch(self):
        # Request each configured ArcGIS Hub endpoint and yield the workflow row plus matched defaults.
        if not self.workflow_input_path or not os.path.exists(self.workflow_input_path):
            raise FileNotFoundError(
                f"[ArcGIS] Workflow input CSV not found: {self.workflow_input_path or '<unset>'}"
            )

        with open(self.workflow_input_path, newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for workflow_row in reader:
                hub_defaults = self._match_hub_defaults(workflow_row)
                website_id = self._first_non_empty(
                    hub_defaults.get("ID", ""),
                    hub_defaults.get("Code", ""),
                    workflow_row.get("Code", ""),
                    workflow_row.get("Identifier", ""),
                    workflow_row.get("ID", ""),
                )
                endpoint_url = workflow_row.get('Endpoint URL', '')
                try:
                    resp = requests.get(endpoint_url, timeout=30)
                    resp.raise_for_status()
                    json_api = resp.json()
                except Exception as e:
                    yield f"[ArcGIS] Error fetching {website_id}: {e}"
                    continue

                hub_title = self._first_non_empty(
                    hub_defaults.get("Title", ""),
                    self._normalize_workflow_title(workflow_row.get("Title", "")),
                )
                yield f"[ArcGIS] Fetched {website_id} — {hub_title or 'No Title'}"
                yield {
                    "workflow": workflow_row,
                    "hub_defaults": hub_defaults,
                    "fetched_catalog": json_api,
                }

    
    def flatten(self, harvested_records):
        # Expand each harvested hub record into one row per dataset resource.

        flattened_list = []

        for source_record in harvested_records:
            if not isinstance(source_record, dict):
                continue

            workflow_record = source_record.get("workflow", source_record)
            hub_defaults = source_record.get("hub_defaults") or workflow_record

            # Extract the list of datasets from within the fetched catalog
            resources = source_record.get("fetched_catalog", {}).get("dataset", [])
            

            # Creates a new, combined record for each individual dataset
            for resource in resources:
                flattened_list.append({
                    "workflow": workflow_record,  # The harvest-record row for the parent hub
                    "hub_defaults": hub_defaults,  # The metadata defaults row for the parent hub
                    "resource": resource      # The record for each dataset
                })

        return flattened_list
    
    def build_dataframe(self, flattened_items):
        # Convert flattened records into a dataframe and apply ArcGIS-specific schema mapping steps.

        df = pd.DataFrame(flattened_items)

        df = (
            df.pipe(self.arcgis_filter_rows)
            .pipe(self.arcgis_map_to_schema)
            .pipe(self.arcgis_extract_distributions) 
        )

        return df

    def derive_fields(self, df):
        # Populate derived metadata fields after the base harvester has applied shared derivations.
        df = super().derive_fields(df)
        df = (
            df.pipe(self.arcgis_parse_identifiers)
            .pipe(self.arcgis_temporal_coverage)
            .pipe(self.arcgis_format_date_ranges)
            .pipe(self.arcgis_compute_bbox_column)
            .pipe(self.arcgis_clean_creator_values)
            .pipe(self.arcgis_reformat_titles)
            .pipe(self.arcgis_set_resource_type)
        )

        return df

    def add_defaults(self, df):
        # Set ArcGIS-specific default values for fields that are constant across harvested records.
        df = super().add_defaults(df)

        df['Display Note'] = "Tip: Check “Visit Source” link for download options."
        df['Language'] = 'eng'
        df['Resource Class'] = 'Web services'
        return df
    
    def add_provenance(self, df: pd.DataFrame) -> pd.DataFrame:
        # Add harvest provenance details and append hub status rows back into the output dataframe.
        # ---------- inherited defaults ----------
        df = super().add_provenance(df)

        today = time.strftime("%Y-%m-%d")

        # ---------- provenance fields for harvested dataset rows ----------
        df["Website Platform"] = "ArcGIS Hub"
        if "workflow_accrual_method" in df.columns:
            df["Accrual Method"] = df["workflow_accrual_method"].map(
                lambda value: self._first_non_empty(value, "Automated retrieval")
            )
        else:
            df["Accrual Method"] = "Automated retrieval"

        if "workflow_accrual_periodicity" in df.columns:
            df["Accrual Periodicity"] = df["workflow_accrual_periodicity"].fillna("")

        if "workflow_harvest_workflow" in df.columns:
            df["Harvest Workflow"] = df["workflow_harvest_workflow"].map(
                lambda value: self._first_non_empty(value, "py_arcgis_hub")
            )
        else:
            df["Harvest Workflow"] = "py_arcgis_hub"

        if "workflow_endpoint_description" in df.columns:
            df["Endpoint Description"] = df["workflow_endpoint_description"].map(
                lambda value: self._first_non_empty(value, "DCAT API")
            )
        else:
            df["Endpoint Description"] = "DCAT API"

        df["Provenance"] = df.apply(
            lambda row: (
                f"The metadata for this resource was last retrieved from "
                f"{row.get('Publisher', ' ArcGIS Hub')} on {today}."
            ),
            axis=1,
        )

        # ---------- rebuild hub list and evaluate indexing status ----------
        indexed_ids = {
            str(value).strip()
            for value in df["Is Part Of"].astype(str)
            if str(value).strip()
        }
        hub_df = self._build_parent_hub_rows(indexed_ids, today)

        if not hub_df.empty:
            df = pd.concat([df, hub_df], ignore_index=True)

            print(f"[ArcGIS] Updated Status for {len(hub_df)} hub records and "
                f"appended them to the harvested metadata dataframe.")
        else:
            print("[ArcGIS] Workflow input CSV not found or produced no hub rows.")

        return df



    def clean(self, df):
        # Run shared cleanup logic without adding ArcGIS-specific post-processing yet.
        df = super().clean(df)
        return df

    def validate(self, df):
        # Run shared validation rules before writing ArcGIS output files.
        df = super().validate(df)
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        # Generate the secondary distributions table and delegate final file writing to the base harvester.
        distributions_df = generate_secondary_table(primary_df.copy(), self.distribution_types)
        return super().write_outputs(primary_df, distributions_df)

# --- ArcGIS-Specific Functions --- #

    def arcgis_filter_rows(self, df):
        # Keep only dataset rows that expose the distribution titles or service URLs this harvester supports.

        ALLOWED_TITLES = {'Shapefile'}
        ACCESS_PATTERNS = ['ImageServer']  # extend if needed, e.g., 'FeatureServer', 'MapServer'

        def is_valid(row):
            # Check whether a resource has a usable title and at least one supported distribution entry.
            resource = row['resource']
            # CORRECTED: Cast to string before stripping to avoid AttributeError
            title = str(resource.get('title', '')).strip()
            if not title or title.startswith('{{'):
                return False

            dists = resource.get('distribution', []) or []
            if not isinstance(dists, list):
                return False

            has_valid_title = any((dist.get('title') in ALLOWED_TITLES) for dist in dists)
            has_valid_url = any(
                any(pat in str(dist.get('accessURL', '')) for pat in ACCESS_PATTERNS)
                for dist in dists
            )
            return has_valid_title or has_valid_url

        return df[df.apply(is_valid, axis=1)].reset_index(drop=True)

    def arcgis_map_to_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        # Translate website- and dataset-level ArcGIS fields into the normalized output schema.
        workflow_records = df["workflow"] if "workflow" in df.columns else df["website"]
        hub_defaults = df["hub_defaults"] if "hub_defaults" in df.columns else df["website"]

        def get_hub_title(hub_row):
            # Normalize the parent hub title before mapping it onto child rows.
            return self._normalize_workflow_title(hub_row.get('Title', ''))

        def get_creator(resource):
            # Normalize publisher data into a single creator string.
            pub = resource.get('publisher')
            if isinstance(pub, dict):
                return pub.get('name') or next(iter(pub.values()), '')
            return pub or ''

        def get_first_spatial(website):
            # Pull the first usable place label from the hub's spatial coverage field.
            spatial = website.get('Spatial Coverage', '')
            if isinstance(spatial, list):
                for val in spatial:
                    if isinstance(val, str) and val.strip():
                        return val.strip()
                return ''
            # Spatial Coverage is stored as a pipe-delimited string; grab the first entry.
            return str(spatial).split('|')[0].strip()

        output_data = {
            # --- Map Hub fields directly to Final Schema ---
            'Is Part Of':       hub_defaults.apply(
                lambda h: self._first_non_empty(h.get('ID', ''), h.get('Code', ''))
            ),
            'Code':             hub_defaults.apply(
                lambda h: self._first_non_empty(h.get('Code', ''), h.get('ID', ''))
            ),
            'Publisher':        hub_defaults.apply(get_hub_title),
            'Endpoint URL':     workflow_records.apply(lambda h: h.get('Endpoint URL', '')),
            'Spatial Coverage': hub_defaults.apply(lambda h: h.get('Spatial Coverage', '')),
            'default_bbox':     hub_defaults.apply(lambda h: h.get('Bounding Box', '')),
            'Member Of':        hub_defaults.apply(lambda h: h.get('Member Of', '')),
            'titlePlace':       hub_defaults.apply(get_first_spatial),

            # --- Map Dataset fields directly to Final Schema ---
            'Alternative Title': df['resource'].apply(lambda d: str(d.get('title', '')).strip()),
            'Description':       df['resource'].apply(lambda d: d.get('description', '')),
            'Creator':           df['resource'].apply(get_creator),
            'Keyword':           df['resource'].apply(lambda d: '|'.join(k.strip() for k in d.get('keyword', []) if isinstance(k, str)).replace(' ', '')),
            'Date Issued':       df['resource'].apply(lambda d: str(d.get('issued', '')).split('T')[0]),
            'Date Modified':     df['resource'].apply(lambda d: str(d.get('modified', '')).split('T')[0]),
            'Rights':            df['resource'].apply(lambda d: d.get('license', '')),
            'identifier_raw':    df['resource'].apply(lambda d: d.get('identifier', '')),
            'information':       df['resource'].apply(lambda d: d.get('landingPage', '')),

            # --- Create Pass-through columns for the next steps in the pipeline ---
            'spatial':           df['resource'].apply(lambda d: d.get('spatial', '')),
            'distributions':     df['resource'].apply(lambda d: d.get('distribution', []) or []),
            'workflow_endpoint_description': workflow_records.apply(
                lambda h: h.get('Endpoint Description', '')
            ),
            'workflow_accrual_method': workflow_records.apply(
                lambda h: h.get('Accrual Method', '')
            ),
            'workflow_accrual_periodicity': workflow_records.apply(
                lambda h: h.get('Accrual Periodicity', '')
            ),
            'workflow_harvest_workflow': workflow_records.apply(
                lambda h: h.get('Harvest Workflow', '')
            ),
        }
        
        return pd.DataFrame(output_data)

    def arcgis_extract_distributions(self, df):
        # Split distribution links into dedicated service columns and assign a matching format label.
        """
        Sorts webs service links
        """

        def derive_dist_fields(dists):
            # Inspect distribution entries and capture the first supported ArcGIS service URL of each type.
            out = {
                'featureService': '',
                'mapService': '',
                'imageService': '',
                'tileService': '',
                'Format': '',
            }
            # Ensure 'dists' is a list before iterating
            if not isinstance(dists, list):
                dists = []

            for dist in dists:
                title = str(dist.get('title', ''))
                access_url = str(dist.get('accessURL', ''))
                if title == 'ArcGIS GeoService' and access_url:
                    if 'FeatureServer' in access_url:
                        out['featureService'] = access_url
                        out['Format'] = 'ArcGIS FeatureLayer'
                    elif 'MapServer' in access_url:
                        out['mapService'] = access_url
                        out['Format'] = 'ArcGIS DynamicMapLayer'
                    elif 'ImageServer' in access_url:
                        out['imageService'] = access_url
                        out['Format'] = 'ArcGIS ImageMapLayer'
                    elif 'TileServer' in access_url:
                        out['tileService'] = access_url
                        out['Format'] = 'ArcGIS TiledMapLayer'
            return pd.Series(out)

        dist_df = df['distributions'].apply(derive_dist_fields)
        # merge columns into df (aligned by index)
        df = pd.concat([df, dist_df], axis=1)

        return df


    def arcgis_compute_bbox_column(self, df):
        # Build a normalized bounding box from dataset spatial values, falling back to the hub default when needed.
        """
        Populate 'Bounding Box' using 'spatial' if it has 4 comma-separated numbers
        and forms a non-degenerate box (xmin != xmax and ymin != ymax).
        Otherwise, use 'default_bbox'.
        """
        def _bbox(r):
            # Validate and normalize a single row's bbox string before writing the final field.
            sp = r.get('spatial', None)
            fallback = r.get('default_bbox', '')

            def use_fallback():
                # Return the cleaned fallback bbox when the dataset-level geometry is missing or invalid.
                fb = '' if pd.isna(fallback) else str(fallback).strip()
                return fb

            if isinstance(sp, str):
                parts = [p.strip() for p in sp.split(',')]
                if len(parts) == 4:
                    try:
                        xmin, ymin, xmax, ymax = [float(p) for p in parts]

                        # Normalize if reversed
                        if xmin > xmax: xmin, xmax = xmax, xmin
                        if ymin > ymax: ymin, ymax = ymax, ymin

                        # Degenerate → line/point → use fallback
                        if xmin == xmax or ymin == ymax:
                            return use_fallback()

                        # Valid polygon bbox
                        return f"{xmin},{ymin},{xmax},{ymax}"
                    except ValueError:
                        pass

            # Not a valid 4-number bbox → use fallback
            return use_fallback()

        df['Bounding Box'] = df.apply(_bbox, axis=1)
        return df


    def arcgis_harvest_identifier_and_id(self, identifier: str) -> tuple:
        # Derive a stable dataset URL and output ID from an ArcGIS identifier query string.
        parsed = urlparse(identifier)
        qs = parse_qs(parsed.query)

        if 'id' in qs:
            resource_id = qs['id'][0]

            # Append sublayer number if present
            if 'sublayer' in qs:
                resource_id = f"{resource_id}_{qs['sublayer'][0]}"

            cleaned = f"https://hub.arcgis.com/datasets/{resource_id}"
            return cleaned, resource_id

        return identifier, identifier


    def arcgis_parse_identifiers(self, df):
        # Expand raw identifier values into normalized Identifier and ID columns.
        ids = df['identifier_raw'].apply(self.arcgis_harvest_identifier_and_id)
        df[['Identifier', 'ID']] = pd.DataFrame(ids.tolist(), index=df.index)
        return df

    def arcgis_temporal_coverage(self, df):
        # Infer temporal coverage from resource titles using the shared temporal parser.
        """
        Adds a 'Temporal Coverage' column based on Title or Date Modified.
        """
        df["Temporal Coverage"] = df.apply(infer_temporal_coverage_from_title, axis=1)
        return df
    
    def arcgis_format_date_ranges(self, df):
        # Convert temporal coverage and date fields into a normalized date range string.
        """
        Adds a 'Date Range' column based on 'Temporal Coverage', 'Date Modified', or 'Date Issued'.
        """
        df["Date Range"] = df.apply(
            lambda row: create_date_range(row, row.get("Temporal Coverage", "")),
            axis=1
        )
        return df

    def arcgis_reformat_titles(self, df):
        # Compose the final display title by combining the dataset title with its place label.
        """
        Updates the Title field by concatenating 'Alternative Title' and 'titlePlace',
        with the titlePlace in square brackets. 
        """
        df['Title'] = df.apply(
            lambda row: f"{row['Alternative Title']} [{row['titlePlace']}]"
            if pd.notna(row['Alternative Title']) and pd.notna(row['titlePlace'])
            else row['Alternative Title'] if pd.notna(row['Alternative Title'])
            else f"[{row['titlePlace']}]" if pd.notna(row['titlePlace'])
            else "",
            axis=1
        )
        return df

    def arcgis_clean_creator_values(self, df):
        # Normalize creator values that may arrive as dicts or stringified dict payloads.
        def _clean(value):
            # Extract the readable creator name from the current value shape.
            if isinstance(value, dict) and 'name' in value:
                return value['name']
            elif isinstance(value, str):
                match = re.match(r"\\{\\s*'name'\\s*:\\s*'(.+?)'\\s*\\}", value)
                if match:
                    return match.group(1)
                return value
            return value
    
        df['Creator'] = df['Creator'].apply(_clean)
        return df
    
    def arcgis_set_resource_type(self, df):
        # Override resource types when ArcGIS metadata contains specific keyword-driven matches.
        """
        Assign values to 'Resource Type' based on keyword matches found in Title, Description, or Keyword.
        Existing values are preserved unless a new match is found.
        """
        keyword_map = {
            'lidar': 'LiDAR',
            'polygon': 'Polygon data'
        }

        def match_keywords(row):
            # Search combined text fields for resource-type keywords and return the best normalized match.
            combined_text = f"{row.get('Alternative Title', '')} {row.get('Description', '')} {row.get('Keyword', '')}".lower()
            for keyword, resource_type in keyword_map.items():
                if keyword in combined_text:
                    return match_resource_type(resource_type)
            return match_resource_type(row.get('Resource Type', ''))  # Keep existing value if no match

        df['Resource Type'] = df.apply(match_keywords, axis=1)
        return df
