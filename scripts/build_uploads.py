#!/usr/bin/env python3
import sys
import re
from datetime import date
from pathlib import Path
import pandas as pd

# =========================
# Configure your harvester:
# =========================
# e.g., "arcgis", "pasda", "ogm", etc.
SOURCE = "arcgis"

# ---- Paths ----
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = SCRIPT_DIR.parent / "outputs"

# Filename patterns (accept yyyy-mm_dd OR yyyy-mm-dd)
FNAME_RE_PRIMARY = re.compile(fr"^(\d{{4}}-\d{{2}}[-_]\d{{2}})_{SOURCE}_primary\.csv$")
FNAME_RE_DIST    = re.compile(fr"^(\d{{4}}-\d{{2}}[-_]\d{{2}})_{SOURCE}_distributions\.csv$")

def load_csv_norm(path: Path) -> pd.DataFrame:
    """Load a CSV/TSV as strings, normalize whitespace and ID, drop blank/dup IDs."""
    # Try comma first, fall back to tab if we don't get an 'ID' column
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    if "ID" not in df.columns:
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False, sep="\t").fillna("")
        except Exception:
            pass

    if "ID" not in df.columns:
        raise SystemExit(f"Missing 'ID' column in {path.name} (tried comma and tab).")

    # Strip whitespace in all string cells
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].str.strip()

    df["ID"] = df["ID"].astype(str).str.strip()
    df = df[df["ID"] != ""].copy()
    df = df.drop_duplicates(subset=["ID"], keep="first")
    return df

# ---- Discover input primary files ----
candidates = []
for p in OUTPUTS_DIR.iterdir():
    if not p.is_file():
        continue
    m = FNAME_RE_PRIMARY.match(p.name)
    if m:
        iso = m.group(1).replace("_", "-")
        try:
            d = date.fromisoformat(iso)
            candidates.append((d, p))
        except ValueError:
            continue

if len(candidates) < 2:
    found = ", ".join(p.name for _, p in candidates) or "(none)"
    raise SystemExit(
        f"Need at least two matching primary files to compare.\n"
        f"Looked in: {OUTPUTS_DIR}\n"
        f"Matched pattern: {FNAME_RE_PRIMARY.pattern}\n"
        f"Found: {found}"
    )

# Sort oldest -> newest
candidates.sort(key=lambda t: t[0])

# Most recent file -> new_df
new_date, new_path = candidates[-1]
# SECOND MOST RECENT (previous) -> old_df
old_date, old_path = candidates[-2]

if new_path == old_path:
    raise SystemExit("Internal error: new and old paths resolved to the same file.")

print(f"Newest primary:         {new_path.name}")
print(f"Second-most-recent:     {old_path.name}")

# ---- Load & normalize primaries ----
new_df = load_csv_norm(new_path)
old_df = load_csv_norm(old_path)

# ---- Build output df ----
parts = []

# (A) Always include "Websites" from newest file
if "Resource Class" in new_df.columns:
    websites = new_df[new_df["Resource Class"].str.lower() == "websites"].copy()
    if not websites.empty:
        parts.append(websites)

# (B) Rows present in new_df but NOT in old_df (new additions)
new_only = new_df.merge(
    old_df[["ID"]].drop_duplicates(),
    on="ID",
    how="left",
    indicator=True
)
new_only = new_only[new_only["_merge"] == "left_only"].drop(columns=["_merge"])
print(f"New additions: {len(new_only)}")
if not new_only.empty:
    parts.append(new_only)

# (C) Rows present in old_df but NOT in new_df (deleted -> retire)
old_only = old_df.merge(
    new_df[["ID"]].drop_duplicates(),
    on="ID",
    how="left",
    indicator=True
)
old_only = old_only[old_only["_merge"] == "left_only"].drop(columns=["_merge"]).copy()
print(f"To retire:     {len(old_only)}")

if not old_only.empty:
    if "Publication State" not in old_only.columns:
        old_only["Publication State"] = ""
    if "Date Retired" not in old_only.columns:
        old_only["Date Retired"] = ""
    old_only["Publication State"] = "unpublished"
    old_only["Date Retired"] = date.today().isoformat()
    parts.append(old_only)

# Combine and de-dup by ID (keep first occurrence: Websites/new additions/retirements in that order)
if parts:
    df = pd.concat(parts, ignore_index=True)
    if "ID" in df.columns:
        df = df.drop_duplicates(subset=["ID"], keep="first")
else:
    df = pd.DataFrame(columns=new_df.columns)

# ---- Write primary upload CSV ----
today_str = date.today().isoformat()
primary_out_path = OUTPUTS_DIR / f"{today_str}_{SOURCE}_primary_upload.csv"
df.to_csv(primary_out_path, index=False, encoding="utf-8")
print(f"Wrote {len(df)} rows to {primary_out_path}")

# =========================
# Distributions step (ID-only)
# =========================

# Find distributions CSV whose date matches *new_date*
dist_candidates = []
for p in OUTPUTS_DIR.iterdir():
    if not p.is_file():
        continue
    m = FNAME_RE_DIST.match(p.name)
    if m:
        iso = m.group(1).replace("_", "-")
        try:
            d = date.fromisoformat(iso)
            dist_candidates.append((d, p))
        except ValueError:
            continue

if not dist_candidates:
    raise SystemExit(f"No {SOURCE} distributions CSVs found in {OUTPUTS_DIR}")

# Find the one with the exact same date as newest primary
dist_path = None
for d, p in dist_candidates:
    if d == new_date:
        dist_path = p
        break

if dist_path is None:
    found_names = ", ".join(sorted(p.name for _, p in dist_candidates)) or "(none)"
    raise SystemExit(
        f"No distributions CSV found for date {new_date.isoformat()}.\n"
        f"Expected: {new_date.isoformat()}_{SOURCE}_distributions.csv (hyphen or underscore ok)\n"
        f"Found distributions files: {found_names}"
    )

print(f"Matched distributions:   {dist_path.name}")

# Load distributions
new_dist = pd.read_csv(dist_path, dtype=str, keep_default_na=False).fillna("")
if "friendlier_id" not in new_dist.columns:
    raise SystemExit(f"'friendlier_id' column not found in {dist_path.name}")

# Normalize friendlier_id
new_dist["friendlier_id"] = new_dist["friendlier_id"].astype(str).str.strip()
new_dist = new_dist[new_dist["friendlier_id"] != ""].copy()

# Filter df to published only
df_pub = df.copy()
if "Publication State" in df_pub.columns:
    df_pub = df_pub[df_pub["Publication State"].str.lower() == "published"].copy()

# Match ONLY by ID
if "ID" not in df_pub.columns:
    raise SystemExit("Output dataframe 'df' has no 'ID' column to match distributions against.")

pub_ids = set(df_pub["ID"].astype(str).str.strip())
filtered_dist = new_dist[new_dist["friendlier_id"].isin(pub_ids)].copy()
print(f"Distributions kept: {len(filtered_dist)} (matched on df['ID'])")

# ---- Write distributions upload CSV ----
dist_out_path = OUTPUTS_DIR / f"{today_str}_{SOURCE}_distributions_upload.csv"
filtered_dist.to_csv(dist_out_path, index=False, encoding="utf-8")
print(f"Wrote {len(filtered_dist)} rows to {dist_out_path}")
