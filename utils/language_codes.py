import csv
from functools import lru_cache
from pathlib import Path


DEFAULT_LANGUAGE_VOCABULARY_CSV = (
    Path(__file__).resolve().parents[1]
    / "reference_data"
    / "language-vocabulary.csv"
)


@lru_cache(maxsize=None)
def load_language_mapping(csv_path=None):
    """Load a case-insensitive language-name to ISO 639-2 code mapping."""
    path = Path(csv_path) if csv_path else DEFAULT_LANGUAGE_VOCABULARY_CSV
    if not path.is_absolute() and not path.exists():
        path = Path(__file__).resolve().parents[1] / path

    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return {
            row["LanguageName"].strip().casefold(): row["ISOCode"].strip()
            for row in reader
            if row.get("LanguageName", "").strip()
            and row.get("ISOCode", "").strip()
        }


def convert_languages_to_iso(languages, language_mapping=None, separator="|"):
    """
    Convert language names in a scalar, pipe-delimited string, or iterable to
    pipe-delimited ISO 639-2 codes while retaining unknown values.
    """
    mapping = language_mapping or load_language_mapping()
    converted = []
    seen = set()

    for language in language_values(languages, separator=separator):
        code = mapping.get(language.casefold(), mapping.get(language, language))
        if not code or code in seen:
            continue
        seen.add(code)
        converted.append(code)

    return separator.join(converted)


def language_values(value, separator="|"):
    """Flatten supported language values into a clean list of labels or codes."""
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        values = []
        for item in value:
            values.extend(language_values(item, separator=separator))
        return values
    if isinstance(value, float) and value != value:
        return []

    return [
        part.strip()
        for part in str(value).split(separator)
        if part.strip()
    ]
