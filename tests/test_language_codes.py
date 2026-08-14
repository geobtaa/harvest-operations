from utils.language_codes import convert_languages_to_iso, load_language_mapping


def test_convert_languages_to_iso_uses_shared_vocabulary():
    language_mapping = load_language_mapping()

    assert convert_languages_to_iso(
        ["English", "Greek, Modern (1453-)", "Latin"],
        language_mapping,
    ) == "eng|gre|lat"


def test_convert_languages_to_iso_handles_pipe_values_and_unknown_languages():
    assert convert_languages_to_iso("French|Unmapped language|French") == (
        "fre|Unmapped language"
    )


def test_convert_languages_to_iso_accepts_an_existing_case_sensitive_mapping():
    assert convert_languages_to_iso("Example Language", {"Example Language": "exa"}) == (
        "exa"
    )
