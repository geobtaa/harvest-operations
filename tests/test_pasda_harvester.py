from harvesters.base import BaseHarvester
from harvesters.pasda import PasdaHarvester


def _config() -> dict:
    return {
        "input_html": "inputs/pasda-search.html",
        "output_primary_csv": "outputs/pasda_primary.csv",
        "output_distributions_csv": "outputs/pasda_distributions.csv",
    }


def test_pasda_harvester_enables_build_uploads_by_default() -> None:
    harvester = PasdaHarvester(_config())

    assert harvester.config["build_uploads"] is True


def test_pasda_harvester_allows_build_uploads_to_be_disabled() -> None:
    config = _config()
    config["build_uploads"] = False

    harvester = PasdaHarvester(config)

    assert harvester.config["build_uploads"] is False


def test_pasda_harvester_keeps_base_method_surface() -> None:
    allowed_methods = {
        "__init__",
        "load_reference_data",
        "fetch",
        "parse",
        "flatten",
        "build_dataframe",
        "derive_fields",
        "add_defaults",
        "add_provenance",
        "clean",
        "validate",
        "write_outputs",
        "build_uploads",
        "harvest_pipeline",
    }

    pasda_methods = {
        name
        for name, value in PasdaHarvester.__dict__.items()
        if callable(value) and not name.startswith("__")
    }
    base_methods = {
        name
        for name, value in BaseHarvester.__dict__.items()
        if callable(value) and not name.startswith("__")
    }

    assert pasda_methods <= allowed_methods
    assert pasda_methods <= base_methods | {"build_uploads"}
