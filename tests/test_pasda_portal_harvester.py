from pathlib import Path

from harvesters.base import BaseHarvester
from harvesters.pasda_portal import PasdaPortalHarvester
from routers.jobs import HARVESTER_REGISTRY


def _config(tmp_path: Path) -> dict:
    input_html = tmp_path / "pasda-search.html"
    input_html.write_text(
        """
        <table>
          <tr>
            <td>2024</td>
            <td>2024-01-01</td>
            <td><h3><a href="DataSummary.aspx?dataset=roads">Roads</a></h3></td>
            <td>Pennsylvania Department of Transportation</td>
            <td><span id="DataGrid1_Label3_0">Road centerlines.</span></td>
            <td><a href="Metadata.aspx?dataset=roads">Metadata</a></td>
          </tr>
        </table>
        """,
        encoding="utf-8",
    )
    return {
        "input_html": str(input_html),
        "output_primary_csv": str(tmp_path / "pasda_primary.csv"),
        "output_distributions_csv": str(tmp_path / "pasda_distributions.csv"),
        "build_uploads": False,
    }


def test_pasda_portal_harvester_keeps_base_method_surface(tmp_path: Path) -> None:
    PasdaPortalHarvester(_config(tmp_path))
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

    portal_methods = {
        name
        for name, value in PasdaPortalHarvester.__dict__.items()
        if callable(value) and not name.startswith("__")
    }
    base_methods = {
        name
        for name, value in BaseHarvester.__dict__.items()
        if callable(value) and not name.startswith("__")
    }

    assert portal_methods <= allowed_methods
    assert portal_methods <= base_methods | {"build_uploads"}


def test_pasda_portal_parse_saved_search_html(tmp_path: Path) -> None:
    harvester = PasdaPortalHarvester(_config(tmp_path))

    parsed = harvester.parse(harvester.fetch())

    assert len(parsed) == 1
    row = parsed.iloc[0]
    assert row["ID"] == "pasda-roads"
    assert row["Alternative Title"] == "Roads"
    assert row["Creator"] == "Pennsylvania Department of Transportation"
    assert row["html"] == "https://www.pasda.psu.edu/uci/Metadata.aspx?dataset=roads"
    assert row["information"] == "https://www.pasda.psu.edu/uci/DataSummary.aspx?dataset=roads"


def test_pasda_portal_registered_as_separate_job_type() -> None:
    assert HARVESTER_REGISTRY["pasda-portal"] is PasdaPortalHarvester
