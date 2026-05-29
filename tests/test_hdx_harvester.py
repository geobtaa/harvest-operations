from harvesters.hdx import HdxHarvester


def _config() -> dict:
    return {
        "input_json": "inputs/hdx_geodata.json",
        "output_primary_csv": "outputs/hdx_primary.csv",
        "output_distributions_csv": "outputs/hdx_distributions.csv",
    }


def test_hdx_harvester_enables_build_uploads_by_default() -> None:
    harvester = HdxHarvester(_config())
    assert harvester.config["build_uploads"] is True


def test_hdx_harvester_allows_build_uploads_to_be_disabled() -> None:
    config = _config()
    config["build_uploads"] = False

    harvester = HdxHarvester(config)
    assert harvester.config["build_uploads"] is False
