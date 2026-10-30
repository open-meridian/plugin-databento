"""The version's declaration as the upload reads it (contract v25): printed
from the plugin's root, as `meridian-declaration` prints it in the built
image, it carries the catalogue, the settings (the key secret), the storage,
the interface -- its pages and the tools their routes make -- and each
section of docs/ by its digest and size."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from meridian.declaration import main

from databento import declaration as version
from databento.declared import CATALOGUE
from databento.lake import US_EQUITY_CALENDAR
from databento.settings import SETTINGS

ROOT = Path(__file__).resolve().parents[1]
NAMED = "databento.declaration:DECLARATION"
SECTIONS = ["overview", "setup", "troubleshooting", "terms", "pages"]


def printed(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> dict:  # type: ignore[type-arg]
    monkeypatch.chdir(ROOT)
    assert main([NAMED]) == 0
    declared = json.loads(capsys.readouterr().out)
    assert isinstance(declared, dict)
    return declared


def test_the_declaration_carries_its_pages_tools_and_documentation(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    declared = printed(monkeypatch, capsys)
    assert [page["title"] for page in declared["interface"]["pages"]] == [
        "Connection",
        "Datasets",
    ]
    # The pages' read tools, and the storage's restore (the SDK's).
    assert {tool["name"] for tool in declared["tools"]} == {
        "read_connection",
        "read_datasets",
        "restore_unit",
    }
    sections = declared["documentation"]
    assert sorted(section["name"] for section in sections) == sorted(SECTIONS)
    for section in sections:
        held = (ROOT / "docs" / f"{section['name']}.md").read_bytes()
        assert section["digest"] == f"sha256:{hashlib.sha256(held).hexdigest()}"
        assert section["bytes"] == len(held)
    assert version.PORT == 8000


def test_the_catalogue_one_dataset_a_kind() -> None:
    by_key = {d.key: d for d in CATALOGUE}
    assert list(by_key) == ["daily", "live", "trades", "options"]
    for dataset in by_key.values():
        # Databento sells no delayed feed: no dataset declares a delay.
        assert dataset.declared_delay_ns == 0
        assert dataset.aggregator == "Databento"
        assert dataset.day_time_zone == "America/New_York" and dataset.day_end_minute == 960
    assert by_key["options"].vendor == "OPRA"
    assert by_key["daily"].vendor == "Nasdaq NLS Plus (consolidated)"
    assert by_key["live"].vendor == by_key["trades"].vendor
    assert "meridian.v1.Quote" in by_key["options"].data_types
    assert {by_key[k].calendar_id for k in ("daily", "options")} == {US_EQUITY_CALENDAR}


def test_each_datasets_default_terms() -> None:
    for dataset in CATALOGUE:
        licence = dataset.licence_default
        assert licence is not None and licence.display
        assert licence.derived_use_terms == "internal_only"
        assert licence.redistribution == "not_allowed"
        assert "not affiliated" in licence.not_affiliated_notice
        # Not declared until core's declaration parser carries it.
        assert licence.eligibility is None
    terms = {
        d.key: (d.licence_default.kept, d.licence_default.retention_days,  # type: ignore[union-attr]
                d.licence_default.personal_use)  # type: ignore[union-attr]
        for d in CATALOGUE
    }  # fmt: skip
    assert terms == {
        "daily": (True, 0, False),
        "live": (False, 0, False),
        "trades": (True, 2, False),
        "options": (True, 0, True),
    }


def test_the_settings_in_the_data_roles_order() -> None:
    assert [s.name for s in SETTINGS] == [
        "databento_api_key",
        "stocks_timeliness",
        "options_timeliness",
        "sample_data",
    ]
    key, stocks, options, sample = SETTINGS
    assert key.secret and key.required
    assert stocks.default == options.default == "end_of_day"
    assert [c.value for c in stocks.choices] == ["real_time", "end_of_day"]
    assert sample.developer
