"""The process's choices from its settings (`__main__`): the clients by key
or sample data, each asset class's live gateway by plan, and the pages on
sample data (preview.py)."""

from __future__ import annotations

from pathlib import Path

import pytest
from meridian.edge import RawStore

from databento.__main__ import feed_for, source_for
from databento.page import State
from databento.preview import PAGES, main
from databento.settings import config_from

from conftest import KEY


def state(**values: object) -> State:
    return State(config=config_from(values))


def test_no_key_no_client_no_stream(tmp_path: Path) -> None:
    held = state()
    store = RawStore(tmp_path, kind="responses")
    assert source_for(held, store) == (None, None)
    assert feed_for(held, store, "stocks") is None


def test_each_plan_streams_its_own(tmp_path: Path) -> None:
    held = state(
        databento_api_key=KEY, stocks_timeliness="end_of_day", options_timeliness="real_time"
    )
    store = RawStore(tmp_path, kind="responses")
    historical, source = source_for(held, store)
    assert historical is not None and source is not None
    assert source.gateway == {"stocks": False, "options": True}
    assert feed_for(held, store, "stocks") is None
    options = feed_for(held, store, "options")
    assert options is not None and options.host == "opra-pillar.lsg.databento.com"
    assert options.schemas == ("trades",)
    real = feed_for(state(databento_api_key=KEY, stocks_timeliness="real_time"), store, "stocks")
    assert real is not None and real.schemas == ("trades", "bbo-1s")
    assert real.host == "equs-mini.lsg.databento.com"


def test_an_unset_plan_is_historical_only() -> None:
    config = config_from({"stocks_timeliness": "real_time"}, unset=["stocks_timeliness"])
    assert config.stocks == "end_of_day" and config.options == "end_of_day"
    assert config_from({"stocks_timeliness": "delayed"}).stocks == "end_of_day"
    assert config_from({"sample_data": True}).timeliness("options") == "real_time"


@pytest.mark.parametrize("page", sorted(PAGES))
def test_each_preview_page_renders(page: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["preview", page]) == 0
    shown = capsys.readouterr().out
    assert "<h1>Databento</h1>" in shown and KEY not in shown
