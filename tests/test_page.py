"""The pages, Connection and Datasets, under Manage alone, each the same answer
as its read tool (`read_connection`, `read_datasets`): asked in-process with
the SDK's PageClient as the sidecar would ask them. Neither shows a price or
the key."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

import pytest
from meridian.suites import Recorder
from meridian.testing import PageClient

from databento.lake import LIVE_KINDS, TRADES, Standing
from databento.page import CONNECTION, DATASETS, State, figures, pages, use
from databento.settings import END_OF_DAY, REAL_TIME, Config, Credentials
from databento.stream import Streamer

from conftest import RECORDS, SPY, Clock, answer_calendar, answer_records, at, synthetic_lake

KEY = Credentials("db-PageTest000000000000000000XYZ")


def state(tmp: Path, config: Config) -> State:
    recorder = Recorder("databento-1")
    answer_records(recorder, RECORDS)
    answer_calendar(recorder)
    lake = synthetic_lake(
        recorder,
        Clock(at("2026-10-12", 9)),
        tmp,
        stocks=config.timeliness("stocks"),
        options=config.timeliness("options"),
    )
    asyncio.run(lake.daily([SPY], dt.date(2026, 10, 8), dt.date(2026, 10, 8)))
    lake.standing["WNT-1"] = Standing("WNT-1", (SPY,), LIVE_KINDS, at("2026-10-12", 9))
    lake.standing["WNT-2"] = Standing(
        "WNT-2", (SPY,), (), 0, dataset=TRADES, data_type="meridian.v1.Trade"
    )
    lake.restream()

    async def nothing(*_: object) -> None:
        pass

    stocks = Streamer(lambda: None, nothing, nothing)
    stocks.session.state, stocks.session.subscribed = "streaming", 1
    options = Streamer(lambda: None, nothing, nothing)
    assert lake.source is not None
    return State(
        config=config,
        lake=lake,
        source=lake.source,  # type: ignore[arg-type]
        historical=lake.source.historical,  # type: ignore[attr-defined]
        stocks=stocks,
        options=options,
    )


@pytest.fixture
def client(tmp_path: Path) -> PageClient:
    use(state(tmp_path, Config(sample_data=True)))
    return PageClient(pages)


def test_each_page_serves_manage_alone(client: PageClient) -> None:
    for path in (CONNECTION, DATASETS):
        assert client.get(path, "admin").status == 200
        assert client.get(path, "write").status == 403
        assert client.get(path, "read").status == 403


def test_the_connection_page_and_its_tool_read_one_source(client: PageClient) -> None:
    page = client.get(CONNECTION, "admin").text
    tabs = ("?tab=key", "?tab=limits-and-use", "?tab=stream")
    assert [page.index(t) for t in tabs] == sorted(page.index(t) for t in tabs)
    assert "<h2>Key</h2>" in page and "<h2>Stream</h2>" not in page
    assert "Stocks plan" in page and "Options plan" in page
    limits = client.get(CONNECTION, "admin", tab="limits-and-use").text
    assert "600 historical calls a minute" in limits and "Records returned" in limits
    stream = client.get(CONNECTION, "admin", tab="stream").text
    assert "<h2>Stream</h2>" in stream and "Stocks" in stream and "Options" in stream
    answer = client.call_tool("read_connection")
    assert answer.status == 200 and answer.outcome == "unchanged"
    data = answer.data
    assert data["mode"] == "Sample data" and data["sessions"] == 1
    assert [c["label"] for c in data["credentials"]] == ["API key"]
    assert data["stocks_plan"] == "Real time" and data["records"] >= 1
    assert [s["asset_class"] for s in data["streams"]] == ["stocks", "options"]


def test_the_datasets_page_and_its_tool_read_one_source(client: PageClient) -> None:
    page = client.get(DATASETS, "admin").text
    assert "<h2>Served</h2>" in page and "databento-1:daily" in page
    assert "671.42" not in page and "Nasdaq NLS Plus" in page and "OPRA" in page
    data = client.call_tool("read_datasets").data
    by_key = {s["key"]: s for s in data["served"]}
    assert list(by_key) == ["daily", "live", "trades", "options"]
    assert by_key["daily"]["recorded"] >= 1 and by_key["daily"]["entitled_at_vendor"] == "Yes"
    assert by_key["options"]["feed"] == "OPRA.PILLAR" and by_key["live"]["feed"] == "EQUS.MINI"
    assert data["backfills_a_restart"] is True and data["listings"] == 1
    assert {w["want_id"] for w in data["wants"]} == {"WNT-1", "WNT-2"}


def test_a_key_is_never_shown(tmp_path: Path) -> None:
    use(
        state(
            tmp_path,
            Config(credentials=KEY, token_set=True, stocks=END_OF_DAY, options=REAL_TIME),
        )
    )
    client = PageClient(pages)
    for path in (CONNECTION, DATASETS):
        for tab in ("key", "limits-and-use", "stream"):
            assert KEY.key not in client.get(path, "admin", tab=tab).text
    data = client.call_tool("read_connection").data
    assert KEY.key not in str(data) and data["credentials"][0]["set"] is True
    assert data["stocks_plan"] == "Historical only" and data["options_plan"] == "Real time"
    served = {s["key"]: s for s in client.call_tool("read_datasets").data["served"]}
    assert served["live"]["entitled_at_vendor"] == "No: the Stocks plan is historical only"


def test_the_figures_are_the_roles(tmp_path: Path) -> None:
    held = state(tmp_path, Config(sample_data=True))
    named = [f.label for f in figures(held)]
    assert named[:5] == [
        "Connection",
        "Entitled at the vendor",
        "Stream",
        "Last recorded",
        "Rate use",
    ]
