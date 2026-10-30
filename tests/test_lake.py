"""The lake's half (lake.py, chains.py) on the SDK's Recorder, against the
synthetic exchange: what each want records or declines, by plan."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Any

from meridian.operations import as_decimal as d
from meridian.plugin.v1 import operations_pb2 as ops
from meridian.suites import Recorder

from plugin_databento.convert import utc_midnight
from plugin_databento.settings import END_OF_DAY

from conftest import (
    BRK,
    ISIN_ONLY,
    RECORDS,
    SPY,
    TQQQ,
    Clock,
    answer_calendar,
    answer_records,
    at,
    calls,
    synthetic_lake,
    want,
)

DAY = "2026-10-08"
AFTER = at("2026-10-12", 9)
#: Friday 2026-10-09, 15:00 New York: the session open.
DURING = at("2026-10-09", 15)


def kind(value: int) -> str:
    return str(ops.PriceKind.Name(value)).removeprefix("PRICE_KIND_").lower()


def statistic_kind(row: Any) -> str:
    field = row.DESCRIPTOR.fields_by_name["kind"]
    if field.enum_type is None:
        return str(row.kind)
    name = str(field.enum_type.values_by_number[row.kind].name)
    return name.split("_KIND_", 1)[-1].lower()


def recorded(recorder: Recorder, name: str, field: str) -> list[Any]:
    return [row for params in calls(recorder, name) for row in getattr(params, field)]


def declined(recorder: Recorder) -> list[tuple[str, int]]:
    return [
        (s.entity_id, params.reason) for params in calls(recorder, "DeclineWant")
        for s in params.subjects
    ]  # fmt: skip


async def ask(lake: Any, recorder: Recorder, event: ops.ObservationsWantedEvent) -> None:
    recorder.answer("ObservationsWanted", lambda _: event)
    await recorder.receive(observations_wanted=lake.on_want)


def made(tmp: Path, now: int = AFTER, **kwargs: Any) -> tuple[Recorder, Any]:
    recorder = Recorder()
    answer_records(recorder, RECORDS)
    answer_calendar(recorder)
    return recorder, synthetic_lake(recorder, Clock(now), tmp, **kwargs)


async def test_a_daily_close_from_the_summary(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path)
    await ask(lake, recorder, want(SPY, business_date=DAY))
    prices = recorded(recorder, "RecordPrices", "prices")
    assert [(d(p.price.amount), p.meta.business_date) for p in prices] == [
        (Decimal("671.420000000"), DAY)
    ]
    assert prices[0].meta.row_key == f"SPY:1d:{DAY}@{SPY}"
    assert prices[0].meta.source.venue_id == ""


async def test_a_bar_has_no_vwap_and_no_trade_count(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path)
    await ask(lake, recorder, want(BRK, data_type="meridian.v1.Bar", business_date=DAY))
    (bar,) = recorded(recorder, "RecordBars", "bars")
    assert d(bar.close.amount) == Decimal("488.170000000") and d(bar.volume) == Decimal(3655812)
    assert not bar.HasField("vwap") and not bar.trade_count


async def test_a_record_with_no_symbol_is_declined(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path)
    await ask(lake, recorder, want(ISIN_ONLY, business_date=DAY))
    assert declined(recorder) == [(ISIN_ONLY, ops.UNANSWERED_REASON_NOT_COVERED)]


async def test_live_prices_from_a_short_replay(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path, DURING)
    await ask(lake, recorder, want(SPY, BRK, dataset="suite-plugin:live"))
    prices = {
        (p.meta.subjects[0].entity_id, kind(p.kind)): d(p.price.amount)
        for p in recorded(recorder, "RecordPrices", "prices")
    }
    assert prices[(SPY, "last")] == Decimal("673.880000000")
    assert prices[(SPY, "bid")] == Decimal("673.870000000")
    assert prices[(SPY, "ask")] == Decimal("673.890000000")
    # BRK.B's offer side is empty: no ask.
    assert (BRK, "ask") not in prices and prices[(BRK, "bid")] == Decimal("490.580000000")


async def test_a_quote_with_its_sizes(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path, DURING)
    await ask(lake, recorder, want(SPY, dataset="suite-plugin:live", data_type="meridian.v1.Quote"))
    (quote,) = recorded(recorder, "RecordQuotes", "quotes")
    assert (d(quote.bid.amount), d(quote.ask.amount)) == (
        Decimal("673.870000000"),
        Decimal("673.890000000"),
    )
    assert (d(quote.bid_quantity), d(quote.ask_quantity)) == (Decimal(300), Decimal(200))


async def test_a_historical_only_plan_declines_live_and_standing_trades(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path, DURING, stocks=END_OF_DAY, options=END_OF_DAY)
    await ask(lake, recorder, want(SPY, dataset="suite-plugin:live"))
    await ask(
        lake,
        recorder,
        want(SPY, dataset="suite-plugin:trades", data_type="meridian.v1.Trade", standing=True),
    )
    await ask(
        lake,
        recorder,
        want(SPY, dataset="suite-plugin:options", data_type="meridian.v1.Quote"),
    )
    assert declined(recorder) == [(SPY, ops.UNANSWERED_REASON_NOT_COVERED)] * 3
    assert not lake.streamed


async def test_a_range_of_trades_from_the_replay_past_the_history(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path, DURING)
    event = want(
        SPY,
        dataset="suite-plugin:trades",
        data_type="meridian.v1.Trade",
        valid_from_ns=DURING - 120 * 10**9,
        valid_until_ns=DURING,
    )
    await ask(lake, recorder, event)
    trades = recorded(recorder, "RecordTrades", "trades")
    assert [d(t.price.amount) for t in trades] == [
        Decimal("673.800000000"), Decimal("673.810000000"), Decimal("673.820000000"),
        Decimal("673.830000000"),
    ]  # fmt: skip
    keys = [t.meta.row_key for t in trades]
    # Two prints stated with the same clocks: two rows, numbered apart.
    assert keys[1] == keys[0].replace(f"@{SPY}", f":1@{SPY}") and len(set(keys)) == 4
    assert all(not t.source_sequence for t in trades)
    # The side that took liquidity, where Databento states it.
    assert [ops.Aggressor.Name(t.aggressor) for t in trades] == [
        "AGGRESSOR_UNSPECIFIED", "AGGRESSOR_BUY", "AGGRESSOR_SELL", "AGGRESSOR_UNSPECIFIED",
    ]  # fmt: skip


async def test_a_range_of_trades_from_the_history(tmp_path: Path) -> None:
    # The history reaches now: the trades come from it, not the gateway.
    clock_now = DURING
    recorder, lake = made(tmp_path, clock_now, reach_end_ns=lambda: clock_now)
    event = want(
        SPY,
        dataset="suite-plugin:trades",
        data_type="meridian.v1.Trade",
        valid_from_ns=DURING - 120 * 10**9,
        valid_until_ns=DURING,
    )
    await ask(lake, recorder, event)
    assert len(recorded(recorder, "RecordTrades", "trades")) == 4
    assert lake.source.replays == 0


def friday(now: int) -> dt.date:
    today = dt.datetime.fromtimestamp(now / 10**9, dt.UTC).date()
    ahead = (4 - today.weekday()) % 7 or 7
    return today + dt.timedelta(days=ahead)


async def test_a_chains_series_never_resolved(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path, DURING)
    await ask(
        lake,
        recorder,
        want(SPY, dataset="suite-plugin:options", data_type="meridian.v1.ListedSeries"),
    )
    series = recorded(recorder, "RecordSeries", "series")
    # Three strikes, a call and a put, two expirations, and the adjusted
    # root: 14; the 680 put's definition states no CFI, so it is not.
    assert len(series) == 14
    assert all(s.meta.subjects[0].entity_id == SPY for s in series)
    assert not calls(recorder, "ResolveIdentifier") or all(
        "occ_osi" not in str(p) for p in calls(recorder, "ResolveIdentifier")
    )
    fields = {f.path: f.value for f in series[0].fields}
    assert fields["exercise_style"] == "EXERCISE_STYLE_AMERICAN"
    assert fields["settlement_method"] == "SETTLEMENT_METHOD_PHYSICAL"
    adjusted = [s for s in series if s.meta.series_key[0].value.startswith("SPY1")]
    assert adjusted and all(
        not any(f.path.startswith("deliverable") for f in s.fields) for s in adjusted
    )
    assert lake.counts["options"].unconverted == 2


async def test_open_interest_dated_by_the_calendar(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path, DURING)
    await ask(
        lake, recorder, want(SPY, dataset="suite-plugin:options", data_type="meridian.v1.Statistic")
    )
    stats = recorded(recorder, "RecordStatistics", "statistics")
    kinds = {statistic_kind(s) for s in stats}
    assert kinds == {"open_interest", "volume"}
    # Published on the history's latest day (Thursday 2026-10-08, UTC),
    # each for the market day before it.
    assert {s.meta.business_date for s in stats} == {"2026-10-07"}
    oi = [s for s in stats if statistic_kind(s) == "open_interest"]
    assert d(oi[0].value.value) in (Decimal(1543), Decimal(2210))


async def test_a_past_dates_open_interest_reads_the_next_market_days(tmp_path: Path) -> None:
    # Tuesday: the history reaches Monday's publication.
    recorder, lake = made(tmp_path, at("2026-10-13", 9))
    event = want(
        SPY, dataset="suite-plugin:options", data_type="meridian.v1.Statistic",
        business_date="2026-10-09",
    )  # fmt: skip
    await ask(lake, recorder, event)
    stats = recorded(recorder, "RecordStatistics", "statistics")
    assert stats and {s.meta.business_date for s in stats} == {"2026-10-09"}
    asked = [
        p for path, p in lake.source.historical.transport.asked if p.get("schema") == "statistics"
    ]
    # Monday 2026-10-12's publication: the market day after Friday.
    assert int(asked[-1]["start"]) == utc_midnight(dt.date(2026, 10, 12))


async def test_a_chains_quotes_from_a_replay(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path, DURING)
    await ask(
        lake, recorder, want(SPY, dataset="suite-plugin:options", data_type="meridian.v1.Quote")
    )
    quotes = recorded(recorder, "RecordQuotes", "quotes")
    assert len(quotes) == 14
    calls_ = [q for q in quotes if q.meta.series_key[0].value[12] == "C"]
    assert d(calls_[0].bid.amount) == Decimal("8.100000000") and d(
        calls_[0].bid_quantity
    ) == Decimal(40)


async def test_option_trades_streamed_for_a_standing_want(tmp_path: Path) -> None:
    from plugin_databento import convert

    recorder, lake = made(tmp_path, DURING)
    first = friday(DURING)
    series = f"SPY   {first:%y%m%d}C00670000"
    event = want(
        SPY, dataset="suite-plugin:options", data_type="meridian.v1.Trade", standing=True,
        series_keys=[series],
    )  # fmt: skip
    await ask(lake, recorder, event)
    assert lake.chains.streamed == [series]
    said = {"ts_recv": str(DURING), "hd": {"ts_event": str(DURING - 1000), "rtype": 0,
            "publisher_id": 22, "instrument_id": 7}, "price": "8.150000000", "size": 3,
            "side": "N"}  # fmt: skip
    stated = convert.trade(series, said, "stream/t#0", convert.Numbering(), osi=series)
    await lake.chains.heard([stated])
    await lake.chains.flush(force=True)
    (trade,) = recorded(recorder, "RecordTrades", "trades")
    assert trade.meta.series_key[0].value == series
    assert [(u.scheme, u.code) for u in trade.meta.unconverted] == [("databento:venue", "XCBO")]


async def test_tqqq_another_sources_record(tmp_path: Path) -> None:
    recorder, lake = made(tmp_path)
    await ask(lake, recorder, want(TQQQ, business_date=DAY))
    (price,) = recorded(recorder, "RecordPrices", "prices")
    assert d(price.price.amount) == Decimal("92.850000000")
