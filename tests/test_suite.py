"""The data suite, the SDK's own (meridian.suites), run unchanged against this
plugin: every case mapped to the plugin's own conversion, from Databento's
synthetic exchange and gateway, through the lake's half it runs in
production (lake.py, chains.py), against the suite's `Recorder`.

Presented: closes, bars, subjects; the side that took liquidity, where
Databento states it; quotes, two-sided and one-sided; a standing want's
trades streamed; a restart's gap backfilled from the history and the live
gateway's replay, which state a trade identically; the licences read; the
calendar read, which dates a chain's open interest; the bounded lake
query; the dataset's liveness; the feed delay; the chain's series, open
interest and quotes, never resolved; a crossed quote recorded invalid by
the edge's checks.

Not presented, each with why (`NOT_PRESENTED`): a currency's rate and a
crypto asset's price; a venue (Databento US Equities Mini anonymizes its
venues); a trade's eligibilities and conditions (Databento normalizes no
trade condition); the resting side (Databento states the side that took
liquidity); a withdrawn trade (no cancel is stated); a day still forming
(the summary is published once the day is over); a settlement date;
outcomes and event groups; the rates and filings slices, holdings,
dividends and ratings; a duplicate quote (each BBO record is its own
interval's) and a sequence gap (Mini numbers no message).
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from meridian.plugin.v1 import operations_pb2 as ops
from meridian.roles.data.testing import RATES_CASES, feed_delay, lake_query
from meridian.suites import Recorder, run

from plugin_databento import convert
from plugin_databento.lake import LIVE_KINDS, Standing

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
    synthetic_lake,
    want,
)

DAY = "2026-10-08"
#: Monday morning after the synthetic week: every day of it final.
AFTER = at("2026-10-12", 9)
#: Friday 2026-10-09, 15:00 New York: the session still open.
DURING = at("2026-10-09", 15)

Producer = Callable[[Recorder], Awaitable[None]]

NO_EVENT_CONTRACT = "Databento's US equity and OPRA datasets list no event contract"
NOT_PRESENTED: dict[str, str] = {
    "an-fx-rate": "Databento's US equity and OPRA datasets state no currency's rate",
    "a-stablecoin-quote": "Databento's US equity and OPRA datasets state no crypto asset's price",
    "a-venue-resolved": (
        "Databento US Equities Mini anonymizes its venues: a stock's price names none"
    ),
    "a-venue-not-held": (
        "Databento US Equities Mini anonymizes its venues; OPRA's are kept as reported"
    ),
    "a-trade-recorded": (
        "Databento normalizes no trade condition, so no trade's eligibility is known"
    ),
    "a-condition-converted": "Databento normalizes no trade condition",
    "a-condition-not-converted": "Databento normalizes no trade condition",
    "the-makers-side-inverted": (
        "Databento states the side that took liquidity, never the resting order's"
    ),
    "a-trade-withdrawn": "Databento's trades schema states no cancel",
    "the-forming-day-restated": (
        "Databento's daily summary is published once the day is over, never while it forms"
    ),
    "asks-for-a-settlement-date": (
        "records bars and trades as Databento publishes them; never computes a settlement date"
    ),
    "records-an-outcome": NO_EVENT_CONTRACT,
    "restates-a-disputed-outcome": NO_EVENT_CONTRACT,
    "records-a-partial-payout": NO_EVENT_CONTRACT,
    "reports-an-event-group": NO_EVENT_CONTRACT,
    "reports-a-flag-the-venue-does-not-state": NO_EVENT_CONTRACT,
    **{case: "this version reads no rates" for case in RATES_CASES},
    **{
        case: "this version reads no filings"
        for case in ("a-filing-recorded", "a-fact-restated", "a-fact-confirmed", "resolves-a-filer")
    },
    **{
        case: "this version reads no holdings report"
        for case in ("a-complete-holdings-report", "a-cusip-used-as-key", "a-stake-crossed")
    },
    "a-dividend-declared": "this version reads no corporate action",
    "a-rating-action": "Databento's US equity and OPRA datasets state no rating",
    "a-duplicate-is-recorded-suspicious": (
        "each of Databento's BBO records is stamped with its own interval's end: a quote "
        "stated again is a later interval's, never the same message renumbered"
    ),
    "rows-after-a-gap-are-suspicious": (
        "Databento US Equities Mini numbers no message (its sequence is 0): no gap is seen"
    ),
}

HD = {"rtype": 0, "publisher_id": 91, "instrument_id": 15144}


def bbo(symbol: str = "SPY", bid: str | None = "673.870000000",
        ask: str | None = "673.890000000", at_ns: int = DURING) -> Any:  # fmt: skip
    """A `bbo-1s` record of Databento's, as the stream converts it."""
    said = {
        "ts_recv": str(at_ns),
        "hd": HD | {"ts_event": str(at_ns - 2 * 10**9), "rtype": 195},
        "price": "673.880000000",
        "size": 100,
        "levels": [{"bid_px": bid or convert.UNDEF_PRICE_TEXT,
                    "ask_px": ask or convert.UNDEF_PRICE_TEXT,
                    "bid_sz": 300, "ask_sz": 200 if ask else 0}],
    }  # fmt: skip
    return convert.quote(symbol, said, "stream/suite#q", "bbo")


def trade(side: str = "N", at_ns: int = DURING - 10**9) -> Any:
    """A `trades` record of Databento's, as the stream converts it."""
    said = {"ts_recv": str(at_ns + 210_000), "hd": HD | {"ts_event": str(at_ns)},
            "price": "673.880000000", "size": 100, "side": side}  # fmt: skip
    return convert.trade("SPY", said, "stream/suite#t", convert.Numbering())


def producers(tmp: Path) -> dict[str, Producer]:
    def lake(recorder: Recorder, now: int = AFTER):  # type: ignore[no-untyped-def]
        answer_records(recorder, RECORDS)
        answer_calendar(recorder)
        return synthetic_lake(recorder, Clock(now), tmp / uuid.uuid4().hex)

    async def wanted(
        recorder: Recorder, event: ops.ObservationsWantedEvent, now: int = AFTER
    ) -> Any:
        held = lake(recorder, now)
        recorder.answer("ObservationsWanted", lambda _: event)
        await recorder.receive(observations_wanted=held.on_want)
        return held

    async def daily_close(recorder: Recorder) -> None:
        # The plugin resolves a listing by its own symbol, then records the
        # close the lake wants for it.
        held = lake(recorder)
        await held.seed(["BRK.B"])
        (subject,) = held.products.symbols
        recorder.answer("ObservationsWanted", lambda _: want(subject, business_date=DAY))
        await recorder.receive(observations_wanted=held.on_want)

    async def exact(recorder: Recorder) -> None:
        # TQQQ's close on 2026-10-09 is stated to nine places, as text.
        await wanted(recorder, want(TQQQ, business_date="2026-10-09"))

    async def bar(recorder: Recorder) -> None:
        await wanted(recorder, want(BRK, data_type="meridian.v1.Bar", business_date=DAY))

    async def streamed(
        recorder: Recorder,
        *stated: Any,
        data_type: str = "meridian.v1.Trade",
        dataset: str = "suite-plugin:trades",
        subject: str = SPY,
    ) -> Any:
        # A standing want: the listing streamed, and what the stream states
        # recorded, as the plugin's stream hands it on.
        held = lake(recorder, DURING)
        asked = want(subject, dataset=dataset, data_type=data_type, standing=True)
        recorder.answer("ObservationsWanted", lambda _: asked)
        await recorder.receive(observations_wanted=held.on_want)
        for said in stated:
            await held.heard([said])
            await held.flush(force=True)
        return held

    async def standing_streamed(recorder: Recorder) -> None:
        await streamed(recorder, trade())

    async def takers(recorder: Recorder) -> None:
        # Databento's `B`: a buyer initiated the trade.
        await streamed(recorder, trade("B"))

    async def quoted(recorder: Recorder) -> None:
        await streamed(recorder, bbo(), data_type="meridian.v1.Quote", dataset="suite-plugin:live")

    async def crossed(recorder: Recorder) -> None:
        await streamed(
            recorder, bbo(bid="673.950000000"), data_type="meridian.v1.Quote",
            dataset="suite-plugin:live",
        )  # fmt: skip

    async def one_sided(recorder: Recorder) -> None:
        # BRK.B's offer side empty.
        await streamed(
            recorder, bbo("BRK.B", "490.580000000", None), data_type="meridian.v1.Quote",
            dataset="suite-plugin:live", subject=BRK,
        )  # fmt: skip

    async def alive(recorder: Recorder) -> None:
        # A heartbeat, no price changed: the stream's liveness.
        held = await streamed(recorder)
        await held.heard([])
        await held.flush(force=True)

    async def delayed(recorder: Recorder, latency: Any) -> None:
        # Quotes streamed for a standing want, each one's age at the edge
        # recorded, then reported.
        held = lake(recorder, DURING)
        held.latency = latency
        asked = want(SPY, dataset="suite-plugin:live", data_type="meridian.v1.Quote", standing=True)
        recorder.answer("ObservationsWanted", lambda _: asked)
        await recorder.receive(observations_wanted=held.on_want)
        await held.heard([bbo(at_ns=DURING - 10**8)])
        await held.flush(force=True)

    async def chain(recorder: Recorder) -> None:
        # SPY's chain wanted of `options`: its open interest recorded under
        # SPY, keyed by OSI symbol, dated by the deployment's calendar.
        await wanted(
            recorder,
            want(SPY, dataset="suite-plugin:options", data_type="meridian.v1.Statistic"),
            now=DURING,
        )

    async def series(recorder: Recorder) -> None:
        await wanted(
            recorder,
            want(SPY, dataset="suite-plugin:options", data_type="meridian.v1.ListedSeries"),
            now=DURING,
        )

    async def chain_quotes(recorder: Recorder) -> None:
        await wanted(
            recorder,
            want(SPY, dataset="suite-plugin:options", data_type="meridian.v1.Quote"),
            now=DURING,
        )

    async def restarted(recorder: Recorder) -> None:
        # The plugin starts again; the lake holds the last trade of SPY it
        # recorded, two minutes ago, and Databento's live gateway replays
        # the trades since. The first standing want of SPY's trades
        # backfills them.
        now = DURING
        held = lake(recorder, now)
        last = ops.Trade(
            meta=ops.ObservationMeta(
                row_key=f"SPY:t:1:1@{SPY}",
                subjects=[ops.SubjectRef(entity_id=SPY)],
                source=ops.Source(dataset="suite-plugin:trades"),
                valid_from_ns=now - 120 * 10**9,
            )
        )
        recorder.answer("ListTrades", lambda _: ops.ListTradesResult(trades=[last]))
        asked = want(
            SPY, dataset="suite-plugin:trades", data_type="meridian.v1.Trade", standing=True
        )
        recorder.answer("ObservationsWanted", lambda _: asked)
        await recorder.receive(observations_wanted=held.on_want)

    async def ambiguous(recorder: Recorder) -> None:
        held = lake(recorder)
        recorder.answer(
            "ResolveIdentifier",
            lambda _: ops.ResolveIdentifierResult(
                found=False, miss_reason=ops.MISS_REASON_AMBIGUOUS
            ),
        )
        await held.seed(["TQQQ"])

    async def recorded_against(recorder: Recorder) -> None:
        await wanted(recorder, want(SPY, business_date=DAY))

    async def elsewhere(recorder: Recorder) -> None:
        # TQQQ as a custodian's book holds it, by its CUSIP and the
        # custodian's symbol: the record read (W3.6), Databento's symbol found.
        await wanted(recorder, want(TQQQ, business_date=DAY))

    async def declined(recorder: Recorder) -> None:
        await wanted(recorder, want(ISIN_ONLY, business_date=DAY))

    async def licences(recorder: Recorder) -> None:
        from plugin_databento.__main__ import read_licences
        from plugin_databento.page import State

        recorder.answer(
            "ListDatasets",
            lambda _: ops.ListDatasetsResult(
                licences=[ops.DatasetLicence(dataset="suite-plugin:daily", kept=True)]
            ),
        )
        state = State()
        await read_licences(recorder, state)
        assert state.licensed == {"suite-plugin:daily"}

    async def withdrawn(recorder: Recorder) -> None:
        clock = Clock(AFTER)
        answer_records(recorder, RECORDS)
        held = synthetic_lake(recorder, clock, tmp / "withdrawn")
        held.standing["WNT-9"] = Standing("WNT-9", (SPY,), LIVE_KINDS, AFTER)
        recorder.answer(
            "WantWithdrawn",
            lambda _: [ops.WantWithdrawnEvent(want_id="WNT-9", dataset="suite-plugin:live")],
        )
        await recorder.receive(want_withdrawn=held.on_withdrawn)
        clock.now += 3600 * 10**9
        await held.tick()

    return {
        "a-daily-close": daily_close,
        "an-exact-price": exact,
        "a-daily-bar": bar,
        "an-asset-that-does-not-resolve": ambiguous,
        "a-want-recorded-against": recorded_against,
        "a-subject-another-source-resolved": elsewhere,
        "a-subject-declined": declined,
        "a-standing-want-withdrawn": withdrawn,
        "the-takers-side-stated": takers,
        "a-quote-recorded": quoted,
        "a-one-sided-quote": one_sided,
        "a-standing-want-streamed": standing_streamed,
        "a-restarts-gap-backfilled": restarted,
        "reads-its-datasets-licences": licences,
        "reads-a-calendar": chain,
        "queries-the-lake-within-bounds": lake_query(SPY, "trades"),
        "records-a-datasets-liveness": alive,
        "reports-its-feed-delay": feed_delay(delayed),
        "records-a-chains-series": series,
        "records-open-interest": chain,
        "a-chain-row-is-never-resolved": chain_quotes,
        "a-crossed-quote-is-recorded-invalid": crossed,
    }


def test_the_data_suite_passes_unchanged(tmp_path: Path) -> None:
    report = run("data", producers(tmp_path), not_presented=NOT_PRESENTED)
    assert report.passed, report.failures
    # Its 22 cases, and the three every suite holds that the SDK answers.
    assert len(report.passed_cases) == 25
    assert report.answered_by_sdk == [
        "hears-and-reads-a-replacement",
        "reads-the-template-catalogue",
        "hears-and-reads-an-event-group",
    ]
    assert sorted(report.not_presented) == sorted(NOT_PRESENTED)
