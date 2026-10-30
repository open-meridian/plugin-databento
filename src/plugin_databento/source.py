"""Databento, as the lake's half of the plugin asks it (lake.Source): blocking
calls, which the SDK's feed runs in a thread.

- **Daily bars**: `EQUS.SUMMARY`'s `ohlcv-1d` from the historical API, many
  listings a call.
- **Live prices and quotes**: a short replay of `EQUS.MINI`'s `bbo-1m` on
  the live gateway, the last `LOOK_BACK_NS`, each listing's latest record
  (gateway.replay): Databento keeps no snapshot to ask.
- **Trades**: `EQUS.MINI`'s `trades` from the historical API as far as it
  reaches for this key, and past that, on a real-time plan, from the live
  gateway's intraday replay (the last 24 hours): a range a reader wants, the
  gap after a reconnect or a restart (the SDK's feed asks, `trades`).
- **Option chains** (chains.py): an underlying's series from `OPRA.PILLAR`'s
  `definition`, and their open interest and volume from its `statistics`,
  each of the latest day the historical API reaches, read by Databento's
  parent symbol (`SPY.OPT`); on a real-time plan, their consolidated best
  bid and offer from a short replay of `cbbo-1m`.

Each listing is asked and named in the one spelling (`BRK.B`), which
Databento's US equity datasets share (Nasdaq's convention).
"""

from __future__ import annotations

import datetime as dt
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from meridian.edge import RawStore

from . import convert, gateway
from .convert import (
    ChainStatistic,
    Contract,
    DailyBar,
    Numbering,
    Quoted,
    SourceQuote,
    SourceTrade,
    option_root,
    utc_date,
    utc_midnight,
)
from .settings import Credentials
from .vendor import (
    EQUS_MINI,
    EQUS_SUMMARY,
    MOST_PAGES,
    NS,
    OPRA,
    SYMBOLS_PER_CALL,
    TRADES_LIMIT,
    Fetched,
    Historical,
    VendorError,
)

#: How far back a live price's replay reaches: a quiet listing's last
#: minute bar may be some minutes old.
LOOK_BACK_NS = 15 * 60 * NS
#: How far back a chain's quotes' replay reaches.
CHAIN_LOOK_BACK_NS = 2 * 60 * NS
#: The live gateway's intraday replay reaches 24 hours; a little less is asked.
REPLAY_REACH_NS = 23 * 3600 * NS
DAY_NS = 86_400 * NS


@dataclass(frozen=True)
class Answer:
    """Whether Databento answered the key, and what it said where it did not."""

    answered: bool
    detail: str = ""


Replay = Callable[[str, Sequence[gateway.Subscription]], gateway.Replayed]


def gateway_replay(
    credentials: Credentials | None, connect: gateway.Connect = gateway.socket_connect
) -> Replay:
    """`gateway.replay` with the key and the connection given."""

    def replay(dataset: str, subscriptions: Sequence[gateway.Subscription]) -> gateway.Replayed:
        if credentials is None:
            raise gateway.Refused("no Databento API key: enter it in Settings", 1)
        return gateway.replay(credentials, dataset, subscriptions, connect=connect)

    return replay


class DatabentoSource:
    """lake.Source on Databento's historical API and its live gateway."""

    def __init__(
        self,
        historical: Historical,
        replay: Replay,
        store: RawStore,
        synthetic: bool = False,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.historical = historical
        self.replay_on = replay
        self.store = store
        self.synthetic = synthetic
        self.clock_ns = clock_ns
        self._answer: Answer | None = None
        self._lock = threading.Lock()
        #: Which asset classes' live gateway the plan reaches (settings).
        self.gateway = {"stocks": False, "options": False}
        #: Replays made since start, for the Connection page.
        self.replays = 0
        self.replay_error = ""
        #: Listings Databento answered it does not know: none so far (an
        #: empty answer is no proof).
        self.invalid: set[str] = set()

    # ── Whether the key answers ──────────────────────────────────────────

    def probe(self, again: bool = False) -> Answer:
        """Whether Databento answers the key: how far `EQUS.SUMMARY` reaches
        for it, asked once and again when the key changes."""
        with self._lock:
            if self._answer is None or again:
                try:
                    self.historical.reach(EQUS_SUMMARY, again=True)
                    self._answer = Answer(True)
                except VendorError as refused:
                    self._answer = Answer(False, f"{refused.status} {refused.detail}")
            return self._answer

    @property
    def known(self) -> Answer | None:
        return self._answer

    # ── Helpers ──────────────────────────────────────────────────────────

    def _range(
        self,
        dataset: str,
        schema: str,
        symbols: Sequence[str],
        start_ns: int,
        end_ns: int,
        *,
        stype_in: str = "raw_symbol",
        name: str,
    ) -> list[tuple[Fetched, list[tuple[str, dict[str, Any]]]]]:
        """A schema's records of `symbols`, `SYMBOLS_PER_CALL` a call, clamped
        to the dataset's reach; each answer with its records and their
        symbols."""
        reach = self.historical.reach(dataset)
        end = min(end_ns, reach.end(schema) or end_ns)
        if end <= start_ns or not symbols:
            return []
        out = []
        for first in range(0, len(symbols), SYMBOLS_PER_CALL):
            chunk = list(symbols[first : first + SYMBOLS_PER_CALL])
            fetched = self.historical.get_range(
                dataset, schema, chunk, start_ns, end, stype_in=stype_in, call=schema,
                name=name if len(chunk) == 1 else f"{len(chunk)}-{name}",
            )  # fmt: skip
            out.append((fetched, _named(fetched.value)))
        return out

    def replay(
        self, dataset: str, subscriptions: Sequence[gateway.Subscription], name: str
    ) -> tuple[str, list[tuple[str, dict[str, Any]]]]:
        """A short replay on the live gateway: kept as one raw record, its
        records with their symbols."""
        try:
            heard = self.replay_on(dataset, subscriptions)
        except (gateway.GatewayError, OSError, TimeoutError) as failed:
            self.replay_error = f"{type(failed).__name__}: {failed}"[:300]
            raise
        self.replays += 1
        self.replay_error = "; ".join(heard.unresolved)[:300]
        raw_key = self.store.keep(
            "replay",
            name,
            {
                "received_at_ns": self.clock_ns(),
                "dataset": dataset,
                "subscriptions": [
                    {
                        "schema": s.schema,
                        "stype_in": s.stype_in,
                        "symbols": list(s.symbols),
                        "start": s.start_ns,
                    }
                    for s in subscriptions
                ],  # fmt: skip
                "lines": heard.lines,
            },
        )
        return raw_key, heard.records

    # ── Stocks (lake.Source) ─────────────────────────────────────────────

    def daily(self, symbols: Sequence[str], first: dt.date, last: dt.date) -> list[DailyBar]:
        bars: list[DailyBar] = []
        start, end = utc_midnight(first), utc_midnight(last + dt.timedelta(days=1))
        for fetched, records in self._range(
            EQUS_SUMMARY, "ohlcv-1d", symbols, start, end, name=symbols[0] if symbols else ""
        ):
            bars.extend(convert.daily(records, fetched.raw_key))
        return bars

    def _bbo(self, symbols: Sequence[str]) -> tuple[str, dict[str, dict[str, Any]]]:
        start = self.clock_ns() - LOOK_BACK_NS
        raw_key, records = self.replay(
            EQUS_MINI,
            [gateway.Subscription("bbo-1m", tuple(symbols), start_ns=start)],
            symbols[0] if len(symbols) == 1 else f"{len(symbols)}-listings",
        )
        return raw_key, convert.latest(records)

    def live(self, symbols: Sequence[str]) -> list[Quoted]:
        raw_key, held = self._bbo(symbols)
        return [q for symbol, said in held.items() for q in convert.bbo_live(symbol, said, raw_key)]

    def quotes(self, symbols: Sequence[str]) -> list[SourceQuote]:
        raw_key, held = self._bbo(symbols)
        out: list[SourceQuote] = []
        for symbol, said in held.items():
            listing = convert.spelled(symbol)
            made = convert.quote(listing, said, raw_key, "bbo") if listing else None
            if made is not None:
                out.append(made)
        return out

    def trades(self, symbols: Sequence[str], start_ns: int, end_ns: int) -> list[SourceTrade]:
        """Each listing's trades from `start_ns` to before `end_ns`: from the
        historical API as far as it reaches, then, on a real-time plan, from
        the live gateway's intraday replay."""
        numbering = Numbering()
        found: list[SourceTrade] = []
        reach = self.historical.reach(EQUS_MINI).end("trades")
        for symbol in symbols:
            start, pages = start_ns, 0
            mine: list[SourceTrade] = []
            while start < min(end_ns, reach) and pages < MOST_PAGES:
                pages += 1
                fetched = self.historical.get_range(
                    EQUS_MINI, "trades", [symbol], start, min(end_ns, reach),
                    limit=TRADES_LIMIT, call="trades", name=symbol,
                )  # fmt: skip
                records = _named(fetched.value)
                mine.extend(convert.trades(_after(mine, records), fetched.raw_key, numbering))
                if len(records) < TRADES_LIMIT:
                    break
                start = convert.ts_recv(records[-1][1]) or end_ns
            found.extend(mine)
        past = max(start_ns, reach, self.clock_ns() - REPLAY_REACH_NS)
        if self.gateway["stocks"] and past < end_ns:
            raw_key, records = self.replay(
                EQUS_MINI,
                [gateway.Subscription("trades", tuple(symbols), start_ns=past)],
                symbols[0] if len(symbols) == 1 else f"{len(symbols)}-listings",
            )
            for made in convert.trades(records, raw_key, numbering):
                if made.at_ns < end_ns:
                    found.append(made)
        return found

    # ── Options (chains.py) ──────────────────────────────────────────────

    def _latest_day(self, schema: str) -> tuple[int, int]:
        """The latest UTC day of OPRA's `schema` the historical API reaches:
        its start, and the end of what is there of it."""
        end = self.historical.reach(OPRA).end(schema)
        if not end:
            end = self.clock_ns()
        day = utc_date(end - 1)
        return utc_midnight(day), min(end, utc_midnight(day) + DAY_NS)

    def contracts(self, symbol: str) -> list[Contract | Any]:
        """An underlying's option series as OPRA defines them on the latest
        day the history reaches: each series' terms, or what did not convert,
        as reported."""
        start, end = self._latest_day("definition")
        parent = f"{option_root(symbol)}.OPT"
        out: list[Contract | Any] = []
        for fetched, records in self._range(
            OPRA, "definition", [parent], start, end, stype_in="parent", name=parent
        ):
            out.extend(convert.contracts(records, fetched.raw_key))
        return out

    def statistics(self, symbol: str, on: dt.date | None = None) -> list[ChainStatistic | Any]:
        """An underlying's series' open interest and cleared volume, as OPRA
        published them on the UTC day `on`, or the latest day the history
        reaches."""
        if on is None:
            start, end = self._latest_day("statistics")
        else:
            start = utc_midnight(on)
            end = start + DAY_NS
        parent = f"{option_root(symbol)}.OPT"
        out: list[ChainStatistic | Any] = []
        for fetched, records in self._range(
            OPRA, "statistics", [parent], start, end, stype_in="parent", name=parent
        ):
            out.extend(convert.statistics(records, fetched.raw_key))
        return out

    def chain_quotes(self, symbol: str) -> list[SourceQuote]:
        """An underlying's series' latest consolidated best bid and offer,
        from a short replay of `cbbo-1m`."""
        parent = f"{option_root(symbol)}.OPT"
        raw_key, records = self.replay(
            OPRA,
            [
                gateway.Subscription(
                    "cbbo-1m", (parent,), "parent", self.clock_ns() - CHAIN_LOOK_BACK_NS
                )
            ],
            parent,
        )
        return list(convert.option_quotes(records, raw_key))


def _named(value: Any) -> list[tuple[str, dict[str, Any]]]:
    """The historical API's records, each with the symbol it carries."""
    if not isinstance(value, list):
        return []
    return [(str(r.get("symbol") or ""), r) for r in value if isinstance(r, dict)]


def _after(
    found: Sequence[SourceTrade], records: list[tuple[str, dict[str, Any]]]
) -> list[tuple[str, dict[str, Any]]]:
    """A page's records past those the last page stated: a page read on from
    the last one's last moment states that moment's records again."""
    if not found:
        return records
    last = found[-1].received_ns
    seen = sum(1 for t in found if t.received_ns == last)
    out: list[tuple[str, dict[str, Any]]] = []
    for symbol, said in records:
        if convert.ts_recv(said) == last and seen:
            seen -= 1
            continue
        out.append((symbol, said))
    return out
