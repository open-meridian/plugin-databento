"""What the plugin records in the lake, and how it answers what the lake wants
(contracts v18 and v19, W10): the SDK's listings feed
(`meridian.roles.data.listings`) for the stock datasets, this module keeping
what is Databento's alone -- its source's name, its history's reach, its
plans -- and chains.py for the option chains.

**One dataset per timeliness** (rulings 382 and 551): Databento sells real
time (its live gateway) and history (its historical API), and no delayed
feed, so each kind is one dataset: `daily`, `live`, `trades` and `options`,
none declaring a delay. What a plan does not reach is declined, not
covered: on a historical-only Stocks plan, live prices and quotes, and a
standing want of trades (a range of trades is read from the history on
either plan); on a historical-only Options plan, option quotes and trades
(chains.py).

**A decline names a reason the lake takes**: `not_covered`,
`source_silent` or `beyond_history`.

**Trades caught up.** Databento states a trade identically from the live
gateway and the historical API, so a trade's row is keyed alike from either
(convert.SourceTrade.row_key): a range a reader wants is read from the
history (and past its reach, on a real-time plan, from the gateway's
intraday replay), and the gap after a reconnect or a restart caught up the
same way, a trade already recorded never sent again (the SDK's feed).

**No venue for a stock.** Databento US Equities Mini anonymizes its venues,
so a stock's price, quote or trade is recorded as consolidated, no venue.

**The edge, done here for now** (contract v28, rulings 424 and 518): each
live price, quote and trade recorded unasked or for a standing want has its
feed delay recorded (`roles.Latency.aged`, the `edge` hop) and is judged by
the SDK's quality checks (`quality.EdgeChecks`). The SDK's listings feed
records neither in 0.33.3; this glue goes when it does.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from typing import Any

from meridian.plugin.v1 import operations_pb2 as ops
from meridian.roles.data import listings
from meridian.roles.data.listings import (
    BAR,
    DAILY,
    LIVE,
    LIVE_CADENCE_SECONDS,
    LIVE_KINDS,
    PRICE,
    QUOTE,
    TRADE,
    TRADES,
    Counts,
    Forming,
    Listings,
    Source,
    Standing,
    row_key,
)
from meridian.roles.data.quality import EdgeChecks

from .chains import OPTIONS, Chains
from .convert import SOURCE, new_york_date, utc_date
from .settings import END_OF_DAY, REAL_TIME
from .vendor import EQUS_SUMMARY

__all__ = [
    "BAR", "DAILY", "LIVE", "LIVE_CADENCE_SECONDS", "LIVE_KINDS", "OPTIONS", "PRICE", "QUOTE",
    "TRADE", "TRADES", "Counts", "Forming", "Lake", "Products", "Source", "Standing",
    "row_key",
]  # fmt: skip

#: The earliest day the daily dataset is asked for until Databento says how
#: far `EQUS.SUMMARY` reaches for the key (its `get_dataset_range`).
HISTORY_FROM = dt.date(2023, 3, 28)
#: The platform's US equity venues (NYSE markets) calendar, read from the
#: platform on 2026-10-14 (the v24 plan's O10), as the retail pair names it:
#: the daily and chain datasets' business dates are its open days.
US_EQUITY_CALENDAR = "CAL-01M4VF0F4XGP3RATCCSKDPZSBP"

#: Which listing each subject is, kept in the plugin's storage.
Products = Listings


class Lake(listings.Feed):
    """Databento's listings in the lake: the SDK's feed, Databento's prints."""

    source_name = SOURCE
    history_from = HISTORY_FROM
    calendar_id = US_EQUITY_CALENDAR

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        #: The option chains wanted, counted as one dataset.
        self.counts[OPTIONS] = listings.Counts()
        self.chains = Chains(self)
        #: What each asset class's plan reaches (settings.py).
        self.plans = {"stocks": END_OF_DAY, "options": END_OF_DAY}
        self.streaming = False
        #: The plugin's `roles.Latency`, where it keeps one, and the edge's
        #: checks.
        self.latency: Any = None
        self.quality = EdgeChecks(clock_ns=self.clock_ns)
        self._reach_on: dt.date | None = None

    # ── Plans and names ──────────────────────────────────────────────────

    def timeliness(self, asset_class: str) -> str:
        return self.plans[asset_class]

    def use_plan(self, stocks: str, options: str) -> None:
        """The plans as the settings stand: on a change, the standing wants
        of the kinds the plan reaches differently are let go; the lake asks
        again."""
        before = dict(self.plans)
        self.plans = {"stocks": stocks, "options": options}
        if before["stocks"] != stocks:
            for want_id, standing in list(self.standing.items()):
                if standing.dataset in (LIVE, TRADES):
                    self.standing.pop(want_id, None)
        self.streaming = stocks == REAL_TIME
        self.restream()
        source = self.source
        if source is not None and hasattr(source, "gateway"):
            source.gateway = {"stocks": stocks == REAL_TIME, "options": options == REAL_TIME}
        if before["options"] != options:
            self.chains.reset()

    def named(self, key: str) -> str:
        """A declared dataset's ID in the deployment, by its own key."""
        return super().dataset(key)

    # ── What is Databento's (listings.Feed) ──────────────────────────────

    def withdrawn_unrecorded(self, stated: Any) -> listings.Attributes | None:
        # Databento's trades schema states no cancel: never reached.
        return None

    async def trade_row(self, stated: Any, subject: str) -> Any:
        """The SDK's trade row, with the side that took liquidity where
        Databento states it: its `side`, the side that initiated the trade."""
        row = await super().trade_row(stated, subject)
        aggressor = getattr(stated, "aggressor", "")
        if row is None or not aggressor or row.cancelled:
            return row
        return dataclasses.replace(row, aggressor=aggressor)

    def sequence_of(self, stated: Any) -> int | None:
        # Databento US Equities Mini numbers no trade (its `sequence` is 0):
        # a trade is keyed by its clocks and place instead.
        return None

    # ── The edge (contract v28) ──────────────────────────────────────────

    def at_the_edge(self, key: str, rows: list[Any], want_id: str) -> list[Any]:
        """Rows of the dataset `key` recorded unasked or for a standing
        want, their age at the edge recorded and judged by the SDK's
        checks; rows pulled for a read as they are. A chain's rows are
        judged series by series (their subject is the underlying)."""
        standing = {w.want_id for w in self.standing.values()}
        for held in self.chains.underlyings.values():
            standing |= set(held.standing) | set(held.trades)
        if want_id and want_id not in standing:
            return rows
        dataset = self.named(key)
        if self.latency is not None:
            for row in rows:
                self.latency.aged(dataset, "AGE_HOP_EDGE", row)
        if key != OPTIONS:
            return self.quality.judged(key, rows)
        out: list[Any] = []
        for row in rows:
            series = row.meta.series_key[0].value if row.meta.series_key else ""
            out.extend(self.quality.judged(f"{key}/{series}", [row]))
        return out

    async def _record(self, key: str, data_type: str, rows: Any, want_id: str) -> None:
        if key in (LIVE, TRADES) and data_type in (PRICE, QUOTE, TRADE):
            rows = self.at_the_edge(key, list(rows), want_id)
        await super()._record(key, data_type, rows, want_id)

    # ── Wants ────────────────────────────────────────────────────────────

    def refused(self, want: ops.ObservationsWantedEvent) -> str:
        """Why a want of a stock dataset is not this plan's, or ""."""
        key = self._key_of(want.dataset)
        if self.plans["stocks"] == REAL_TIME:
            return ""
        if key == LIVE:
            # Live prices and quotes come from the live gateway alone.
            return "not_covered"
        if key == TRADES and want.standing:
            # Streamed from the live gateway alone; a range is the history's.
            return "not_covered"
        return ""

    async def on_want(self, heard: Any) -> None:
        """A want of a chain is chains.py's; one this plan does not serve is
        declined, not covered; any other is the SDK's to answer."""
        want: ops.ObservationsWantedEvent = heard.message
        key = self._key_of(want.dataset)
        await self.reference()
        if key == OPTIONS:
            await self.chains.on_want(want)
            return
        reason = self.refused(want)
        if reason and key in self.counts:
            self.counts[key].wants += 1
            subjects = [s.entity_id for s in want.subjects]
            await self._decline(key, want.want_id, subjects, reason, settled=False)
            return
        await super().on_want(heard)

    async def on_withdrawn(self, heard: Any) -> None:
        """A standing want withdrawn, a chain's among them."""
        self.chains.withdraw(heard.message.want_id)
        await super().on_withdrawn(heard)

    async def reference(self) -> None:
        """How far Databento's daily history reaches for the key, asked once
        a day: an older day is declined, beyond history. A failure is said
        and asked again later."""
        source: Any = self.source
        if source is None or not hasattr(source, "historical"):
            return
        today = new_york_date(self.clock_ns())
        if self._reach_on == today:
            return
        reach = await self._fetch(lambda: source.historical.reach(EQUS_SUMMARY))
        if reach is not None:
            self._reach_on = today
            if reach.start_ns:
                self.history_from = utc_date(reach.start_ns)

    async def tick(self) -> None:
        """The history's reach, the SDK's timetable, then the chains due
        (chains.py)."""
        await self.reference()
        await super().tick()
        await self.chains.refresh()
