"""OPRA's option chains, as Databento carries them, in the lake (contract v28:
meridian-design spec/the-options-and-event-contracts-sample-plugin; rulings
561, 567, 617, 660 and 821).

**One dataset, `options`** (ruling 660's split, after 551 and 382, is by
timeliness, and Databento sells no delayed OPRA feed): OPRA's data as
Databento carries it from its feed (`OPRA.PILLAR`), licensed and entitled
apart from the equity datasets. Its series, open interest and volume come
from the historical API on any plan; its quotes and trades only from the
live gateway, on a real-time plan (the Options plan setting): on a
historical-only plan a want of them is declined, not covered.

**Never minted, never resolved** (ruling 561, W10.4): every row names the
underlying's record as its subject and the series by its OCC OSI symbol
(`occ_osi`, W1.26) as its series key, through the SDK's helpers
(`meridian.roles.chains`): each series' terms as the platform's
listed-option template's fields (`TPL-listed-option`, version 1; ruling
617), its best bid and offer as a quote, its open interest and volume as
statistics, its trades as trades. A series gets an `INS-` record only when a
reader asks for one (RequestSeriesRecords, W6.37), never this plugin.

**What Databento states, and what it does not:**
- The series: `definition` records of the underlying's parent symbol
  (`SPY.OPT`, its OCC root's), the latest day the historical API reaches,
  read once a day: OSI symbol, put or call, strike, expiration, multiplier
  (`unit_of_measure_qty`), and style and settlement from the definition's
  CFI code (ISO 10962). A series whose definition states no option CFI is
  not recorded, its code kept as reported: no style is guessed. A standard
  contract -- its root its underlying's, 100 shares, physical -- delivers
  100 shares of the underlying; an adjusted one's deliverable is not stated
  by the deployment's records, so none is recorded. No settlement time.
- Open interest and volume: OPRA's `statistics` (`stat_type` 9, open
  interest; 6, the volume cleared), published before the open for the
  market day before; each dated that day by the deployment's US equity
  calendar (the dataset's `calendar_id`, ReadCalendar); where the calendar
  does not answer, none is recorded rather than misdated. A want of a past
  date reads the statistics published the market day after it. A value
  Databento leaves out records no row, never a zero (W10.4). Greeks and
  implied volatility are never read: the engine computes its own (565).
- Quotes: OPRA's consolidated best bid and offer (`cbbo-1m`), a short
  replay of the last minutes for the whole underlying, at most once a
  minute for a standing want; sizes in contracts; a side with no price is
  empty.
- Trades: OPRA's prints, streamed from the live gateway for the series a
  standing want names, up to `STREAM_LIMIT`; each keyed by its series, its
  two clocks and its place; its venue Databento's publisher, by its MIC,
  kept as reported; no condition (Databento normalizes none) and never an
  aggressor (OPRA states no side). A range of an option's trades is not read.

**Bounded by what readers ask** (rulings 152 and 821): a want names its
underlyings and, from contract v28's wants, the expirations it shows
(`expiration_from`, `expiration_to`) or its series; only those are
recorded. Only what changed is sent again: a quote when its prices or sizes
changed (ruling 770), a statistic when its value did; each pass that read
a chain's quotes records the dataset's liveness, which confirms every quote
in force.
"""

from __future__ import annotations

import datetime as dt
import logging
from collections import OrderedDict, deque
from collections.abc import Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from meridian.edge import as_reported
from meridian.errors import MeridianError
from meridian.operations import Money, Quote, SourceTime, Trade
from meridian.plugin.v1 import operations_pb2 as ops
from meridian.roles import chains as helpers
from meridian.roles.data.listings import Counts

from .convert import (
    NEW_YORK,
    SOURCE,
    STANDARD_SHARES,
    ChainStatistic,
    Contract,
    Numbering,
    SourceQuote,
    SourceTrade,
    new_york_date,
)
from .settings import REAL_TIME

log = logging.getLogger("databento.chains")

#: The chain's dataset (ruling 660, one timeliness).
OPTIONS = "options"
SERIES = "meridian.v1.ListedSeries"
STATISTIC = "meridian.v1.Statistic"
QUOTE = "meridian.v1.Quote"
TRADE = "meridian.v1.Trade"
DATA_TYPES = {OPTIONS: (SERIES, STATISTIC, QUOTE, TRADE)}
#: What a historical-only plan serves of the chain.
HISTORICAL = (SERIES, STATISTIC)
#: How often a standing want's quotes are read again, at most (152).
CADENCE_SECONDS = 60
#: The chain quote replays a minute, at most.
READS_PER_MINUTE = 10
#: The most series one options session streams.
STREAM_LIMIT = 1_000
#: The platform's listed-option template (ruling 617) and its version.
TEMPLATE_ID = "TPL-listed-option"
TEMPLATE_VERSION = "1"
CURRENCY = "USD"
#: How far back a market day is looked for.
LOOK_BACK_DAYS = 14
#: How many rows' last sendings are remembered, to send only what changed.
REMEMBERED = 200_000
#: How many option trades recorded are remembered, so none is sent twice.
RECENT_TRADES = 50_000
BATCH = 500
NS = 1_000_000_000
MINUTE_NS = 60 * NS
#: Streamed option trades wait at most this long before they are recorded.
TRADE_EVERY_NS = NS // 2


@dataclass
class Underlying:
    """One underlying whose chain is wanted: its listing, its series as
    defined on the latest day read, when its statistics and quotes were last
    read, the wants waiting on quotes, the standing wants keeping quotes,
    statistics and series current, and the series each standing want of
    trades streams."""

    subject: str
    symbol: str
    terms: dict[str, Contract] = field(default_factory=dict)
    listed_on: dt.date | None = None
    stats_on: dt.date | None = None
    quoted_at: int = 0
    pending: dict[str, set[dt.date]] = field(default_factory=dict)
    standing: dict[str, set[dt.date]] = field(default_factory=dict)
    statistics: dict[str, set[dt.date]] = field(default_factory=dict)
    trades: dict[str, list[str]] = field(default_factory=dict)
    series: set[str] = field(default_factory=set)

    @property
    def expirations(self) -> list[dt.date]:
        return sorted({c.expiration for c in self.terms.values()})

    @property
    def wanted(self) -> bool:
        return bool(self.pending or self.standing or self.statistics or self.trades or self.series)


def _window(want: Any) -> tuple[dt.date | None, dt.date | None]:
    first = dt.date.fromisoformat(want.expiration_from) if want.expiration_from else None
    last = dt.date.fromisoformat(want.expiration_to) if want.expiration_to else None
    return first, last


def _within(day: dt.date, first: dt.date | None, last: dt.date | None) -> bool:
    return (first is None or day >= first) and (last is None or day <= last)


class Chains:
    """The chains wanted of `lake` (lake.Lake), read from its source and
    recorded, and the series whose trades are streamed."""

    def __init__(self, lake: Any) -> None:
        self.lake = lake
        self.underlyings: dict[str, Underlying] = {}
        #: When each quote replay of the last minute was made.
        self.calls: deque[int] = deque()
        #: Each row's last sending, by (data type, dataset, row key).
        self.sent: OrderedDict[tuple[str, str, str], str] = OrderedDict()
        #: Each trade recorded, by row key: none sent twice.
        self.recent: OrderedDict[str, None] = OrderedDict()
        #: Market days as the deployment's calendar states them.
        self.days: dict[dt.date, bool] = {}
        self.calendar_read: tuple[dt.date, dt.date] | None = None
        self.calendar_error = ""
        #: Quote reads since start; when a chain's quotes were last read or a
        #: series' message heard, and the liveness last recorded.
        self.read_count = 0
        self.alive_ns = 0
        self.liveness_ns = 0
        #: The series streamed, as OSI symbols, and those past the limit.
        self.streamed: list[str] = []
        self.past_limit: set[str] = set()
        self.streamer: Any = None
        self.pending_trades: list[Trade] = []
        self.numbering = Numbering()
        self._trades_at = 0

    @property
    def counts(self) -> Counts:
        counts: Counts = self.lake.counts[OPTIONS]
        return counts

    # ── Names ────────────────────────────────────────────────────────────

    def timeliness(self) -> str:
        return str(self.lake.timeliness("options"))

    def dataset(self) -> str:
        return str(self.lake.named(OPTIONS))

    def now(self) -> int:
        return int(self.lake.clock_ns())

    # ── Wants (W10.7) ────────────────────────────────────────────────────

    def refused(self, want: Any) -> str:
        """Why a want is not this plan's to answer, or "" where it is."""
        if want.data_type not in DATA_TYPES[OPTIONS]:
            return "not_covered"
        if self.timeliness() != REAL_TIME and want.data_type not in HISTORICAL:
            # Quotes and trades come from the live gateway alone.
            return "not_covered"
        if want.data_type == TRADE and not want.standing:
            # A range of an option's trades is not read: streamed only.
            return "not_covered"
        if self.lake.source is None:
            return "source_silent"
        return ""

    async def on_want(self, want: Any) -> None:
        """A want of a chain's series, quotes, statistics or trades: each
        subject an underlying, the expirations or series it names recorded
        (and kept current, standing), or declined with why."""
        subjects = [s.entity_id for s in want.subjects]
        self.counts.wants += 1
        reason = self.refused(want)
        if reason:
            await self.decline(want.want_id, subjects, reason, settled=False)
            return
        first, last = _window(want)
        keys = {k.value for k in want.series_keys if k.scheme == helpers.OCC_OSI}
        declined: dict[str, list[str]] = {}
        for subject in subjects:
            held = await self.underlying(subject)
            if isinstance(held, str):
                declined.setdefault(held, []).append(subject)
                continue
            chosen = [
                c
                for c in held.terms.values()
                if _within(c.expiration, first, last) and (not keys or c.osi in keys)
            ]
            if not chosen:
                declined.setdefault("not_covered", []).append(subject)
                self._forget(held)
                continue
            days = {c.expiration for c in chosen}
            if want.data_type == SERIES:
                await self.record_series(held, chosen, want.want_id)
                if want.standing:
                    held.series.add(want.want_id)
            elif want.data_type == TRADE:
                held.trades[want.want_id] = sorted(c.osi for c in chosen)
            elif want.data_type == STATISTIC:
                why = await self.read_statistics(held, days, want.want_id, want.business_date)
                if why:
                    declined.setdefault(why, []).append(subject)
                if want.standing:
                    held.statistics[want.want_id] = days
            else:
                held.pending[want.want_id] = set(days)
                if want.standing:
                    held.standing[want.want_id] = set(days)
            self._forget(held)
        for why, refused in sorted(declined.items()):
            await self.decline(want.want_id, refused, why, settled=why != "source_silent")
        if want.data_type == TRADE:
            past = self.restream()
            over = [
                s
                for s in subjects
                if any(
                    osi in past
                    for osi in self.underlyings.get(s, Underlying(s, "")).trades.get(
                        want.want_id, []
                    )
                )
            ]
            await self.decline(want.want_id, over, "not_covered", day=f"{self.lake.today()}:stream")
        await self.refresh()

    def withdraw(self, want_id: str) -> None:
        """A standing want withdrawn: its chains no longer kept current, its
        series no longer streamed for it."""
        streamed = False
        for held in list(self.underlyings.values()):
            held.standing.pop(want_id, None)
            held.pending.pop(want_id, None)
            held.statistics.pop(want_id, None)
            held.series.discard(want_id)
            streamed = held.trades.pop(want_id, None) is not None or streamed
            self._forget(held)
        if streamed:
            self.restream()

    def reset(self) -> None:
        """The plan changed: what was wanted is let go, and the lake asks
        again."""
        self.underlyings.clear()
        self.restream()

    async def decline(
        self,
        want_id: str,
        subjects: Sequence[str],
        reason: str,
        *,
        day: str = "",
        settled: bool = True,
    ) -> None:
        await self.lake._decline(OPTIONS, want_id, subjects, reason, day=day, settled=settled)

    async def underlying(self, subject: str) -> Underlying | str:
        """The underlying a subject is, its series as defined on the latest
        day the history reaches; or why its chain is not read: no listing, or
        no series defined (`not_covered`), or Databento not answering
        (`source_silent`)."""
        lake = self.lake
        source = lake.source
        symbol = await lake.symbol_of(subject)
        if symbol is None or source is None:
            return "not_covered" if symbol is None else "source_silent"
        held = self.underlyings.get(subject) or Underlying(subject, symbol)
        today = new_york_date(self.now())
        if held.listed_on != today:
            listed = await lake._fetch(lambda: source.contracts(symbol))
            if listed is None:
                return "source_silent"
            terms: dict[str, Contract] = {}
            for each in listed:
                if isinstance(each, Contract):
                    if each.expiration >= today:
                        terms[each.osi] = each
                else:
                    self.counts.unconverted += 1
            held.terms, held.listed_on = terms, today
            for days in (*held.pending.values(), *held.standing.values()):
                days.intersection_update(held.expirations)
        if not held.terms:
            return "not_covered"
        self.underlyings[subject] = held
        return held

    def _forget(self, held: Underlying) -> None:
        for want_id in [w for w, left in held.pending.items() if not left]:
            del held.pending[want_id]
        if not held.wanted:
            self.underlyings.pop(held.subject, None)

    # ── The timetable ────────────────────────────────────────────────────

    def budget(self) -> int:
        """The quote replays still allowed this minute."""
        now = self.now()
        while self.calls and self.calls[0] <= now - MINUTE_NS:
            self.calls.popleft()
        return READS_PER_MINUTE - len(self.calls)

    async def refresh(self) -> int:
        """Read what is due within the minute's budget: the quotes a want
        waits for first, then a standing want's, unread longest first; and
        each day, a standing want's series defined again, its statistics
        read again. How many chains' quotes were read."""
        if self.lake.source is None:
            return 0
        today = new_york_date(self.now())
        for held in list(self.underlyings.values()):
            if held.listed_on != today and held.wanted:
                found = await self.underlying(held.subject)
                if isinstance(found, str):
                    log.warning("%s's series not defined today: %s", held.symbol, found)
                    continue
                if found.series:
                    await self.record_series(found, list(found.terms.values()), "")
                if found.trades:
                    self.restream()
            if held.statistics and held.stats_on != today:
                days = set().union(*held.statistics.values())
                await self.read_statistics(held, days, min(held.statistics), "")
        now = self.now()
        due: list[tuple[int, int, str]] = []
        if self.timeliness() == REAL_TIME:
            for held in self.underlyings.values():
                waiting = bool(held.pending)
                stale = bool(held.standing) and now - held.quoted_at >= CADENCE_SECONDS * NS
                if waiting or stale:
                    due.append((0 if waiting else 1, held.quoted_at, held.subject))
        read = 0
        for _, _, subject in sorted(due)[: max(self.budget(), 0)]:
            wanted = self.underlyings.get(subject)
            if wanted is not None:
                await self.read_quotes(wanted)
                read += 1
        await self.alive()
        return read

    async def alive(self) -> bool:
        """The chain's sign of life since the last (contract v27, W10.4,
        ruling 418): the chain dataset's liveness, recorded once a pass for
        its consolidated view, which confirms every quote in force, an
        unchanged one never sent again. A refusal is said, never raised."""
        if self.alive_ns <= self.liveness_ns:
            return False
        self.liveness_ns = self.alive_ns
        try:
            await self.lake.plugin.record_liveness(
                liveness=[
                    ops.DatasetLiveness(
                        dataset=self.dataset(), venue_id="", alive_at_ns=self.alive_ns
                    )
                ]
            )
        except (MeridianError, ValueError) as refused:
            log.warning("the chain's liveness not recorded: %s", refused)
            return False
        return True

    async def read_quotes(self, held: Underlying) -> None:
        """An underlying's quotes read and recorded for the expirations
        wanted, against the want waiting for them, else the standing want
        keeping them current."""
        source = self.lake.source
        self.calls.append(self.now())
        listed = await self.lake._fetch(lambda: source.chain_quotes(held.symbol))
        held.quoted_at = self.now()
        waiting = sorted(held.pending)
        days = set().union(*held.pending.values(), *held.standing.values())
        held.pending.clear()
        if listed is None:
            for want_id in waiting:
                await self.decline(want_id, [held.subject], "source_silent", settled=False)
        else:
            self.read_count += 1
            self.alive_ns = held.quoted_at
            want_id = waiting[0] if waiting else min(held.standing, default="")
            await self.record_quotes(held, listed, days, want_id)
        self._forget(held)

    # ── Dates ────────────────────────────────────────────────────────────

    async def is_open(self, day: dt.date) -> bool | None:
        """Whether a day is a US equity market day by the deployment's
        calendar (ReadCalendar); None where the calendar did not answer."""
        if day in self.days:
            return self.days[day]
        span = (day - dt.timedelta(days=LOOK_BACK_DAYS + 7), day + dt.timedelta(days=14))
        if self.calendar_read == span:
            return None
        self.calendar_read = span
        try:
            answer = await self.lake.plugin.read_calendar(
                calendar_id=self.lake.calendar_id, from_date=span[0], to_date=span[1]
            )
        except (MeridianError, ValueError) as failed:
            self.calendar_error = f"the calendar was not read: {failed}"[:300]
            log.warning("%s", self.calendar_error)
            return None
        if not answer.found:
            self.calendar_error = f"the calendar {self.lake.calendar_id} was not found"
            return None
        self.calendar_error = ""
        weekends = {int(w) for weekend in answer.weekends for w in weekend.weekdays}
        closed = {c.date for c in answer.days if c.kind == ops.CALENDAR_DAY_KIND_CLOSED and c.date}
        opened = {c.date for c in answer.days if c.kind == ops.CALENDAR_DAY_KIND_OPEN and c.date}
        day_of = span[0]
        while day_of <= span[1]:
            text = day_of.isoformat()
            weekday = ops.WEEKDAY_MONDAY + day_of.weekday()
            self.days[day_of] = text in opened or (text not in closed and weekday not in weekends)
            day_of += dt.timedelta(days=1)
        return self.days.get(day)

    async def market_day(self, day: dt.date, step: int) -> dt.date | None:
        """The market day before (`step` -1) or after (+1) `day`; None where
        the calendar did not answer."""
        for n in range(1, LOOK_BACK_DAYS + 1):
            candidate = day + dt.timedelta(days=n * step)
            held = await self.is_open(candidate)
            if held is None:
                return None
            if held:
                return candidate
        return None

    # ── Recording (W10.4) ────────────────────────────────────────────────

    def _changed(self, data_type: str, dataset: str, row_key: str, said: str) -> bool:
        """Whether a row says something other than its last sending."""
        mark = (data_type, dataset, row_key)
        if self.sent.get(mark) == said:
            return False
        self.sent[mark] = said
        self.sent.move_to_end(mark)
        while len(self.sent) > REMEMBERED:
            self.sent.popitem(last=False)
        return True

    def series_row(self, held: Underlying, contract: Contract, dataset: str) -> Any | None:
        raw = self.lake.plugin.raw_record(contract.raw_key[:256])
        try:
            return helpers.option_series(
                held.subject,
                contract.osi,
                template_id=TEMPLATE_ID,
                template_version=TEMPLATE_VERSION,
                dataset=dataset,
                currency=CURRENCY,
                exercise_style=contract.exercise_style,
                settlement_method=contract.settlement_method,
                contract_multiplier=contract.multiplier,
                deliverable=[(held.subject, STANDARD_SHARES)] if contract.standard else (),
                valid_from_ns=self.now(),
                raw=raw,
            )
        except ValueError:
            self.counts.unconverted += 1
            return None

    async def record_series(
        self, held: Underlying, chosen: Sequence[Contract], want_id: str
    ) -> None:
        """The terms of the series chosen, each sent only where changed."""
        dataset = self.dataset()
        rows: list[Any] = []
        for contract in chosen:
            row = self.series_row(held, contract, dataset)
            if row is None:
                continue
            terms = repr([(f.path, f.value) for f in row.fields])
            if self._changed(SERIES, dataset, contract.osi, terms):
                rows.append(row)
        if rows:
            await self.send("series", rows, want_id)

    async def read_statistics(
        self, held: Underlying, days: set[dt.date], want_id: str, business_date: str = ""
    ) -> str:
        """The open interest and volume of the series expiring on `days`, as
        OPRA published them for `business_date` (the latest, where none),
        each dated the market day it is for and sent only where it changed.
        Why none was recorded, or ""."""
        source = self.lake.source
        on: dt.date | None = None
        if business_date:
            on = await self.market_day(dt.date.fromisoformat(business_date), +1)
            if on is None:
                return "beyond_history"
        published = on
        listed = await self.lake._fetch(lambda: source.statistics(held.symbol, published))
        if listed is None:
            return "source_silent"
        if not business_date:
            held.stats_on = new_york_date(self.now())
        dataset = self.dataset()
        rows: list[Any] = []
        dated: dict[dt.date, dt.date | None] = {}
        for each in listed:
            if not isinstance(each, ChainStatistic):
                self.counts.unconverted += 1
                continue
            contract = held.terms.get(each.osi)
            if contract is None or contract.expiration not in days:
                continue
            publish_day = new_york_date(each.published_ns)
            if publish_day not in dated:
                dated[publish_day] = await self.market_day(publish_day, -1)
            day = dated[publish_day]
            if day is None or (business_date and day.isoformat() != business_date):
                continue
            stat = helpers.statistic(
                held.subject,
                helpers.osi_key(each.osi),
                each.kind,
                each.value,
                day,
                dataset=dataset,
                valid_from_ns=each.published_ns,
                raw=self.lake.plugin.raw_record(each.raw_key[:256]),
            )
            assert stat.meta is not None
            changed = self._changed(STATISTIC, dataset, stat.meta.row_key, str(each.value))
            if changed or want_id:
                # A reader waiting is answered, an unchanged row kept so.
                rows.append(stat)
        if rows:
            await self.send("statistics", rows, want_id)
            return ""
        if None in dated.values():
            return "source_silent"
        return "beyond_history" if business_date else ""

    async def record_quotes(
        self, held: Underlying, listed: Sequence[SourceQuote], days: set[dt.date], want_id: str
    ) -> None:
        """The quotes of the series expiring on `days`, each sent only where
        it changed, judged at the edge."""
        dataset = self.dataset()
        quotes: list[Any] = []
        for option in listed:
            self.counts.unconverted += len(option.unconverted)
            contract = held.terms.get(option.osi)
            if contract is None or (days and contract.expiration not in days):
                continue
            made = self.quote(held.subject, option, dataset)
            if made is None:
                continue
            said = repr((made.bid, made.ask, made.bid_quantity, made.ask_quantity))
            if self._changed(QUOTE, dataset, option.osi, said):
                quotes.append(made)
        if quotes:
            await self.send("quotes", self.lake.at_the_edge(OPTIONS, quotes, want_id), want_id)

    def quote(self, subject: str, option: SourceQuote, dataset: str) -> Quote | None:
        """A series' best bid and offer under the underlying, keyed by the
        series; none where Databento states no side."""
        if option.bid is None and option.ask is None:
            return None
        return Quote(
            meta=helpers.series_meta(
                subject,
                helpers.osi_key(option.osi),
                row_key=f"{option.osi}:q:{option.at_ns}",
                dataset=dataset,
                valid_from_ns=option.at_ns,
                raw=self.lake.plugin.raw_record(option.raw_key[:256]),
                source_times=[SourceTime(kind="event", at_ns=option.at_ns)],
            ),
            bid=None if option.bid is None else Money(option.bid, CURRENCY),
            ask=None if option.ask is None else Money(option.ask, CURRENCY),
            bid_quantity=option.bid_size if option.bid is not None else None,
            ask_quantity=option.ask_size if option.ask is not None else None,
        )

    async def send(self, kind: str, rows: Sequence[Any], want_id: str) -> None:
        """Rows in the lake's batches, against the want, counted."""
        call = getattr(self.lake.plugin, f"record_{kind}")
        counts = self.counts
        for start in range(0, len(rows), BATCH):
            done = await call(**{kind: list(rows[start : start + BATCH])}, want_id=want_id)
            counts.recorded += int(getattr(done, "recorded", 0))
            counts.restated += int(getattr(done, "restated", 0))
            counts.unchanged += int(getattr(done, "unchanged", 0))
            counts.last_recorded_at_ns = self.now()
            if want_id:
                held = self.lake.by_want.setdefault(want_id, [0, 0])
                held[0] += int(getattr(done, "recorded", 0)) + int(getattr(done, "restated", 0))

    # ── The options stream: trades ───────────────────────────────────────

    def restream(self) -> set[str]:
        """Stream the series under the standing wants of trades, in the order
        they were wanted, as many as one session may; those past that."""
        ordered: list[str] = []
        if self.timeliness() == REAL_TIME:
            for held in self.underlyings.values():
                for osis in held.trades.values():
                    for osi in osis:
                        if osi not in ordered:
                            ordered.append(osi)
        accepted = ordered[:STREAM_LIMIT]
        self.streamed = accepted
        self.past_limit = set(ordered) - set(accepted)
        if self.streamer is not None:
            self.streamer.want(accepted)
        return self.past_limit

    def underlying_of(self, osi: str) -> list[Underlying]:
        """The underlyings whose standing wants stream a series."""
        return [
            held
            for held in self.underlyings.values()
            if any(osi in osis for osis in held.trades.values())
        ]

    async def heard(self, stated: Sequence[Any]) -> None:
        """What the options stream stated: each trade of a series streamed
        made a row, waiting to be recorded."""
        self.alive_ns = self.now()
        streamed = set(self.streamed)
        for each in stated:
            if not isinstance(each, SourceTrade) or each.osi not in streamed:
                continue
            for held in self.underlying_of(each.osi):
                row = self.trade_row(held.subject, each)
                if row is not None:
                    self.pending_trades.append(row)

    def trade_row(self, subject: str, stated: SourceTrade) -> Trade | None:
        """An option's trade under its underlying, keyed by its series: its
        venue as Databento's publisher, by its MIC, kept as reported; no
        condition and no aggressor."""
        if stated.price is None or stated.quantity is None:
            self.counts.unconverted += len(stated.unconverted) or 1
            return None
        meta = helpers.series_meta(
            subject,
            helpers.osi_key(stated.osi),
            row_key=f"{stated.row_key}@{subject}",
            dataset=self.dataset(),
            valid_from_ns=stated.at_ns,
            raw=self.lake.plugin.raw_record(stated.raw_key[:256]),
            source_times=[SourceTime(kind="event", at_ns=stated.at_ns)],
        )
        reported = [as_reported(f"{SOURCE}:venue", stated.venue_code)] if stated.venue_code else []
        meta = replace(meta, unconverted=[*reported, *stated.unconverted])
        return Trade(
            meta=meta,
            price=Money(stated.price, CURRENCY),
            quantity=stated.quantity,
            aggressor=stated.aggressor or None,
        )

    async def flush(self, force: bool = False) -> None:
        """Record the option trades heard, every half second or 500 at a
        time, none twice; and the dataset's liveness."""
        await self.alive()
        now = self.now()
        due = force or len(self.pending_trades) >= BATCH or now - self._trades_at >= TRADE_EVERY_NS
        if not self.pending_trades or not due:
            return
        rows, self.pending_trades = self.pending_trades, []
        self._trades_at = now
        fresh: list[Trade] = []
        for row in rows:
            assert row.meta is not None
            if row.meta.row_key in self.recent:
                self.counts.unchanged += 1
                continue
            self.recent[row.meta.row_key] = None
            fresh.append(row)
        while len(self.recent) > RECENT_TRADES:
            self.recent.popitem(last=False)
        if fresh:
            await self.send("trades", self.lake.at_the_edge(OPTIONS, fresh, ""), "")


__all__ = [
    "CADENCE_SECONDS", "DATA_TYPES", "NEW_YORK", "OPTIONS", "QUOTE", "SERIES", "STATISTIC",
    "STREAM_LIMIT", "TRADE", "Chains", "SourceQuote", "Underlying",
]  # fmt: skip
