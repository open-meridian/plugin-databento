"""Databento's records, converted to what the plugin records.

The edge is the only place Databento's vocabulary is read. Each converter
takes records as Databento's JSON encoding states them -- a header `hd`
(`ts_event`, `rtype`, `publisher_id`, `instrument_id`), then the schema's
fields; prices as decimal strings (`pretty_px`), times as UNIX nanoseconds
-- with the symbol each is of (the historical API's `symbol`, or the live
gateway's mapping) and the raw record it was kept in, and returns what is
stated, named by the listing's one spelling (`BRK.B`, Databento's own in
its US equity datasets, which follow Nasdaq's convention) or an option's
OCC OSI symbol (OPRA's raw symbol, 21 characters).

- **The daily close** (`EQUS.SUMMARY`, `ohlcv-1d`): Nasdaq's NLS Plus
  end-of-day summary, consolidated across the US exchanges, one bar a date,
  its date the UTC date its `ts_event` opens. No VWAP and no trade count:
  Databento's bar states neither.
- **Live prices and quotes** (`EQUS.MINI`, `bbo-1m` and `bbo-1s`): the last
  sale (`price`, at its `ts_event`) and the best bid and offer across the
  venues Databento aggregates (`levels[0]`, at the interval's `ts_recv`),
  sizes in shares. Not the SIP's NBBO: Databento's own aggregate of the
  venues' feeds. No venue: Databento anonymizes the dataset's venues.
- **Trades** (`trades`): price, size, both clocks (`ts_event`, the matching
  engine's; `ts_recv`, Databento's capture), the side where a venue states
  one, kept as reported (the SDK's listings feed records no aggressor). A
  stock trade's row is keyed by its listing, its two clocks and its place
  among trades stated with the same two (`Numbering`), the same from the
  stream, a replay and the history, which Databento states identically; an
  option's by its series, the same way. OPRA's prints name their venue by
  Databento's publisher ID, kept as reported by its MIC.
- **Options** (`OPRA.PILLAR`): a series' terms from its definition
  (`definition`): put or call (`instrument_class`), strike, expiration (a
  UTC date), its multiplier (`unit_of_measure_qty`), and its style and
  settlement from its ISO 10962 CFI code where the definition states one;
  its open interest (`statistics`, `stat_type` 9) and cleared volume (6);
  its consolidated best bid and offer (`cbbo-1m`).
- **Exact.** A value that is not a number, or past the contract's Decimal
  (18 places), does not convert: the row is not recorded, and Databento's
  text is kept as reported (`databento:trades.price`) for the Datasets page
  to count. Databento's null sentinels (the largest value of a field's
  type) are no value.
"""

from __future__ import annotations

import datetime as dt
import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from meridian.edge import as_reported
from meridian.plugin.v1.operations_pb2 import AsReported
from meridian.roles import chains as helpers

NS = 1_000_000_000
SOURCE = "databento"
NEW_YORK = ZoneInfo("America/New_York")
#: The session's close, the daily dataset's day end: 16:00 New York.
DAY_END_MINUTE = 16 * 60
MOST_PLACES = 18
MOST_DIGITS = 38

#: Databento's null sentinels: a field's type's largest value.
UNDEF_PRICE = 2**63 - 1
UNDEF_TIMESTAMP = 2**64 - 1
UNDEF_QUANTITY = 2**63 - 1
UNDEF_STAT_QUANTITY = 2**31 - 1
#: `UNDEF_PRICE` as `pretty_px` writes it.
UNDEF_PRICE_TEXT = "9223372036.854775807"

#: A US listing in the one spelling: a root of capitals (digits after the
#: first), and a class or series after a dot.
SYMBOL = re.compile(r"[A-Z][A-Z0-9]{0,5}(?:\.[A-Z0-9]{1,4})?")

#: Databento's `side` of a trade, the side that initiated it, as the
#: aggressor: `B` a buyer took liquidity, `A` a seller; `N` none stated.
AGGRESSORS = {"B": "buy", "A": "sell"}

#: Statistics read of OPRA's (Databento's StatType).
OPEN_INTEREST, CLEARED_VOLUME = 9, 6
STATISTICS = {OPEN_INTEREST: "open_interest", CLEARED_VOLUME: "volume"}

#: OPRA's venues, by Databento's publisher ID, with their MICs: as
#: databento.com/docs/venues-and-datasets/opra-pillar lists them.
OPRA_VENUES = {
    20: "AMXO", 21: "XBOX", 22: "XCBO", 23: "EMLD", 24: "EDGO", 25: "GMNI", 26: "XISX",
    27: "MCRY", 28: "XMIO", 29: "ARCO", 30: "OPRA", 31: "MPRL", 32: "XNDQ", 33: "XBXO",
    34: "C2OX", 35: "XPHL", 36: "BATO", 37: "MXOP", 61: "SPHR", 108: "MXTO", 109: "IEXO",
}  # fmt: skip


def spelled(symbol: str) -> str | None:
    """The listing in the one spelling this plugin names it by (`BRK.B`), from
    a symbol in any of the usual spellings, or None where it is not one."""
    candidate = symbol.strip().upper().replace("/", ".").replace("-", ".")
    return candidate if SYMBOL.fullmatch(candidate) else None


def option_root(symbol: str) -> str:
    """The OCC root of a listing's standard options, Databento's parent
    symbol before `.OPT`: the listing with its class's dot dropped (BRK.B's
    options are `BRKB`)."""
    return symbol.replace(".", "")


def osi_of(raw: str) -> str | None:
    """The OCC OSI symbol OPRA names a series by (21 characters), or None."""
    text = raw.rstrip("\n")
    if len(text) != 21:
        return None
    try:
        helpers.osi_terms(text)
    except ValueError:
        return None
    return text


# ── Days ────────────────────────────────────────────────────────────────────


def new_york_date(at_ns: int) -> dt.date:
    return dt.datetime.fromtimestamp(at_ns // NS, NEW_YORK).date()


def utc_date(at_ns: int) -> dt.date:
    return dt.datetime.fromtimestamp(at_ns // NS, dt.UTC).date()


def utc_midnight(day: dt.date) -> int:
    return int(dt.datetime(day.year, day.month, day.day, tzinfo=dt.UTC).timestamp()) * NS


# ── Values ──────────────────────────────────────────────────────────────────


def exact(value: object) -> Decimal | None:
    """A number exactly as Databento stated it, or None where it is not one
    the contract carries: not a number, a null sentinel, not finite, or past
    18 places."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        number = Decimal(value)
    elif isinstance(value, Decimal):
        number = value
    elif isinstance(value, str) and re.fullmatch(r"-?\d+(\.\d+)?", value.strip()):
        text = value.strip()
        if text == UNDEF_PRICE_TEXT:
            return None
        if "." in text:
            # `pretty_px` writes every price to nine places: the zeros past
            # the last digit are its format's, not the price's.
            text = text.rstrip("0").rstrip(".")
        number = Decimal(text)
    else:
        return None
    if not number.is_finite():
        return None
    _, digits, exponent = number.as_tuple()
    if not isinstance(exponent, int) or -exponent > MOST_PLACES:
        return None
    if len(digits) + max(exponent, 0) > MOST_DIGITS:
        return None
    return number


def whole(value: object) -> int | None:
    """A whole number Databento stated (a time, a size, a count, an ID), as a
    JSON number or as text, or None: not whole, or a null sentinel."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        held = value
    elif isinstance(value, Decimal) and value == value.to_integral_value():
        held = int(value)
    elif isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        held = int(value.strip())
    else:
        return None
    if held in (UNDEF_TIMESTAMP, UNDEF_QUANTITY, UNDEF_STAT_QUANTITY, 2**32 - 1):
        return None
    return held


def header(said: dict[str, Any]) -> dict[str, Any]:
    held = said.get("hd")
    return held if isinstance(held, dict) else {}


def ts_event(said: dict[str, Any]) -> int | None:
    held = whole(header(said).get("ts_event"))
    return held if held and held > 0 else None


def ts_recv(said: dict[str, Any]) -> int | None:
    held = whole(said.get("ts_recv"))
    return held if held and held > 0 else None


def publisher(said: dict[str, Any]) -> int | None:
    return whole(header(said).get("publisher_id"))


def top(said: dict[str, Any]) -> dict[str, Any]:
    """The top of book of an MBP-1, BBO or CBBO record: `levels[0]` in
    Databento's JSON, or the flat `_00` fields as its CSV names them."""
    levels = said.get("levels")
    if isinstance(levels, list) and levels and isinstance(levels[0], dict):
        return levels[0]
    return {
        name: said.get(f"{name}_00")
        for name in ("bid_px", "ask_px", "bid_sz", "ask_sz", "bid_pb", "ask_pb")
        if f"{name}_00" in said
    }


@dataclass
class _Values:
    endpoint: str
    unconverted: list[AsReported] = field(default_factory=list)

    def number(self, name: str, value: object, *, positive: bool = False) -> Decimal | None:
        converted = exact(value)
        if converted is None or (positive and converted <= 0) or converted < 0:
            if value is not None and str(value) != UNDEF_PRICE_TEXT:
                self.unconverted.append(
                    as_reported(f"{SOURCE}:{self.endpoint}.{name}", str(value)[:64])
                )
            return None
        return converted

    def keep(self, name: str, value: object) -> None:
        """Something Databento stated that is kept as reported, not converted."""
        self.unconverted.append(as_reported(f"{SOURCE}:{self.endpoint}.{name}", str(value)[:64]))


# ── Daily bars ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DailyBar:
    """One listing's day, as Databento's `ohlcv-1d` states it."""

    symbol: str
    business_date: dt.date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    volume: Decimal | None
    raw_key: str
    unconverted: tuple[AsReported, ...] = ()
    vwap: Decimal | None = None
    trade_count: int | None = None

    @property
    def row_key(self) -> str:
        return f"{self.symbol}:1d:{self.business_date.isoformat()}"

    @property
    def complete(self) -> bool:
        return self.close is not None and None not in (self.open, self.high, self.low)


def daily(records: Iterable[tuple[str, dict[str, Any]]], raw_key: str) -> Iterator[DailyBar]:
    """Each day of `ohlcv-1d` records, named by the listing's one spelling."""
    for index, (symbol, said) in enumerate(records):
        listing = spelled(symbol)
        start = ts_event(said)
        if listing is None or start is None:
            continue
        values = _Values("ohlcv-1d")
        volume = whole(said.get("volume"))
        if volume is None and said.get("volume") is not None:
            values.keep("volume", said.get("volume"))
        yield DailyBar(
            symbol=listing,
            business_date=utc_date(start),
            open=values.number("open", said.get("open"), positive=True),
            high=values.number("high", said.get("high"), positive=True),
            low=values.number("low", said.get("low"), positive=True),
            close=values.number("close", said.get("close"), positive=True),
            volume=Decimal(volume) if volume is not None and volume >= 0 else None,
            raw_key=f"{raw_key}#{index}",
            unconverted=tuple(values.unconverted),
        )


# ── Live prices and quotes ──────────────────────────────────────────────────


@dataclass(frozen=True)
class Quoted:
    """One live price: the last sale's (`last`), or a quote's side."""

    symbol: str
    kind: str
    amount: Decimal | None
    at_ns: int
    venue_code: str
    raw_key: str
    unconverted: tuple[AsReported, ...] = ()

    @property
    def row_key(self) -> str:
        return f"{self.symbol}:{self.kind}:{self.at_ns}"


@dataclass(frozen=True)
class SourceQuote:
    """A best bid and offer, sizes in shares (a listing's) or contracts (an
    option series', with `osi`)."""

    symbol: str
    at_ns: int
    bid: Decimal | None
    ask: Decimal | None
    bid_size: Decimal | None
    ask_size: Decimal | None
    raw_key: str
    unconverted: tuple[AsReported, ...] = ()
    venue_code: str = ""
    osi: str = ""
    event_ns: int | None = None

    @property
    def row_key(self) -> str:
        return f"{self.osi or self.symbol}:q:{self.at_ns}"


def _side(values: _Values, price: object, size: object, name: str) -> tuple[Any, Any]:
    """One side of a quote: empty where Databento states no price."""
    amount = exact(price)
    if amount is None or amount == 0:
        return None, None
    converted = values.number(f"{name}_px", price, positive=True)
    held = whole(size)
    return converted, (Decimal(held) if held is not None and held >= 0 else None)


def quote(
    symbol: str, said: dict[str, Any], raw_key: str, endpoint: str, osi: str = ""
) -> SourceQuote | None:
    """A BBO, CBBO or MBP-1 record's best bid and offer, at its `ts_recv`
    (the interval's end for BBO and CBBO), or its `ts_event` where it states
    no `ts_recv`."""
    at = ts_recv(said) or ts_event(said)
    if at is None:
        return None
    values = _Values(endpoint)
    best = top(said)
    bid, bid_size = _side(values, best.get("bid_px"), best.get("bid_sz"), "bid")
    ask, ask_size = _side(values, best.get("ask_px"), best.get("ask_sz"), "ask")
    return SourceQuote(
        symbol=symbol,
        at_ns=at,
        bid=bid,
        ask=ask,
        bid_size=bid_size,
        ask_size=ask_size,
        raw_key=raw_key,
        unconverted=tuple(values.unconverted),
        osi=osi,
        event_ns=ts_event(said),
    )


def bbo_live(symbol: str, said: dict[str, Any], raw_key: str) -> list[Quoted]:
    """`last`, `bid` and `ask` of a listing's BBO record: the last sale at
    its own time, the sides at the interval's end. A side with no price is
    not sent."""
    listing = spelled(symbol)
    if listing is None:
        return []
    out: list[Quoted] = []
    sale_at = ts_event(said)
    if sale_at is not None and exact(said.get("price")) not in (None, Decimal(0)):
        values = _Values("bbo")
        out.append(
            Quoted(listing, "last", values.number("price", said.get("price"), positive=True),
                   sale_at, "", f"{raw_key}/last", tuple(values.unconverted))
        )  # fmt: skip
    best = top(said)
    at = ts_recv(said)
    if at is not None:
        for kind, name in (("bid", "bid_px"), ("ask", "ask_px")):
            if exact(best.get(name)) in (None, Decimal(0)):
                continue
            values = _Values("bbo")
            out.append(
                Quoted(listing, kind, values.number(name, best.get(name), positive=True), at, "",
                       f"{raw_key}/{kind}", tuple(values.unconverted))
            )  # fmt: skip
    return out


def latest(records: Iterable[tuple[str, dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    """Each symbol's latest record, by its `ts_recv`."""
    out: dict[str, dict[str, Any]] = {}
    for symbol, said in records:
        held = out.get(symbol)
        if held is None or (ts_recv(said) or 0) >= (ts_recv(held) or 0):
            out[symbol] = said
    return out


# ── Trades ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SourceTrade:
    """One trade, as Databento states it: a listing's, or (with `osi`) an
    option series'."""

    symbol: str
    at_ns: int
    received_ns: int
    venue_code: str
    price: Decimal | None
    quantity: Decimal | None
    place: int
    raw_key: str
    unconverted: tuple[AsReported, ...] = ()
    sequence: int | None = None
    cancelled: bool = False
    osi: str = ""
    #: The side that took liquidity, where Databento states one (`buy` for
    #: its `B`, `sell` for `A`); never inferred.
    aggressor: str = ""

    @property
    def row_key(self) -> str:
        said = f"{self.osi or self.symbol}:t:{self.received_ns}:{self.at_ns}"
        return said if not self.place else f"{said}:{self.place}"


class Numbering:
    """Each trade's place among those a symbol states with the same two
    clocks, so two prints in one packet are two rows, numbered alike by the
    stream, a replay and the history: a symbol's last two clocks and how many
    were heard with them, held across a stream's batches."""

    def __init__(self) -> None:
        self.last: dict[str, tuple[int, int, int]] = {}

    def place(self, symbol: str, received_ns: int, at_ns: int) -> int:
        held = self.last.get(symbol)
        n = held[2] + 1 if held is not None and held[:2] == (received_ns, at_ns) else 0
        self.last[symbol] = (received_ns, at_ns, n)
        return n


def trade(
    symbol: str,
    said: dict[str, Any],
    raw_key: str,
    numbering: Numbering,
    endpoint: str = "trades",
    osi: str = "",
) -> SourceTrade | None:
    """A `trades` record: its price, size, two clocks and place; an option's
    venue as Databento's publisher, by its MIC; a stated side as reported."""
    at = ts_event(said)
    received = ts_recv(said) or at
    if at is None or received is None:
        return None
    values = _Values(endpoint)
    size = whole(said.get("size"))
    if size is None or size <= 0:
        values.keep("size", said.get("size"))
    side = str(said.get("side") or "N")
    venue = ""
    if osi:
        code = publisher(said)
        venue = OPRA_VENUES.get(code, str(code)) if code is not None else ""
    sequence = whole(said.get("sequence"))
    return SourceTrade(
        symbol=symbol,
        at_ns=at,
        received_ns=received,
        venue_code=venue,
        price=values.number("price", said.get("price"), positive=True),
        quantity=Decimal(size) if size is not None and size > 0 else None,
        place=numbering.place(osi or symbol, received, at),
        raw_key=raw_key,
        unconverted=tuple(values.unconverted),
        sequence=sequence if sequence else None,
        osi=osi,
        aggressor=AGGRESSORS.get(side, ""),
    )


def trades(
    records: Iterable[tuple[str, dict[str, Any]]], raw_key: str, numbering: Numbering
) -> Iterator[SourceTrade]:
    """Each listing's trade of a history's or a replay's records."""
    for index, (symbol, said) in enumerate(records):
        listing = spelled(symbol)
        if listing is None:
            continue
        made = trade(listing, said, f"{raw_key}#{index}", numbering)
        if made is not None:
            yield made


# ── Options ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Contract:
    """One option series' terms, as its OPRA definition states them."""

    osi: str
    underlying: str
    put_or_call: str
    exercise_style: str
    settlement_method: str
    expiration: dt.date
    strike: Decimal
    multiplier: Decimal
    standard: bool
    raw_key: str
    unconverted: tuple[AsReported, ...] = ()


#: An option's ISO 10962 CFI code: `O`, then call or put, its exercise style
#: (third letter) and its delivery (fifth letter).
STYLES = {"A": "EXERCISE_STYLE_AMERICAN", "E": "EXERCISE_STYLE_EUROPEAN"}
DELIVERY = {"P": "SETTLEMENT_METHOD_PHYSICAL", "C": "SETTLEMENT_METHOD_CASH"}
RIGHTS = {"C": "call", "P": "put"}
#: What one standard equity contract delivers: 100 shares of its underlying.
STANDARD_SHARES = 100


def contracts(
    records: Iterable[tuple[str, dict[str, Any]]], raw_key: str
) -> Iterator[Contract | AsReported]:
    """Each series of `definition` records, the latest definition of each;
    what did not convert as reported (an `AsReported`) for the Datasets page
    to count: a raw symbol that is no OSI symbol, a CFI code that does not
    state an option's style and delivery, a strike or multiplier that is no
    number. A deleted definition is left out."""
    latest_of: dict[str, tuple[int, dict[str, Any]]] = {}
    for index, (symbol, said) in enumerate(records):
        raw = str(said.get("raw_symbol") or symbol)
        held = latest_of.get(raw)
        if held is None or (ts_recv(said) or 0) >= (ts_recv(held[1]) or 0):
            latest_of[raw] = (index, said)
    for raw, (index, said) in sorted(latest_of.items()):
        if str(said.get("security_update_action") or "A") == "D":
            continue
        osi = osi_of(raw)
        if osi is None:
            yield as_reported(f"{SOURCE}:definition.raw_symbol", raw[:64])
            continue
        _, expiration, right, strike = helpers.osi_terms(osi)
        cfi = str(said.get("cfi") or "")
        style = STYLES.get(cfi[2]) if len(cfi) == 6 and cfi[0] == "O" else None
        delivery = DELIVERY.get(cfi[4]) if len(cfi) == 6 and cfi[0] == "O" else None
        if style is None or delivery is None:
            yield as_reported(f"{SOURCE}:definition.cfi", f"{osi.strip()} {cfi}"[:64])
            continue
        multiplier = exact(said.get("unit_of_measure_qty"))
        if multiplier is None or multiplier <= 0:
            yield as_reported(
                f"{SOURCE}:definition.unit_of_measure_qty", str(said.get("unit_of_measure_qty"))
            )
            continue
        underlying = str(said.get("underlying") or "").strip()
        root = osi[:6].rstrip()
        standard = (
            delivery == "SETTLEMENT_METHOD_PHYSICAL"
            and multiplier == STANDARD_SHARES
            and bool(underlying)
            and root == option_root(underlying)
        )
        yield Contract(
            osi=osi,
            underlying=underlying,
            put_or_call=RIGHTS[right],
            exercise_style=style,
            settlement_method=delivery,
            expiration=expiration,
            strike=strike,
            multiplier=multiplier.normalize(),
            standard=standard,
            raw_key=f"{raw_key}#{index}",
        )


@dataclass(frozen=True)
class ChainStatistic:
    """One series' statistic as OPRA published it: its kind (open interest,
    or the cleared volume), its value in contracts, and when it was
    published."""

    osi: str
    kind: str
    value: Decimal
    published_ns: int
    raw_key: str


def statistics(
    records: Iterable[tuple[str, dict[str, Any]]], raw_key: str
) -> Iterator[ChainStatistic | AsReported]:
    """Each series' open interest and cleared volume of `statistics`
    records; a deleted statistic left out, a quantity that is no number
    kept as reported."""
    for index, (symbol, said) in enumerate(records):
        kind = STATISTICS.get(whole(said.get("stat_type")) or -1)
        osi = osi_of(symbol)
        if kind is None or osi is None:
            continue
        if whole(said.get("update_action")) == 2:
            continue
        at = ts_recv(said) or ts_event(said)
        quantity = whole(said.get("quantity"))
        if at is None or quantity is None or quantity < 0:
            yield as_reported(f"{SOURCE}:statistics.quantity", str(said.get("quantity"))[:64])
            continue
        yield ChainStatistic(osi, kind, Decimal(quantity), at, f"{raw_key}#{index}")


def option_quotes(
    records: Iterable[tuple[str, dict[str, Any]]], raw_key: str
) -> Iterator[SourceQuote]:
    """Each series' latest consolidated best bid and offer of `cbbo` records."""
    for index, (symbol, said) in enumerate(latest(records).items()):
        osi = osi_of(symbol)
        if osi is None:
            continue
        made = quote(osi, said, f"{raw_key}#{index}", "cbbo", osi=osi)
        if made is not None:
            yield made
