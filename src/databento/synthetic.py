"""Synthetic Databento: answers the plugin's calls from the built-in exchange,
with no key and no network (the `sample_data` setting, for development, the
tests and the e2e).

`synthetic/exchange.json` is shaped on Databento's documented records with
every value invented (real records stay in a developer's own storage, never
committed): `EQUS.SUMMARY` daily bars for SPY, BRK.B and TQQQ over
2026-10-05 to 2026-10-09, TQQQ's last close to nine places; `EQUS.MINI`'s
latest BBO of each, BRK.B's offer side empty; SPY's trades a minute before
now, two of them stated with the same clocks; SPY's option series --
generated for the first two Fridays after the day asked, so the chain is
always current, with an adjusted root and one series whose definition
states no CFI -- their open interest, volume and consolidated quotes; and
the stream's records, stamped with the moment they are sent.

Two halves, as Databento has two services: `SyntheticTransport`, the
historical API (vendor.Transport), and `SyntheticGateway`, the live
gateway's Raw API, served blocking (`connect`, for gateway.replay) and on
asyncio (`connect_async`, for gateway.Session). Each record is written as
Databento's JSON encoding writes it: its header `hd`, its times as
nanosecond strings, its prices as decimal strings.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
import urllib.parse
from collections.abc import Callable, Mapping
from decimal import Decimal
from importlib import resources
from pathlib import Path
from typing import Any

from . import gateway
from .convert import NEW_YORK, UNDEF_PRICE_TEXT, utc_midnight
from .vendor import EQUS_MINI, EQUS_SUMMARY, OPRA, Response

NS = 1_000_000_000
MS = 1_000_000
DAY_NS = 86_400 * NS
#: Databento's publisher IDs for the synthetic datasets (invented here).
PUBLISHERS = {EQUS_SUMMARY: 90, EQUS_MINI: 91, OPRA: 30}
RTYPES = {"trades": 0, "bbo-1s": 195, "bbo-1m": 196, "cbbo-1m": 193, "ohlcv-1d": 35,
          "definition": 19, "statistics": 24}  # fmt: skip


def exchange(path: Path | None = None) -> dict[str, Any]:
    """The built-in exchange, or the one at `path` (a developer's recording)."""
    if path is not None:
        loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
        return loaded
    text = resources.files("databento").joinpath("synthetic/exchange.json").read_text("utf-8")
    built: dict[str, Any] = json.loads(text)
    return built


def fridays(today: dt.date) -> list[dt.date]:
    """The first two Fridays after `today`: the synthetic chain's expirations."""
    ahead = (4 - today.weekday()) % 7 or 7
    first = today + dt.timedelta(days=ahead)
    return [first, first + dt.timedelta(days=7)]


def osi(root: str, expiry: dt.date, right: str, strike: str) -> str:
    thousandths = int(Decimal(strike) * 1000)
    return f"{root:<6}{expiry:%y%m%d}{right[0].upper()}{thousandths:08d}"


def _hd(ts_event: int, schema: str, dataset: str, instrument: int) -> dict[str, Any]:
    return {
        "ts_event": str(ts_event),
        "rtype": RTYPES[schema],
        "publisher_id": PUBLISHERS[dataset],
        "instrument_id": instrument,
    }


def _px(value: object) -> object:
    return UNDEF_PRICE_TEXT if value is None else value


class Exchange:
    """The built-in exchange's records, in Databento's JSON shapes, as of
    the clock."""

    def __init__(self, held: Mapping[str, Any], clock_ns: Callable[[], int]) -> None:
        self.held, self.clock_ns = held, clock_ns

    def today(self) -> dt.date:
        return dt.datetime.fromtimestamp(self.clock_ns() / NS, NEW_YORK).date()

    def instrument(self, symbol: str) -> int:
        held: dict[str, int] = self.held.get("instruments", {})
        return held.get(symbol) or 100_000 + sum(ord(c) * (n + 1) for n, c in enumerate(symbol))

    # ── Stocks ──────────────────────────────────────────────────────────

    def daily(self, symbol: str, start: int, end: int) -> list[dict[str, Any]]:
        out = []
        for day in self.held["daily"].get(symbol, []):
            at = utc_midnight(dt.date.fromisoformat(day["date"]))
            if start <= at < end:
                out.append(
                    {"hd": _hd(at, "ohlcv-1d", EQUS_SUMMARY, self.instrument(symbol))}
                    | {k: day[k] for k in ("open", "high", "low", "close", "volume")}
                )
        return out

    def bbo(self, symbol: str, schema: str, said: Mapping[str, Any], at: int) -> dict[str, Any]:
        sale = at + int(said.get("sale_offset_ms", 0)) * MS - int(said.get("offset_ms", 0)) * MS
        return {
            "ts_recv": str(at),
            "hd": _hd(sale, schema, EQUS_MINI, self.instrument(symbol)),
            "side": said.get("side", "N"),
            "price": _px(said.get("price")),
            "size": said.get("size", 0),
            "flags": 0,
            "sequence": 0,
            "levels": [
                {
                    "bid_px": _px(said.get("bid_px")),
                    "ask_px": _px(said.get("ask_px")),
                    "bid_sz": said.get("bid_sz", 0),
                    "ask_sz": said.get("ask_sz", 0),
                    "bid_ct": 0,
                    "ask_ct": 0,
                }
            ],
        }

    def latest_bbo(self, symbol: str) -> dict[str, Any] | None:
        said = self.held["bbo"].get(symbol)
        if said is None:
            return None
        at = self.clock_ns() + int(said["offset_ms"]) * MS
        return self.bbo(symbol, "bbo-1m", said, at)

    def trade(
        self, symbol: str, said: Mapping[str, Any], at: int, dataset: str = EQUS_MINI,
        osi_symbol: str = "",
    ) -> dict[str, Any]:  # fmt: skip
        publisher = int(said.get("publisher_id", PUBLISHERS[dataset]))
        hd = _hd(at, "trades", dataset, self.instrument(osi_symbol or symbol))
        hd["publisher_id"] = publisher
        return {
            "ts_recv": str(at + 210_000),
            "hd": hd,
            "action": "T",
            "side": said.get("side", "N"),
            "depth": 0,
            "price": said["price"],
            "size": said["size"],
            "flags": 0,
            "ts_in_delta": 0,
            "sequence": 0,
        }

    def trades(self, symbol: str, start: int, end: int) -> list[dict[str, Any]]:
        base = (self.clock_ns() // MS) * MS
        out = []
        for said in self.held["trades"].get(symbol, []):
            at = base + int(said["offset_ms"]) * MS
            if start <= at + 210_000 < end:
                out.append(self.trade(symbol, said, at))
        return out

    # ── Options ─────────────────────────────────────────────────────────

    def series(self, root: str) -> list[dict[str, Any]]:
        held = self.held["options"].get(root)
        if held is None:
            return []
        out: list[dict[str, Any]] = []
        for expiry in fridays(self.today()):
            for right in ("call", "put"):
                for strike in held["strikes"]:
                    out.append({"osi": osi(root, expiry, right, strike), "right": right,
                                "strike": strike, "expiry": expiry, "cfi": True,
                                "multiplier": "100.000000000"})  # fmt: skip
            adjusted = held["adjusted"]
            out.append({"osi": osi(adjusted["root"], expiry, "call", adjusted["strike"]),
                        "right": "call", "strike": adjusted["strike"], "expiry": expiry,
                        "cfi": True, "multiplier": adjusted["multiplier"]})  # fmt: skip
            missing = held["no_cfi"]
            out.append({"osi": osi(root, expiry, missing["right"], missing["strike"]),
                        "right": missing["right"], "strike": missing["strike"],
                        "expiry": expiry, "cfi": False,
                        "multiplier": "100.000000000"})  # fmt: skip
        return out

    def definitions(self, root: str, start: int) -> list[dict[str, Any]]:
        out = []
        for one in self.series(root):
            right = "C" if one["right"] == "call" else "P"
            expiry = utc_midnight(one["expiry"])
            out.append(
                {
                    "ts_recv": str(start + 1000),
                    "hd": _hd(start, "definition", OPRA, self.instrument(one["osi"])),
                    "raw_symbol": one["osi"],
                    "security_update_action": "A",
                    "instrument_class": right,
                    "min_price_increment": "0.010000000",
                    "display_factor": "1.000000000",
                    "expiration": str(expiry),
                    "activation": str(expiry - 60 * DAY_NS),
                    "unit_of_measure_qty": one["multiplier"],
                    "currency": "USD",
                    "exchange": "OPRA",
                    "asset": root,
                    "cfi": f"O{right}AXPS" if one["cfi"] else "",
                    "security_type": "OPT",
                    "underlying": root,
                    "strike_price_currency": "USD",
                    "strike_price": f"{Decimal(one['strike']):.9f}",
                }
            )
        return out

    def statistics(self, root: str, start: int) -> list[dict[str, Any]]:
        held = self.held["options"].get(root)
        if held is None:
            return []
        published = start + 10 * 3600 * NS + 30 * 60 * NS
        out = []
        for one in self.series(root):
            for stat, figure in ((9, "open_interest"), (6, "volume")):
                out.append(
                    {
                        "ts_recv": str(published + 500),
                        "hd": _hd(published, "statistics", OPRA, self.instrument(one["osi"])),
                        "ts_ref": str(18446744073709551615),
                        "price": UNDEF_PRICE_TEXT,
                        "quantity": held[figure][one["right"]],
                        "sequence": 0,
                        "ts_in_delta": 0,
                        "stat_type": stat,
                        "channel_id": 0,
                        "update_action": 1,
                        "stat_flags": 0,
                        "symbol": one["osi"],
                    }
                )
        return out

    def cbbo(self, root: str, at: int) -> list[tuple[str, dict[str, Any]]]:
        held = self.held["options"].get(root)
        if held is None:
            return []
        out = []
        for one in self.series(root):
            quote = held["quote"][one["right"]]
            out.append(
                (
                    one["osi"],
                    {
                        "ts_recv": str(at),
                        "hd": _hd(at - 2 * NS, "cbbo-1m", OPRA, self.instrument(one["osi"])),
                        "side": "N",
                        "price": UNDEF_PRICE_TEXT,
                        "size": 0,
                        "flags": 0,
                        "levels": [
                            {
                                "bid_px": quote["bid"],
                                "ask_px": quote["ask"],
                                "bid_sz": quote["bid_size"],
                                "ask_sz": quote["ask_size"],
                                "bid_pb": 22,
                                "ask_pb": 36,
                            }
                        ],  # fmt: skip
                    },
                )
            )
        return out


class SyntheticTransport:
    """vendor.Transport on the built-in exchange: Databento's historical API."""

    def __init__(
        self, held: Mapping[str, Any] | None = None, clock_ns: Callable[[], int] = time.time_ns,
        reach_end_ns: Callable[[], int] | None = None,
    ) -> None:  # fmt: skip
        self.held = dict(held) if held is not None else exchange()
        self.clock_ns = clock_ns
        self.exchange = Exchange(self.held, clock_ns)
        #: Where the history ends: by default the UTC midnight that began
        #: today, so today's records come from the live gateway alone.
        self.reach_end_ns = reach_end_ns or (
            lambda: utc_midnight(dt.datetime.fromtimestamp(self.clock_ns() / NS, dt.UTC).date())
        )
        self.asked: list[tuple[str, dict[str, str]]] = []

    def __call__(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes | None
    ) -> Response:
        parsed = urllib.parse.urlparse(url)
        query = parsed.query if method == "GET" else (body or b"").decode()
        params = {k: v[0] for k, v in urllib.parse.parse_qs(query).items()}
        self.asked.append((parsed.path, params))
        if parsed.path == "/v0/metadata.get_dataset_range":
            end = dt.datetime.fromtimestamp(self.reach_end_ns() / NS, dt.UTC)
            stamp = end.strftime("%Y-%m-%dT%H:%M:%S.000000000Z")
            start = self.held.get("reach", {}).get("start", "2023-03-28T00:00:00.000000000Z")
            return self._json(200, {"start": start, "end": stamp, "schema": {}})
        if parsed.path != "/v0/timeseries.get_range":
            return self._json(404, {"detail": "Not found"})
        rows = self.records(params)
        limit = int(params.get("limit") or 0)
        if limit:
            rows = rows[:limit]
        text = "".join(json.dumps(row) + "\n" for row in rows)
        return Response(200, {"content-type": "application/json"}, text.encode())

    @staticmethod
    def _json(status: int, body: Any) -> Response:
        return Response(status, {"content-type": "application/json"}, json.dumps(body).encode())

    def records(self, params: Mapping[str, str]) -> list[dict[str, Any]]:
        dataset, schema = params.get("dataset", ""), params.get("schema", "")
        symbols = [s for s in params.get("symbols", "").split(",") if s]
        start, end = int(params.get("start", "0")), int(params.get("end", "0") or 2**63)
        made = self.exchange
        out: list[dict[str, Any]] = []
        for symbol in symbols:
            if (dataset, schema) == (EQUS_SUMMARY, "ohlcv-1d"):
                rows = made.daily(symbol, start, end)
            elif (dataset, schema) == (EQUS_MINI, "trades"):
                rows = made.trades(symbol, start, end)
            elif (dataset, schema) == (OPRA, "definition"):
                root = symbol.removesuffix(".OPT")
                rows = [r | {"symbol": r["raw_symbol"]} for r in made.definitions(root, start)]
            elif (dataset, schema) == (OPRA, "statistics"):
                rows = made.statistics(symbol.removesuffix(".OPT"), start)
            else:
                rows = []
            out.extend(r if "symbol" in r else r | {"symbol": symbol} for r in rows)
        return out


class SyntheticGateway:
    """The live gateway's Raw API on the built-in exchange: it greets,
    challenges, takes any answer, and after the session starts sends each
    subscription's records -- a replay's, then its completion; a stream's
    stamped now, then nothing more, held open."""

    def __init__(
        self, held: Mapping[str, Any] | None = None, clock_ns: Callable[[], int] = time.time_ns
    ) -> None:
        self.held = dict(held) if held is not None else exchange()
        self.clock_ns = clock_ns
        self.exchange = Exchange(self.held, clock_ns)
        #: The control lines each session heard, for the tests (no key is
        #: ever among them).
        self.heard: list[str] = []
        self.sessions = 0

    def opening(self) -> list[str]:
        self.sessions += 1
        return ["lsg_version=0.0.0-synthetic\n", "cram=SyntheticChallenge0123456789abcd\n"]

    def answer(self, line: str, state: dict[str, Any]) -> list[str]:
        """What the gateway says to one control line."""
        self.heard.append(line.rstrip("\n"))
        said = gateway.fields(line)
        if "auth" in said:
            state["dataset"] = said.get("dataset", "")
            return [f"success=1|session_id={self.sessions}\n"]
        if "schema" in said:
            state.setdefault("pending", []).append(said)
            if state.get("started"):
                return self._records(state)
            return []
        if "start_session" in said:
            state["started"] = True
            return self._records(state)
        return []

    def _records(self, state: dict[str, Any]) -> list[str]:
        dataset = state.get("dataset", "")
        pending, state["pending"] = state.get("pending", []), []
        out: list[dict[str, Any]] = []
        replayed: list[str] = []
        now = self.clock_ns()
        made = self.exchange
        for said in pending:
            schema = said["schema"]
            symbols = [s for s in said.get("symbols", "").split(",") if s]
            start = int(said.get("start") or 0)
            records: list[tuple[str, dict[str, Any]]] = []
            if dataset == EQUS_MINI and schema == "bbo-1m":
                records = [(s, r) for s in symbols if (r := made.latest_bbo(s)) is not None]
            elif dataset == EQUS_MINI and schema == "trades" and start:
                records = [(s, r) for s in symbols for r in made.trades(s, start, 2**63)]
            elif dataset == EQUS_MINI and not start:
                for event in self.held["stream"]["stocks"]:
                    if event["schema"] == schema and event["symbol"] in symbols:
                        at = now + int(event["offset_ms"]) * MS
                        if schema == "trades":
                            records.append(
                                (event["symbol"], made.trade(event["symbol"], event, at))
                            )
                        else:
                            records.append((event["symbol"], made.bbo(event["symbol"], schema,
                                                                      event, at)))  # fmt: skip
            elif dataset == OPRA and schema == "cbbo-1m":
                records = [r for s in symbols for r in made.cbbo(s.removesuffix(".OPT"), now)]
            elif dataset == OPRA and schema == "trades" and not start:
                for symbol in symbols:
                    for event in self.held["stream"]["options"]:
                        at = now + int(event["offset_ms"]) * MS
                        records.append((symbol, made.trade(symbol, event, at, OPRA, symbol)))
            for symbol, record in records:
                instrument = record["hd"]["instrument_id"]
                out.append({"hd": {"ts_event": str(now), "rtype": gateway.RTYPE_MAPPING,
                                   "publisher_id": 0, "instrument_id": instrument},
                            "stype_in_symbol": symbol, "stype_out_symbol": symbol,
                            "start_ts": str(now), "end_ts": str(now + DAY_NS)})  # fmt: skip
                out.append(record)
            if start:
                replayed.append(schema)
        for schema in replayed:
            out.append({"hd": {"ts_event": str(now), "rtype": gateway.RTYPE_SYSTEM,
                               "publisher_id": 0, "instrument_id": 0},
                        "msg": f"Finished {schema} replay",
                        "code": gateway.REPLAY_COMPLETED})  # fmt: skip
        return [json.dumps(r) + "\n" for r in out]


class SyntheticLines:
    """gateway.Lines on the synthetic gateway, blocking."""

    def __init__(self, held: SyntheticGateway) -> None:
        self.gateway = held
        self.queue: list[str] = held.opening()
        self.state: dict[str, Any] = {}
        self.closed = False

    def readline(self) -> bytes:
        if self.closed or not self.queue:
            return b""
        return self.queue.pop(0).encode()

    def write(self, data: bytes) -> None:
        self.queue.extend(self.gateway.answer(data.decode(), self.state))

    def close(self) -> None:
        self.closed = True


class SyntheticStream:
    """gateway.Stream on the synthetic gateway, on asyncio: held open, silent
    once it has said what it has."""

    def __init__(self, held: SyntheticGateway) -> None:
        self.gateway = held
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        for line in held.opening():
            self.queue.put_nowait(line)
        self.state: dict[str, Any] = {}
        self.closed = False

    async def readline(self) -> bytes:
        if self.closed:
            return b""
        try:
            return (await asyncio.wait_for(self.queue.get(), gateway.HEARTBEAT_SECONDS)).encode()
        except TimeoutError:
            # Nothing to say for a heartbeat's interval: a heartbeat, as the
            # gateway sends one.
            beat = {"hd": {"ts_event": str(self.gateway.clock_ns()), "rtype": gateway.RTYPE_SYSTEM,
                           "publisher_id": 0, "instrument_id": 0},
                    "msg": "Heartbeat", "code": gateway.HEARTBEAT}  # fmt: skip
            return (json.dumps(beat) + "\n").encode()

    async def write(self, data: bytes) -> None:
        for line in self.gateway.answer(data.decode(), self.state):
            await self.queue.put(line)

    async def close(self) -> None:
        self.closed = True
        await self.queue.put("")


def connect(held: SyntheticGateway) -> gateway.Connect:
    """A blocking `connect` for gateway.replay on the synthetic gateway."""

    def opened(host: str, port: int, timeout: float) -> gateway.Lines:
        return SyntheticLines(held)

    return opened


def connect_async(held: SyntheticGateway) -> gateway.AsyncConnect:
    """An asyncio `connect` for gateway.Session on the synthetic gateway."""

    async def opened(host: str, port: int) -> gateway.Stream:
        return SyntheticStream(held)

    return opened
