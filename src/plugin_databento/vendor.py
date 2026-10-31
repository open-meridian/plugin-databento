"""Databento's historical API: the one module that speaks HTTP to it.

`https://hist.databento.com/v0/<family>.<method>` (its documentation is
public at databento.com/docs/api-reference-historical): `timeseries.get_range`
(a dataset's records of one schema for some symbols over a time range,
asked as a form `POST` and answered as JSON lines, one record a line) and
`metadata.get_dataset_range` (how far a dataset's history reaches for this
key, overall and per schema; asked with `GET`). Every timeseries answer is
asked with `encoding=json`, `pretty_px=true` (each price a decimal string,
as Databento scales its fixed-point 1e-9 integers), `pretty_ts=false` (each
time UNIX nanoseconds) and `map_symbols=true` (each record carries the
symbol asked for), so no binary record is ever decoded here.

**The key.** Databento's API key, from `settings.Credentials`, goes in the
`Authorization` header as Basic authentication's user name and nowhere
else: never in an address, a raw record or a log line, and any text holding
it is redacted before it is kept, as a last guard. Only Databento's own host
is ever asked.

**Billing.** Databento bills historical data by volume on its usage-based
plan; every read here names its symbols and a bounded range, never a whole
dataset (`ALL_SYMBOLS` is never asked), and a range of trades is read
`TRADES_LIMIT` records at a time.

**Pacing.** Databento allows 100 timeseries and 20 metadata requests a
second per address; this client spends at most `PER_MINUTE`, one at a time.
A 429 is waited out (its Retry-After, else a second) and asked again at most
three times, each counted for the Connection page.

**Exact.** Prices arrive as decimal strings and any JSON number is parsed
as a Decimal from its own text, never a float; integers stay integers.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from meridian.edge import RawStore

from .settings import Credentials

API = "https://hist.databento.com"
HOST = "hist.databento.com"
VERSION = "v0"
USER_AGENT = "meridian-databento/0.1.1 (+https://open-meridian.dev)"
NS = 1_000_000_000

#: Databento's datasets this plugin reads (databento.com/docs/venues-and-datasets).
EQUS_SUMMARY = "EQUS.SUMMARY"
EQUS_MINI = "EQUS.MINI"
OPRA = "OPRA.PILLAR"

#: Calls a minute this client spends at most: far inside Databento's limits.
PER_MINUTE = 600
#: The most symbols one timeseries call names: Databento's limit is 2,000.
SYMBOLS_PER_CALL = 500
#: The most records one read of trades asks for; a range past it is read on.
TRADES_LIMIT = 50_000
#: The most pages one read follows: a guard.
MOST_PAGES = 200
#: How many times a 429 is waited out before the call fails.
RETRIES = 3
#: How long a dataset's reach is held before it is asked again.
RANGE_SECONDS = 600
KEPT_HEADERS = ("date", "content-type", "retry-after", "request-id", "x-request-id")
REDACTED = "[redacted]"


@dataclass(frozen=True)
class Response:
    status: int
    headers: Mapping[str, str]
    body: bytes


Transport = Callable[[str, str, Mapping[str, str], bytes | None], Response]


def urllib_transport(
    method: str, url: str, headers: Mapping[str, str], body: bytes | None
) -> Response:
    """One request over HTTPS, with the standard library."""
    request = urllib.request.Request(url, data=body, headers=dict(headers), method=method)
    try:
        with urllib.request.urlopen(request, timeout=120) as answer:
            return Response(
                answer.status, {k.lower(): v for k, v in answer.headers.items()}, answer.read()
            )
    except urllib.error.HTTPError as answer:
        return Response(
            answer.code, {k.lower(): v for k, v in answer.headers.items()}, answer.read()
        )


def parse(text: str) -> Any:
    """JSON with every non-integer number a Decimal from its own text."""
    return json.loads(text, parse_float=Decimal)


def lines(text: str) -> list[Any]:
    """JSON lines, each parsed (`parse`); a blank line skipped."""
    return [parse(line) for line in text.splitlines() if line.strip()]


class NoKey(Exception):
    """No key is set: nothing is asked."""


class VendorError(Exception):
    """Databento answered with an error: its status and what it said."""

    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"{status} {detail}")
        self.status, self.detail = status, detail

    @property
    def not_entitled(self) -> bool:
        """Databento's answer to a call the key may not make (403), or one
        its account cannot be billed for (402)."""
        return self.status in (402, 403)


class RateLimiter:
    """At most `per_minute` calls a minute, evenly, one at a time."""

    def __init__(
        self,
        per_minute: int = PER_MINUTE,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.per_minute = per_minute
        self._clock, self._sleep = clock, sleep
        self._next = 0.0

    @property
    def interval(self) -> float:
        return 60.0 / self.per_minute

    def acquire(self) -> None:
        now = self._clock()
        if now < self._next:
            self._sleep(self._next - now)
            now = self._next
        self._next = now + self.interval


@dataclass
class Usage:
    """What the client spent, for the Connection page: never a key."""

    calls: int = 0
    rate_limited: int = 0
    records: int = 0
    last_status: int = 0
    last_call_at_ns: int = 0
    last_error: str = ""
    recent: deque[int] = field(default_factory=deque)

    def note(self, at_ns: int, status: int) -> None:
        self.calls += 1
        self.last_status = status
        self.last_call_at_ns = at_ns
        self.recent.append(at_ns)
        while self.recent and self.recent[0] <= at_ns - 60 * NS:
            self.recent.popleft()

    def last_minute(self, now_ns: int) -> int:
        return sum(1 for at in self.recent if at > now_ns - 60 * NS)


@dataclass(frozen=True)
class Fetched:
    """One answer: its parsed body (a timeseries' records, a list) and the
    raw record it was kept as."""

    value: Any
    raw_key: str
    status: int = 200


@dataclass(frozen=True)
class Reach:
    """How far a dataset's history reaches for this key: overall, and each
    schema's end where Databento states one."""

    start_ns: int
    end_ns: int
    schemas: Mapping[str, int] = field(default_factory=dict)

    def end(self, schema: str) -> int:
        return self.schemas.get(schema, self.end_ns)


def iso_ns(text: object) -> int | None:
    """An ISO 8601 moment, as Databento states one, in UNIX nanoseconds."""
    if not isinstance(text, str) or not text:
        return None
    found = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?)(?:\.(\d{1,9}))?(Z|[+-]\d{2}:?\d{2})?",
        text.strip(),
    )
    if found is None:
        return None
    whole, fraction, zone = found.groups()
    try:
        moment = dt.datetime.fromisoformat(whole + (zone or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=dt.UTC)
    seconds = int(moment.timestamp())
    return seconds * NS + int((fraction or "").ljust(9, "0") or 0)


class Historical:
    """Databento's historical API, every answer kept first."""

    def __init__(
        self,
        store: RawStore,
        credentials: Credentials | None,
        transport: Transport = urllib_transport,
        base: str = API,
        keyless: bool = False,
        clock_ns: Callable[[], int] = time.time_ns,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.store = store
        self.credentials = credentials
        self.transport = transport
        self.base = base.rstrip("/")
        self.keyless = keyless
        self.clock_ns = clock_ns
        self.sleep = sleep
        self.usage = Usage()
        self.limiter = RateLimiter(sleep=sleep)
        self._reach: dict[str, tuple[int, Reach]] = {}

    # ── One call ─────────────────────────────────────────────────────────

    def _headers(self, form: bool) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if form:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        if self.credentials is not None:
            headers.update(self.credentials.headers())
        return headers

    def _redacted(self, text: str) -> str:
        if self.credentials is None:
            return text
        for secret in self.credentials.secrets():
            text = text.replace(secret, REDACTED)
        return text

    def call(
        self,
        method: str,
        params: Mapping[str, object],
        *,
        post: bool = False,
        call: str,
        name: str,
        json_lines: bool = False,
    ) -> Fetched:
        """`method` (`timeseries.get_range`) with `params`, on Databento's
        host alone: kept, then parsed. A 429 is waited out; any other status
        but 200 and 206 (some symbols resolved) is a `VendorError`."""
        if self.credentials is None and not self.keyless:
            raise NoKey("no Databento API key: enter it in Settings")
        given = {k: str(v) for k, v in params.items() if v is not None and v != ""}
        encoded = urllib.parse.urlencode(given)
        url = f"{self.base}/{VERSION}/{method}"
        body: bytes | None = None
        if post:
            body = encoded.encode()
        elif encoded:
            url = f"{url}?{encoded}"
        for attempt in range(RETRIES + 1):
            self.limiter.acquire()
            sent = self.clock_ns()
            try:
                answer = self.transport("POST" if post else "GET", url, self._headers(post), body)
            except OSError as failed:
                self.usage.last_error = f"{type(failed).__name__}: {failed}"[:300]
                raise
            received = self.clock_ns()
            self.usage.note(received, answer.status)
            text = self._redacted(answer.body.decode("utf-8", errors="replace"))
            raw_key = self.store.keep(
                call,
                name,
                {
                    "received_at_ns": received,
                    "sent_at_ns": sent,
                    "request": {
                        "method": "POST" if post else "GET",
                        "url": self._redacted(url),
                        "form": {k: self._redacted(v) for k, v in given.items()} if post else {},
                    },
                    "status": answer.status,
                    "headers": {k: v for k, v in answer.headers.items() if k in KEPT_HEADERS},
                    "body": text,
                },
            )
            if answer.status == 429 and attempt < RETRIES:
                self.usage.rate_limited += 1
                wait = answer.headers.get("retry-after", "")
                self.sleep(float(wait) if wait.isdigit() else 1.0)
                continue
            if answer.status not in (200, 206):
                detail = _message(text) or f"HTTP {answer.status}"
                self.usage.last_error = f"{answer.status} {detail}"[:300]
                raise VendorError(answer.status, detail[:200])
            try:
                value = lines(text) if json_lines else parse(text)
            except ValueError:
                self.usage.last_error = "not JSON"
                raise VendorError(answer.status, "the answer was not JSON") from None
            if json_lines:
                self.usage.records += len(value)
            self.usage.last_error = ""
            return Fetched(value, raw_key, answer.status)
        raise VendorError(429, "rate limited")  # pragma: no cover - the loop returns or raises

    # ── Metadata ─────────────────────────────────────────────────────────

    def reach(self, dataset: str, again: bool = False) -> Reach:
        """How far `dataset` reaches for this key (`metadata.get_dataset_range`),
        held `RANGE_SECONDS`."""
        now = self.clock_ns()
        held = self._reach.get(dataset)
        if held is not None and not again and now - held[0] < RANGE_SECONDS * NS:
            return held[1]
        fetched = self.call(
            "metadata.get_dataset_range", {"dataset": dataset}, call="range", name=dataset
        )
        value = fetched.value if isinstance(fetched.value, dict) else {}
        schemas: dict[str, int] = {}
        for schema, span in (value.get("schema") or {}).items():
            end = iso_ns(span.get("end")) if isinstance(span, dict) else None
            if end is not None:
                schemas[str(schema)] = end
        reach = Reach(
            start_ns=iso_ns(value.get("start")) or 0,
            end_ns=iso_ns(value.get("end")) or 0,
            schemas=schemas,
        )
        self._reach[dataset] = (now, reach)
        return reach

    # ── Time series ──────────────────────────────────────────────────────

    def get_range(
        self,
        dataset: str,
        schema: str,
        symbols: Sequence[str],
        start_ns: int,
        end_ns: int,
        *,
        stype_in: str = "raw_symbol",
        limit: int = 0,
        call: str,
        name: str,
    ) -> Fetched:
        """One schema's records of `symbols` in `dataset` from `start_ns` to
        before `end_ns`, oldest first, as JSON lines."""
        return self.call(
            "timeseries.get_range",
            {
                "dataset": dataset,
                "schema": schema,
                "symbols": ",".join(symbols),
                "stype_in": stype_in,
                "stype_out": "instrument_id",
                "start": start_ns,
                "end": end_ns,
                "encoding": "json",
                "compression": "none",
                "pretty_px": "true",
                "pretty_ts": "false",
                "map_symbols": "true",
                "limit": limit or None,
            },
            post=True,
            call=call,
            name=name,
            json_lines=True,
        )


def _message(text: str) -> str:
    """What Databento said of an error: its `detail` or `message`, else none."""
    try:
        said = json.loads(text)
    except ValueError:
        return text.strip().splitlines()[0][:200] if text.strip() else ""
    if not isinstance(said, dict):
        return ""
    detail = said.get("detail") or said.get("message") or said.get("error") or ""
    if isinstance(detail, dict):
        detail = detail.get("message") or json.dumps(detail)[:200]
    return str(detail)
