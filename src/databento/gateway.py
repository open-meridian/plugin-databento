"""Databento's live gateway, by its Raw API: the one module that speaks it.

Databento's live data is served over a plain TCP connection, one per
dataset, at `<dataset, lowercased, dots to dashes>.lsg.databento.com:13000`
(`opra-pillar.lsg.databento.com`), by a line protocol its documentation
publishes (databento.com/docs/api-reference-live, the Raw API's control
messages). There is no WebSocket: this is the plugin's own client of that
protocol, as plugin-massive's ws.py is of Massive's WebSocket.

**The session.** The gateway greets (`lsg_version=...`) and challenges
(`cram=<challenge>`); the client answers `auth=<answer>|dataset=...|
encoding=json|...`, the answer the SHA-256 of the challenge and the key
(settings.Credentials.answer), so the key itself never crosses the
connection; the gateway says `success=1|session_id=...` or `success=0|
error=...`. Then subscriptions, one line each -- `schema`, `stype_in`,
`symbols` (at most `SYMBOLS_PER_LINE` a line, `is_last=0` on all but the
last), and an intraday replay's `start` where one is asked (the last 24
hours; only before the session starts) -- and `start_session=0`. Records
follow as JSON lines (`encoding=json`, `pretty_px=1`, `pretty_ts=0`): each
with its header `hd` (`ts_event`, `rtype`, `publisher_id`,
`instrument_id`). A record names its instrument by Databento's numeric ID;
a symbol mapping record (`rtype` 22) says which symbol each ID is.

**What the gateway says** (`rtype` 23, `SystemMsg`): a heartbeat (code 0),
a subscription acknowledged (1), a slow reader warned (2), a replay
completed for a schema (3), an interval's end (4). **What went wrong**
(`rtype` 21, `ErrorMsg`): authentication refused (1), the key deactivated
(2), too many connections (3) and an invalid subscription (5) are fatal and
end the session, as is an internal error (6) or a replay past the gateway's
retention (8); a symbol that did not resolve (4) and records skipped for a
slow reader (7) are not.

Two forms of one protocol: `replay`, blocking, a short session that asks
the last minutes of some schemas and closes once each has caught up (a
live price, a chain's quotes, a gap's trades: run in a thread, as the
SDK's feed runs a source's calls); and `Session`, asyncio's, kept open for
the stream (stream.py).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Protocol

from .settings import Credentials

PORT = 13000
DOMAIN = "lsg.databento.com"
CLIENT = "meridian-databento 0.1.0"
#: The most symbols one subscription line names: Databento's advice.
SYMBOLS_PER_LINE = 500
#: The gateway's heartbeat asked for, in seconds (5 to 1800).
HEARTBEAT_SECONDS = 30
#: How long the greeting, the challenge and the answer are waited for.
ANSWER_SECONDS = 15.0
#: The longest line read: a record is far shorter.
MOST_BYTES = 1024 * 1024

RTYPE_ERROR, RTYPE_MAPPING, RTYPE_SYSTEM = 21, 22, 23
HEARTBEAT, SUBSCRIPTION_ACK, SLOW_READER, REPLAY_COMPLETED, END_OF_INTERVAL = 0, 1, 2, 3, 4
#: The error codes that end the session (Databento's ErrorCode).
FATAL = frozenset({1, 2, 3, 5, 6, 8})
#: An authentication refused, or the key deactivated: not tried again soon.
REFUSED = frozenset({1, 2})
SYMBOL_RESOLUTION_FAILED = 4


def host_of(dataset: str) -> str:
    """The live gateway of a dataset: `OPRA.PILLAR` at
    `opra-pillar.lsg.databento.com`."""
    return f"{dataset.lower().replace('.', '-')}.{DOMAIN}"


class GatewayError(Exception):
    """The gateway said something went wrong: its code, where it gave one."""

    def __init__(self, detail: str, code: int = 0) -> None:
        super().__init__(detail)
        self.code = code


class Refused(GatewayError):
    """The gateway refused the key: tried again only after a while."""


@dataclass(frozen=True)
class Subscription:
    """One subscription: a schema of some symbols, and an intraday replay's
    start where one is asked (UNIX nanoseconds; 0 asks none)."""

    schema: str
    symbols: tuple[str, ...]
    stype_in: str = "raw_symbol"
    start_ns: int = 0


def fields(line: str) -> dict[str, str]:
    """A control line's `key=value|key=value` fields."""
    out: dict[str, str] = {}
    for part in line.strip().split("|"):
        key, _, value = part.partition("=")
        if key:
            out[key] = value
    return out


def auth_line(credentials: Credentials, challenge: str, dataset: str) -> bytes:
    """The authentication request: the challenge answered, never the key."""
    said = [
        f"auth={credentials.answer(challenge)}",
        f"dataset={dataset}",
        "encoding=json",
        "compression=none",
        "ts_out=0",
        "pretty_px=1",
        "pretty_ts=0",
        f"heartbeat_interval_s={HEARTBEAT_SECONDS}",
        f"client={CLIENT}",
    ]
    return ("|".join(said) + "\n").encode()


def subscription_lines(subscription: Subscription, first_id: int = 1) -> list[bytes]:
    """A subscription's request, its symbols `SYMBOLS_PER_LINE` a line."""
    symbols = list(subscription.symbols)
    chunks = [symbols[i : i + SYMBOLS_PER_LINE] for i in range(0, len(symbols), SYMBOLS_PER_LINE)]
    out: list[bytes] = []
    for n, chunk in enumerate(chunks):
        said = [
            f"schema={subscription.schema}",
            f"stype_in={subscription.stype_in}",
            f"symbols={','.join(chunk)}",
        ]
        if subscription.start_ns:
            said.append(f"start={subscription.start_ns}")
        said.append(f"id={first_id}")
        said.append(f"is_last={1 if n == len(chunks) - 1 else 0}")
        out.append(("|".join(said) + "\n").encode())
    return out


START_SESSION = b"start_session=0\n"


def authenticated(reply: str) -> str:
    """The gateway's answer to the authentication: its session ID, or
    `Refused` naming what it said."""
    said = fields(reply)
    if said.get("success") != "1":
        raise Refused(said.get("error") or reply.strip()[:200] or "authentication refused", 1)
    return said.get("session_id", "")


def record(text: str) -> dict[str, Any] | None:
    """One JSON line from the gateway, each number its own text's (a
    Decimal where it has a point); None where it is not a record (a
    metadata line, a blank)."""
    if not text.strip():
        return None
    try:
        said = json.loads(text, parse_float=Decimal)
    except ValueError:
        return None
    if not isinstance(said, dict) or not isinstance(said.get("hd"), dict):
        return None
    return said


def rtype(said: dict[str, Any]) -> int:
    held = said.get("hd", {}).get("rtype")
    try:
        return int(held)
    except (TypeError, ValueError):
        return -1


def instrument_id(said: dict[str, Any]) -> int:
    held = said.get("hd", {}).get("instrument_id")
    try:
        return int(held)
    except (TypeError, ValueError):
        return 0


def code_of(said: dict[str, Any]) -> int | None:
    held = said.get("code")
    try:
        return None if held is None else int(held)
    except (TypeError, ValueError):
        return None


def replay_completed(said: dict[str, Any]) -> bool:
    """A system record saying a replay caught up: its code 3, or, from a
    gateway that sends no code, its words."""
    if rtype(said) != RTYPE_SYSTEM:
        return False
    code = code_of(said)
    if code is not None:
        return code == REPLAY_COMPLETED
    words = str(said.get("msg") or "").lower()
    return "replay" in words and ("complet" in words or "finish" in words)


def failed(said: dict[str, Any]) -> GatewayError | None:
    """What an error record says, as the error to raise where it ends the
    session; None where it does not (a symbol not resolved, records skipped)."""
    if rtype(said) != RTYPE_ERROR:
        return None
    code = code_of(said) or 0
    detail = str(said.get("err") or "the gateway said an error")[:200]
    if code in REFUSED:
        return Refused(detail, code)
    if code in FATAL or (code == 0 and said.get("is_last") in (True, 1, "1", "true")):
        return GatewayError(detail, code)
    return None


@dataclass
class Mapping_:
    """Which symbol each of the gateway's instrument IDs is, this session."""

    symbols: dict[int, str] = field(default_factory=dict)
    unresolved: list[str] = field(default_factory=list)

    def hear(self, said: dict[str, Any]) -> bool:
        """Take a symbol mapping, or a symbol not resolved; whether it was one."""
        kind = rtype(said)
        if kind == RTYPE_MAPPING:
            symbol = str(said.get("stype_out_symbol") or said.get("stype_in_symbol") or "")
            if symbol:
                self.symbols[instrument_id(said)] = symbol
            return True
        if kind == RTYPE_ERROR and code_of(said) == SYMBOL_RESOLUTION_FAILED:
            self.unresolved.append(str(said.get("err") or "")[:200])
            return True
        return False

    def of(self, said: dict[str, Any]) -> str:
        return self.symbols.get(instrument_id(said), "")


# ── Blocking: a short replay ────────────────────────────────────────────────


class Lines(Protocol):
    """A connection read and written a line at a time, blocking."""

    def readline(self) -> bytes: ...

    def write(self, data: bytes) -> None: ...

    def close(self) -> None: ...


class SocketLines:
    """`Lines` on a TCP socket."""

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.file = self.sock.makefile("rb")

    def readline(self) -> bytes:
        return self.file.readline(MOST_BYTES)

    def write(self, data: bytes) -> None:
        self.sock.sendall(data)

    def close(self) -> None:
        with contextlib.suppress(OSError):
            self.file.close()
        with contextlib.suppress(OSError):
            self.sock.close()


Connect = Callable[[str, int, float], Lines]


def socket_connect(host: str, port: int, timeout: float) -> Lines:
    return SocketLines(host, port, timeout)


@dataclass
class Replayed:
    """What a replay heard: each record, with the symbol it is of, and the
    lines as they came, for the raw record."""

    records: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)
    unresolved: list[str] = field(default_factory=list)
    completed: bool = False


def _readline(conn: Lines, clock: Callable[[], float], deadline: float) -> str:
    if clock() > deadline:
        raise TimeoutError("the gateway did not finish the replay in time")
    raw = conn.readline()
    if not raw:
        raise GatewayError("the gateway closed the connection")
    return raw.decode("utf-8", errors="replace")


def open_blocking(
    conn: Lines, credentials: Credentials, dataset: str, clock: Callable[[], float], deadline: float
) -> str:
    """Greeting, challenge, answer: the session's ID."""
    for _ in range(3):
        line = _readline(conn, clock, deadline)
        said = fields(line)
        if "cram" in said:
            conn.write(auth_line(credentials, said["cram"], dataset))
            return authenticated(_readline(conn, clock, deadline))
    raise GatewayError("the gateway sent no challenge")


def replay(
    credentials: Credentials,
    dataset: str,
    subscriptions: Sequence[Subscription],
    *,
    connect: Connect = socket_connect,
    port: int = PORT,
    seconds: float = 60.0,
    clock: Callable[[], float] = time.monotonic,
) -> Replayed:
    """A short session on `dataset`'s gateway: `subscriptions`, each with its
    replay's start, heard until every schema's replay has completed (or
    `seconds` have passed), then closed. Each record with its symbol, by the
    session's symbol mappings."""
    deadline = clock() + seconds
    conn = connect(host_of(dataset), port, ANSWER_SECONDS)
    heard = Replayed()
    mapping = Mapping_()
    try:
        open_blocking(conn, credentials, dataset, clock, deadline)
        for n, subscription in enumerate(subscriptions, start=1):
            for line in subscription_lines(subscription, n):
                conn.write(line)
        conn.write(START_SESSION)
        waiting = {s.schema for s in subscriptions if s.symbols}
        while waiting:
            text = _readline(conn, clock, deadline)
            said = record(text)
            if said is None:
                continue
            heard.lines.append(text.rstrip("\n"))
            if mapping.hear(said):
                continue
            stop = failed(said)
            if stop is not None:
                raise stop
            if replay_completed(said):
                schema = str(said.get("msg") or "")
                done = {s for s in waiting if s in schema} or {min(waiting)}
                waiting -= done
                continue
            if rtype(said) in (RTYPE_SYSTEM, RTYPE_ERROR):
                continue
            heard.records.append((mapping.of(said), said))
        heard.completed = True
    finally:
        conn.close()
    heard.unresolved = mapping.unresolved
    return heard


# ── asyncio: the stream ─────────────────────────────────────────────────────


class Stream(Protocol):
    """A connection read and written a line at a time, on asyncio."""

    async def readline(self) -> bytes: ...

    async def write(self, data: bytes) -> None: ...

    async def close(self) -> None: ...


class TcpStream:
    """`Stream` on asyncio's streams."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.reader, self.writer = reader, writer

    async def readline(self) -> bytes:
        return await self.reader.readline()

    async def write(self, data: bytes) -> None:
        self.writer.write(data)
        await self.writer.drain()

    async def close(self) -> None:
        self.writer.close()
        with contextlib.suppress(Exception):
            await self.writer.wait_closed()


AsyncConnect = Callable[[str, int], Awaitable[Stream]]


async def tcp_connect(host: str, port: int) -> Stream:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port, limit=MOST_BYTES), ANSWER_SECONDS
    )
    return TcpStream(reader, writer)


class Session:
    """One live session on a dataset's gateway, kept open: authenticated,
    subscribed, started, then read a record at a time."""

    def __init__(
        self,
        credentials: Credentials,
        dataset: str,
        connect: AsyncConnect = tcp_connect,
        port: int = PORT,
    ) -> None:
        self.credentials, self.dataset = credentials, dataset
        self.connect, self.port = connect, port
        self.conn: Stream | None = None
        self.session_id = ""
        self.started = False
        self.mapping = Mapping_()
        self._next_id = 1

    async def _line(self) -> str:
        assert self.conn is not None
        raw = await self.conn.readline()
        if not raw:
            raise GatewayError("the gateway closed the connection")
        return raw.decode("utf-8", errors="replace")

    async def open(self) -> None:
        self.conn = await self.connect(host_of(self.dataset), self.port)
        self.started = False
        self.mapping = Mapping_()
        for _ in range(3):
            said = fields(await asyncio.wait_for(self._line(), ANSWER_SECONDS))
            if "cram" in said:
                await self.conn.write(auth_line(self.credentials, said["cram"], self.dataset))
                reply = await asyncio.wait_for(self._line(), ANSWER_SECONDS)
                self.session_id = authenticated(reply)
                return
        raise GatewayError("the gateway sent no challenge")

    async def subscribe(self, subscriptions: Iterable[Subscription]) -> None:
        """Subscribe; a replay's start is asked only before the session
        starts, as the gateway allows."""
        assert self.conn is not None
        for subscription in subscriptions:
            if not subscription.symbols:
                continue
            if self.started and subscription.start_ns:
                subscription = Subscription(
                    subscription.schema, subscription.symbols, subscription.stype_in
                )
            for line in subscription_lines(subscription, self._next_id):
                await self.conn.write(line)
            self._next_id += 1

    async def start(self) -> None:
        assert self.conn is not None
        if not self.started:
            await self.conn.write(START_SESSION)
            self.started = True

    async def receive(self) -> tuple[str, str, dict[str, Any] | None]:
        """The next line: its text, the symbol of the record it is, and the
        record (None for a mapping or a line that is no record). A fatal
        error is raised."""
        text = await self._line()
        said = record(text)
        if said is None:
            return text, "", None
        if self.mapping.hear(said):
            return text, "", None
        stop = failed(said)
        if stop is not None:
            raise stop
        return text, self.mapping.of(said), said

    async def close(self) -> None:
        if self.conn is not None:
            with contextlib.suppress(Exception):
                await self.conn.close()
        self.conn = None
        self.started = False
