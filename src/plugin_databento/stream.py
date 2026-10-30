"""Databento's live data, streamed from its live gateway, kept open (contract
v19).

What is streamed is what the lake keeps current: the listings under a
standing want of the stock datasets (lake.py says which) on `EQUS.MINI`'s
gateway -- their trades and their best bid and offer each second
(`bbo-1s`) -- and the option series under a standing want of option trades
(chains.py) on `OPRA.PILLAR`'s, their trades; nothing while there is none;
nothing of it kept here. One session per dataset, as Databento serves one
dataset a session (gateway.Session).

**Subscribing.** The first subscription and the session's start are sent
once the session is authenticated; a listing added later is subscribed in
the same session; a listing let go closes the session and opens another
for what is still wanted, as the gateway takes no unsubscription a client
can rely on.

**Reconnect.** A session that breaks, closes, errs fatally or says nothing
for a heartbeat and ten seconds (`SILENT_SECONDS`, Databento's own advice)
is opened again a second later, then doubling to a minute; the
subscriptions are sent again from what is wanted. The gap is caught up by
the SDK's feed from the history and the gateway's intraday replay, a trade
recorded already never sent again (lake.py). A refused key waits
`REFUSED_WAIT` before it is tried again. Each reconnect is counted on the
Connection page. Databento restarts its gateways each Sunday (10:30 UTC);
the session is simply opened again.

**Every line is a raw record first**, a line of the day's stream record,
before anything converts it; the key is never in one (it is never sent).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from meridian.edge import RawStore

from . import convert, gateway
from .settings import Credentials

log = logging.getLogger("databento.stream")

#: How long a session may say nothing before it is taken for broken: the
#: heartbeat asked for, and ten seconds (Databento's advice).
SILENT_SECONDS = float(gateway.HEARTBEAT_SECONDS + 10)
#: How often what is heard is flushed to the lake, at the most.
FLUSH_SECONDS = 0.5
#: The waits between sessions: a second, doubling to a minute.
FIRST_WAIT, MOST_WAIT = 1.0, 60.0
#: After the key is refused, how long before it is tried again.
REFUSED_WAIT = 300.0
#: The schemas streamed, by asset class.
SCHEMAS = {"stocks": ("trades", "bbo-1s"), "options": ("trades",)}
#: Databento's record types read from the stream: a trade, a BBO each second.
RTYPE_TRADE, RTYPE_BBO_1S = 0, 195


class Feed(Protocol):
    """One session with a stream: opened, subscribed, read, closed."""

    name: str

    async def open(self) -> None: ...

    async def subscribe(self, symbols: Sequence[str]) -> None: ...

    async def receive(self) -> list[Any]: ...

    async def close(self) -> None: ...


class DatabentoFeed:
    """Feed on one dataset's live gateway (`stocks`: `EQUS.MINI`;
    `options`: `OPRA.PILLAR`), every line kept."""

    def __init__(
        self,
        asset_class: str,
        dataset: str,
        credentials: Credentials,
        store: RawStore,
        connect: gateway.AsyncConnect = gateway.tcp_connect,
        clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.asset_class, self.dataset, self.credentials = asset_class, dataset, credentials
        self.schemas = SCHEMAS[asset_class]
        self.store, self.connect, self.clock_ns = store, connect, clock_ns
        self.name = f"databento {asset_class}"
        self.session = gateway.Session(credentials, dataset, connect)
        self.subscribed: list[str] = []
        self.numbering = convert.Numbering()
        self.said = ""

    @property
    def host(self) -> str:
        return gateway.host_of(self.dataset)

    def _keep(self, text: str) -> str:
        return self.store.keep_line(
            "stream", self.asset_class, {"received_at_ns": self.clock_ns(), "body": text.rstrip()}
        )

    async def open(self) -> None:
        self.subscribed = []
        await self.session.open()

    def _subscriptions(self, symbols: Sequence[str]) -> list[gateway.Subscription]:
        return [gateway.Subscription(schema, tuple(symbols)) for schema in self.schemas]

    async def subscribe(self, symbols: Sequence[str]) -> None:
        wanted = sorted(symbols)
        added = [s for s in wanted if s not in self.subscribed]
        gone = [s for s in self.subscribed if s not in wanted]
        if gone and self.session.started:
            # The gateway takes no unsubscription a client relies on: the
            # session is opened again for what is still wanted.
            await self.session.close()
            await self.session.open()
            added = wanted
        if added:
            await self.session.subscribe(self._subscriptions(added))
        if wanted:
            await self.session.start()
        self.subscribed = wanted

    async def receive(self) -> list[Any]:
        text, symbol, said = await self.session.receive()
        key = self._keep(text)
        if said is None:
            return []
        kind = gateway.rtype(said)
        if kind == gateway.RTYPE_SYSTEM:
            if gateway.code_of(said) == gateway.SLOW_READER:
                self.said = f"slow reader: {said.get('msg') or ''}"[:300]
            return []
        if kind == gateway.RTYPE_ERROR:
            self.said = str(said.get("err") or "")[:300]
            return []
        if self.asset_class == "options":
            osi = convert.osi_of(symbol)
            if kind != RTYPE_TRADE or osi is None:
                return []
            made = convert.trade(osi, said, key, self.numbering, osi=osi)
            return [made] if made is not None else []
        listing = convert.spelled(symbol)
        if listing is None:
            return []
        if kind == RTYPE_TRADE:
            made_trade = convert.trade(listing, said, key, self.numbering)
            return [made_trade] if made_trade is not None else []
        if kind == RTYPE_BBO_1S:
            made_quote = convert.quote(listing, said, key, "bbo")
            return [made_quote] if made_quote is not None else []
        return []

    async def close(self) -> None:
        await self.session.close()


@dataclass
class Session:
    """A stream as the Connection page shows it: never a key."""

    state: str = "idle"
    subscribed: int = 0
    opened: int = 0
    reconnects: int = 0
    messages: int = 0
    last_message_at_ns: int = 0
    last_error: str = ""
    since_ns: int = 0
    symbols: list[str] = field(default_factory=list)


class Restart(Exception):
    """The settings changed: the session is closed and opened again."""


class Streamer:
    """Keeps one session open while anything is wanted, subscribed to what
    is; hands what it hears to `heard`, and flushes with `flush`."""

    def __init__(
        self,
        make_feed: Callable[[], Feed | None],
        heard: Callable[[list[Any]], Awaitable[None]],
        flush: Callable[[], Awaitable[None]],
        clock_ns: Callable[[], int] = time.time_ns,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        silent_seconds: float = SILENT_SECONDS,
    ) -> None:
        self.make_feed, self.heard, self.flush = make_feed, heard, flush
        self.clock_ns, self.sleep, self.silent_seconds = clock_ns, sleep, silent_seconds
        self.wanted: list[str] = []
        self.session = Session()
        self.host = ""
        self._changed = asyncio.Event()
        self._restart = False
        #: Called each time a session opens anew after the first, with the
        #: moment the last one was last heard: the gap to catch up.
        self.reopened: Callable[[int], Awaitable[None]] | None = None

    def want(self, symbols: Sequence[str]) -> None:
        """Stream these, and only these, from now on."""
        if list(symbols) != self.wanted:
            self.wanted = list(symbols)
            self._changed.set()

    def restart(self) -> None:
        """Open the session again: the key or the plan changed."""
        self._restart = True
        self._changed.set()

    async def _idle(self, state: str) -> None:
        self.session.state = state
        self.session.subscribed, self.session.symbols = 0, []
        self._changed.clear()
        self._restart = False
        await self._changed.wait()

    async def run(self, sessions: int | None = None) -> None:
        """Forever (or for `sessions` sessions, in the tests)."""
        wait = FIRST_WAIT
        held = 0
        while sessions is None or held < sessions:
            if not self.wanted:
                await self._idle("idle")
                continue
            feed = self.make_feed()
            if feed is None:
                await self._idle("no key")
                continue
            held += 1
            self._restart = False
            self.host = str(getattr(feed, "host", ""))
            try:
                await self._session(feed)
                wait = FIRST_WAIT
                continue
            except Restart:
                wait = FIRST_WAIT
                continue
            except gateway.Refused as refused:
                self.session.state, self.session.last_error = "refused", f"refused: {refused}"
                log.warning("Databento refused the stream: %s", refused)
                await self._pause(REFUSED_WAIT)
                continue
            except (gateway.GatewayError, OSError, TimeoutError, ValueError) as broken:
                self.session.last_error = f"{type(broken).__name__}: {broken}"[:300]
                log.warning("the stream stopped: %s", self.session.last_error)
            finally:
                await feed.close()
            self.session.reconnects += 1
            self.session.state = "waiting"
            await self._pause(wait)
            wait = min(wait * 2, MOST_WAIT)

    async def _pause(self, seconds: float) -> None:
        """Wait, or less where what is wanted changes meanwhile."""
        if self._restart:
            return
        changed = asyncio.ensure_future(self._changed.wait())
        slept = asyncio.ensure_future(self.sleep(seconds))
        try:
            await asyncio.wait({changed, slept}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            changed.cancel()
            slept.cancel()

    async def _session(self, feed: Feed) -> None:
        self.session.state = "connecting"
        await feed.open()
        last_heard = self.session.last_message_at_ns
        self.session.opened += 1
        self.session.since_ns = self.clock_ns()
        self._changed.clear()
        await feed.subscribe(self.wanted)
        self._note_subscribed()
        self.session.state = "streaming"
        if self.session.opened > 1 and last_heard and self.reopened is not None:
            await self.reopened(last_heard)
        pending: asyncio.Future[list[Any]] | None = None
        quiet_since = time.monotonic()
        try:
            while True:
                if pending is None:
                    pending = asyncio.ensure_future(feed.receive())
                changed: asyncio.Future[Any] = asyncio.ensure_future(self._changed.wait())
                done, _ = await asyncio.wait(
                    {pending, changed}, timeout=FLUSH_SECONDS, return_when=asyncio.FIRST_COMPLETED
                )
                if changed in done:
                    self._changed.clear()
                    if self._restart:
                        raise Restart()
                    if not self.wanted:
                        return
                    if pending is not None and not pending.done():
                        # The session may be opened again to let a listing go:
                        # the read in flight is of the session being closed.
                        pending.cancel()
                        with contextlib.suppress(BaseException):
                            await pending
                        pending = None
                    await feed.subscribe(self.wanted)
                    self._note_subscribed()
                else:
                    changed.cancel()
                if pending is not None and pending in done:
                    stated = pending.result()
                    pending = None
                    quiet_since = time.monotonic()
                    now = self.clock_ns()
                    self.session.messages += 1
                    self.session.last_message_at_ns = now
                    said = getattr(feed, "said", "")
                    if said:
                        self.session.last_error = said
                    await self.heard(stated)
                elif time.monotonic() - quiet_since > self.silent_seconds:
                    raise gateway.GatewayError(
                        f"nothing heard for {self.silent_seconds:.0f} seconds"
                    )
                await self.flush()
        finally:
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(BaseException):
                    await pending
            await self.flush()

    def _note_subscribed(self) -> None:
        self.session.subscribed = len(self.wanted)
        self.session.symbols = list(self.wanted)
