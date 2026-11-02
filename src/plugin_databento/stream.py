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

**Reconnect**, the SDK's supervisor's from 0.2.0 (`helpers.sessions`,
ruling 1027). A session that breaks, closes, errs fatally or says nothing
for a heartbeat and ten seconds (`SILENT_SECONDS`, Databento's own advice)
is opened again a second later, then doubling to a minute, each wait within
a fifth of itself; the
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

import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol

from meridian.edge import RawStore
from meridian.helpers import sessions

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


class Streamer(sessions.Supervisor):
    """One of Databento's live gateways kept open by the SDK's supervisor
    (open-meridian 0.35, ruling 1027: `helpers.sessions.Supervisor` over the
    plugin's own feed, its `Link`): one session while anything is wanted,
    subscribed to what is; what it hears handed to `heard` and flushed with
    `flush`; after a reopen, the gap since each listing was last heard
    handed to `caught_up` (the history and the gateway's intraday replay). A refused key is
    waited out `REFUSED_WAIT`; any other break the supervisor's backoff,
    a second doubling to a minute. Every line is handed on, one that
    converted to nothing (a heartbeat, a system message) as an empty list:
    the feed's sign of life. What the gateway says of a slow reader or an
    error it does not end the session on (the feed's `said`) is the
    session's last error, the session kept."""

    def __init__(
        self,
        make_feed: Callable[[], Feed | None],
        heard: Callable[[list[Any]], Awaitable[Any]],
        flush: Callable[[], Awaitable[Any]],
        **kwargs: Any,
    ) -> None:
        self._feed: Feed | None = None
        self._handed = 0
        #: The gateway the last session was made for, for the Connection page.
        self.host = ""

        def made() -> Feed | None:
            self._feed = make_feed()
            if self._feed is not None:
                self.host = str(getattr(self._feed, "host", ""))
            return self._feed

        async def told(stated: list[Any]) -> None:
            self._handed = self.state.messages
            await heard(stated)

        async def flushed() -> None:
            # A message that converted to nothing (a status) is still the
            # stream's sign of life: handed on empty, as each was before.
            if self._handed != self.state.messages:
                self._handed = self.state.messages
                await heard([])
            said = getattr(self._feed, "said", "")
            if said:
                self.state.last_error = said
            await flush()

        kwargs.setdefault("silent_seconds", SILENT_SECONDS)
        kwargs.setdefault("backoff", sessions.Backoff(first_s=FIRST_WAIT, most_s=MOST_WAIT))
        super().__init__(
            made,
            heard=told,
            flush=flushed,
            refused=(gateway.Refused,),
            refused_seconds=REFUSED_WAIT,
            flush_seconds=FLUSH_SECONDS,
            **kwargs,
        )
