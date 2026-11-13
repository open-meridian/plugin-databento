"""The Databento plugin: Databento's US equity and OPRA option market data in
an Open Meridian deployment's lake, as a data plugin.

It runs as the SDK runs a plugin (open-meridian 0.35, `meridian.run`): its
data role's class (`Market`, the SDK's listings feed with Databento's own,
lake.py) registered on the sidecar it is launched beside with its whole
declaration (declaration.py) -- its catalogue, its storage, its settings,
its pages (page.py) and its documentation -- and the pages served. On each delivery of
its settings it makes its Databento clients again with the key (or, with
sample data, the built-in exchange), sets what each asset class's plan
reaches, and asks whether Databento answers the key; it reads which of its
datasets a deployment admin has licensed (contract v20) and reports the data
role's figures; then it hears what the lake wants of it and records against
each want or declines it per subject (lake.py, chains.py), and keeps its
timetable: standing wants kept current, days still forming read again, a
final close pushed, the chains due read. While a standing want stands, on a
real-time plan, it streams Databento's trades and best bid and offer for
the listings wanted, and the option trades of the series wanted, from the
live gateway (stream.py), each stream kept open by the SDK's supervisor,
reconnecting when a session breaks and catching the gap up from the history
and the gateway's intraday replay.

It holds nothing it cannot lose: which listing each subject is, and every
raw answer, are in the storage the deployment grants it (decisions/028); a
restart reads them back and hears the lake again. The key arrives as a
secret setting, never logged, shown or sent anywhere but Databento.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import meridian
from meridian import edge
from meridian.edge import RawStore, storage_root
from meridian.roles import Latency

from .declaration import DECLARATION, PORT
from .declared import KIND
from .lake import Lake, Products
from .page import State, figures, pages, use
from .settings import REAL_TIME, Credentials, config_from
from .source import DatabentoSource, gateway_replay
from .stream import DatabentoFeed, Streamer
from .synthetic import SyntheticGateway, SyntheticTransport, connect, connect_async, exchange
from .vendor import EQUS_MINI, OPRA, Historical

log = logging.getLogger("databento")

#: How often the timetable is looked at.
TICK_SECONDS = 15
#: How often the datasets' licences are read again.
LICENCES_SECONDS = 60
#: The dataset streamed for each asset class.
STREAMED = {"stocks": EQUS_MINI, "options": OPRA}

#: The synthetic gateway sample data streams from, one for the process.
_synthetic_gateway: SyntheticGateway | None = None


def synthetic_gateway() -> SyntheticGateway:
    global _synthetic_gateway
    if _synthetic_gateway is None:
        _synthetic_gateway = SyntheticGateway()
    return _synthetic_gateway


def source_for(state: State, store: RawStore) -> tuple[Historical | None, DatabentoSource | None]:
    """The clients and the source the settings give: sample data, the key's,
    or none while there is no key."""
    config = state.config
    if config.sample_data:
        historical = Historical(store, None, transport=SyntheticTransport(), keyless=True)
        sample = Credentials("synthetic-key-never-sent")
        replay = gateway_replay(sample, connect(synthetic_gateway()))
        source = DatabentoSource(historical, replay, store, synthetic=True)
    elif config.credentials is not None:
        historical = Historical(store, config.credentials)
        source = DatabentoSource(historical, gateway_replay(config.credentials), store)
    else:
        return None, None
    source.gateway = {
        asset: config.timeliness(asset) == REAL_TIME for asset in ("stocks", "options")
    }
    return historical, source


def feed_for(state: State, store: RawStore, asset_class: str) -> DatabentoFeed | None:
    """A session on the live gateway of an asset class's dataset, as the
    settings give it: none on a historical-only plan or with no key."""
    config = state.config
    if config.timeliness(asset_class) != REAL_TIME:
        return None
    if config.sample_data:
        return DatabentoFeed(
            asset_class, STREAMED[asset_class], Credentials("synthetic-key-never-sent"), store,
            connect=connect_async(synthetic_gateway()),
        )  # fmt: skip
    if config.credentials is None:
        return None
    return DatabentoFeed(asset_class, STREAMED[asset_class], config.credentials, store)


async def read_licences(role: Any, state: State) -> None:
    """Which of its own declared datasets a deployment admin has licensed
    (contract v20, W10.9), for the Datasets page."""
    answered = await role.list_datasets()
    state.licensed = frozenset(licence.dataset for licence in answered.licences)


def streams(state: State, store: RawStore, lake: Lake) -> tuple[Streamer, Streamer]:
    """The two streams, each kept open by the SDK's supervisor: stocks into
    the listings feed, a reconnect's gap caught up from the history and the
    gateway's intraday replay; options into the chains."""
    stocks = Streamer(
        lambda: feed_for(state, store, "stocks"),
        lake.heard,
        lake.flush,
        caught_up=lake.caught_up,
        latency=lake.latency,
        session="stocks",
    )
    options = Streamer(
        lambda: feed_for(state, store, "options"),
        lake.chains.heard,
        lake.chains.flush,
        latency=lake.latency,
        session="options",
    )
    lake.streamer, lake.chains.streamer = stocks, options
    return stocks, options


class Market(Lake):
    """The data role's class: Databento's listings and option chains in the
    lake (lake.py, chains.py); the settings applied as they are delivered;
    and in `run` its two streams kept open, its timetable kept, its
    licences read and its latency reported -- each restarted by
    `meridian.run` after a backoff should it stop."""

    def __init__(self) -> None:
        granted = edge.storage_dir()
        super().__init__(
            None,
            Products(granted / "products.json" if granted else None),
            series_path=granted / "series.json" if granted else None,
        )
        self.store = RawStore(storage_root("databento-raw-"), kind=KIND)
        # The sessions' latency (contract v27) and each live row's age at
        # the edge (contract v28), reported every ten seconds through this
        # class (SDK 0.35).
        self.latency = Latency()
        self.state = State(lake=self, latency=self.latency)
        self.state.stocks, self.state.options = streams(self.state, self.store, self)

    async def on_settings(self, delivered: Any) -> None:
        """One delivery of the settings, the first before anything is heard:
        the clients made again, the plans set, the streams restarted, and the
        raw answers past their window moved. The key's value is never
        logged: only whether it is set."""
        state = self.state
        state.config = config_from(delivered.values, delivered.missing)
        config = state.config
        log.info(
            "settings delivered: sample data %s, a key %s, stocks %s, options %s",
            config.sample_data, "set" if config.token_set else "not set",
            config.timeliness("stocks"), config.timeliness("options"),
        )  # fmt: skip
        state.historical, state.source = source_for(state, self.store)
        self.source = state.source
        self.use_plan(config.timeliness("stocks"), config.timeliness("options"))
        if config.sample_data:
            # The built-in exchange's listings, as a source's own list.
            await self.seed_symbols(sorted(exchange()["daily"]))
        await self.health()
        self.restream()
        self.chains.restream()
        for streamer in (state.stocks, state.options):
            if streamer is not None:
                streamer.restart()
        try:
            await self.store.tend(self.plugin, settings=delivered)
        except (meridian.MeridianError, OSError, ValueError) as failed:
            log.warning("the raw answers were not tended this time: %s", failed)

    async def health(self) -> None:
        """Say how the source is: a missing key or a refused one is the
        plugin's health, never a ticket."""
        state = self.state
        if state.config.sample_data:
            await self.say(True, "sample data: the built-in exchange")
            return
        source = state.source
        if source is None:
            await self.say(False, "no Databento API key: enter it in Settings")
            return
        try:
            answer = await asyncio.to_thread(source.probe, True)
        except OSError as failed:
            await self.say(False, f"Databento did not answer: {failed}")
            return
        if not answer.answered:
            await self.say(False, f"Databento refused the key: {answer.detail}")
            return
        await self.say(True, "answering")

    async def say(self, healthy: bool, detail: str) -> None:
        """The plugin's health, with the data role's figures as they stand."""
        await self.plugin.report(healthy=healthy, detail=detail[:200], figures=figures(self.state))

    async def on_start(self) -> None:
        self.latency.role = self

    async def run(self) -> None:
        state = self.state
        assert state.stocks is not None and state.options is not None
        await asyncio.gather(
            state.stocks.run(),
            state.options.run(),
            self._timetable(),
            self._licences(),
            self.latency.run(),
        )

    async def _timetable(self) -> None:
        while True:
            try:
                await self.tick()
            except meridian.MeridianError as failed:
                log.warning("the timetable stopped this time: %s", failed)
            await asyncio.sleep(TICK_SECONDS)

    async def _licences(self) -> None:
        """The licences, read on start and every minute."""
        while True:
            try:
                await read_licences(self, self.state)
            except meridian.MeridianError as failed:
                log.warning("the datasets' licences were not read this time: %s", failed)
            await asyncio.sleep(LICENCES_SECONDS)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    market = Market()
    use(market.state)
    meridian.run(
        DECLARATION,
        roles=[market],
        pages=pages,
        port=PORT,
        on_settings=market.on_settings,
        figures=lambda: figures(market.state),
    )


if __name__ == "__main__":
    main()
