"""The Databento plugin: Databento's US equity and OPRA option market data in
an Open Meridian deployment's lake, as a data plugin.

It connects to the sidecar it is launched beside with its whole declaration
(declaration.py): its catalogue, its storage, its settings, its pages
(page.py) and its documentation; and serves the pages. On each delivery of
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
live gateway (stream.py), reconnecting when a session breaks and catching
the gap up from the history and the gateway's intraday replay.

It holds nothing it cannot lose: which listing each subject is, and every
raw answer, are in the storage the deployment grants it (decisions/028); a
restart reads them back and hears the lake again. The key arrives as a
secret setting, never logged, shown or sent anywhere but Databento.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

import meridian
from meridian import edge
from meridian.edge import RawStore, storage_root
from meridian.roles import Latency

from .declaration import DECLARATION, PORT
from .declared import KIND, WINDOW_DAYS
from .lake import Lake, Products
from .page import State, figures, serve
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


async def health(plugin: meridian.Plugin, state: State) -> None:
    """Say how the source is, on the heartbeat: a missing key or a refused one
    is the plugin's health, never a ticket."""
    if state.config.sample_data:
        await say(plugin, state, True, "sample data: the built-in exchange")
        return
    source = state.source
    if source is None:
        await say(plugin, state, False, "no Databento API key: enter it in Settings")
        return
    try:
        answer = await asyncio.to_thread(source.probe, True)
    except OSError as failed:
        await say(plugin, state, False, f"Databento did not answer: {failed}")
        return
    if not answer.answered:
        await say(plugin, state, False, f"Databento refused the key: {answer.detail}")
        return
    await say(plugin, state, True, "answering")


async def say(plugin: meridian.Plugin, state: State, healthy: bool, detail: str) -> None:
    """The plugin's health, with the data role's figures as they stand."""
    await plugin.report(healthy=healthy, detail=detail[:200], figures=figures(state))


async def read_licences(plugin: Any, state: State) -> None:
    """Which of its own declared datasets a deployment admin has licensed
    (contract v20, W10.9), for the Datasets page."""
    answered = await plugin.list_datasets()
    state.licensed = frozenset(licence.dataset for licence in answered.licences)


async def licences(plugin: meridian.Plugin, state: State) -> None:
    while True:
        try:
            await read_licences(plugin, state)
        except meridian.MeridianError as failed:
            log.warning("the datasets' licences were not read this time: %s", failed)
        await asyncio.sleep(LICENCES_SECONDS)


async def apply(plugin: meridian.Plugin, state: State, store: RawStore, delivered: Any) -> None:
    """One delivery of the settings: the clients made again, the plans set,
    the streams restarted, and the raw answers past their window moved. The
    key's value is never logged: only whether it is set."""
    state.config = config_from(delivered.values, delivered.missing_required)
    config = state.config
    log.info(
        "settings delivered: sample data %s, a key %s, stocks %s, options %s",
        config.sample_data, "set" if config.token_set else "not set",
        config.timeliness("stocks"), config.timeliness("options"),
    )  # fmt: skip
    state.historical, state.source = source_for(state, store)
    lake = state.lake
    if lake is not None:
        lake.source = state.source
        lake.use_plan(config.timeliness("stocks"), config.timeliness("options"))
        if config.sample_data:
            # The built-in exchange's listings, as a source's own list.
            await lake.seed(sorted(exchange()["daily"]))
    await health(plugin, state)
    if lake is not None:
        lake.restream()
        lake.chains.restream()
    for streamer in (state.stocks, state.options):
        if streamer is not None:
            streamer.restart()
    window = delivered.values.get(f"{KIND}_window_days")
    past = delivered.values.get(f"{KIND}_past_window")
    try:
        await store.tend(
            plugin,
            window if isinstance(window, int) else WINDOW_DAYS,
            past if isinstance(past, str) else "kept",
            datetime.now(UTC).date(),
        )
    except (meridian.MeridianError, OSError) as failed:
        log.warning("the raw answers were not tended this time: %s", failed)


async def watch_settings(
    plugin: meridian.Plugin, state: State, store: RawStore, configured: asyncio.Event
) -> None:
    async for delivered in plugin.settings():
        await apply(plugin, state, store, delivered)
        configured.set()


async def timetable(plugin: meridian.Plugin, state: State, lake: Lake) -> None:
    while True:
        try:
            await lake.tick()
        except meridian.MeridianError as failed:
            log.warning("the timetable stopped this time: %s", failed)
        plugin.figures = figures(state)
        await asyncio.sleep(TICK_SECONDS)


async def again(work: Callable[[], Coroutine[Any, Any, None]], what: str) -> None:
    """A stream that stops (the deployment still starting, a sidecar
    restarted) is said and opened again, a little later each time."""
    wait = 1.0
    while True:
        try:
            await work()
            return
        except asyncio.CancelledError:
            raise
        except Exception as failed:  # noqa: BLE001 - said, and opened again
            log.warning("%s stopped (%s); again in %.0fs", what, failed, wait)
        await asyncio.sleep(wait)
        wait = min(wait * 2, 30.0)


def streams(state: State, store: RawStore, lake: Lake) -> tuple[Streamer, Streamer]:
    """The two streams: stocks into the listings feed, options into the
    chains; a stocks reconnect's gap caught up from the history and the
    gateway's intraday replay."""
    stocks = Streamer(lambda: feed_for(state, store, "stocks"), lake.heard, lake.flush)

    async def reopened(last_heard_ns: int) -> None:
        await lake.caught_up({symbol: last_heard_ns for symbol in lake.streamed})

    stocks.reopened = reopened
    options = Streamer(
        lambda: feed_for(state, store, "options"), lake.chains.heard, lake.chains.flush
    )
    lake.streamer, lake.chains.streamer = stocks, options
    return stocks, options


async def run() -> None:
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for stop in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(stop, stopped.set)

    granted = edge.storage_dir()
    store = RawStore(storage_root("databento-raw-"), kind=KIND)
    async with await meridian.connect(declaration=DECLARATION) as plugin:
        log.info("registered as %s", plugin.identity.instance_id)
        lake = Lake(plugin, None, Products(granted / "products.json" if granted else None))
        latency = Latency(plugin)
        lake.latency = latency
        state = State(lake=lake, latency=latency)
        state.stocks, state.options = streams(state, store, lake)
        configured = asyncio.Event()
        page = serve(plugin, state, PORT)
        tasks = [
            asyncio.create_task(
                again(lambda: watch_settings(plugin, state, store, configured), "the settings")
            )
        ]
        await configured.wait()

        async def hear() -> None:
            await plugin.receive(observations_wanted=lake.on_want, want_withdrawn=lake.on_withdrawn)

        tasks += [
            asyncio.create_task(again(hear, "hearing the lake")),
            asyncio.create_task(again(lambda: licences(plugin, state), "the licences")),
            asyncio.create_task(again(lambda: timetable(plugin, state, lake), "the timetable")),
            asyncio.create_task(again(state.stocks.run, "the stocks stream")),
            asyncio.create_task(again(state.options.run, "the options stream")),
            asyncio.create_task(again(latency.run, "the latency reports")),
        ]
        log.info("serving its pages on 127.0.0.1:%d", PORT)
        await stopped.wait()
        log.info("stopping")
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        page.shutdown()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    asyncio.run(run())


if __name__ == "__main__":
    main()
