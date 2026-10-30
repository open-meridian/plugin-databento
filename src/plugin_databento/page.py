"""The plugin's pages, under Manage alone: the data role's common pages
(spec/plugin-pages-follow-their-role), Connection and Datasets, in its
order, each a read tool on the deployment's MCP surface answering what the
page shows under the role's names (`read_connection`, `read_datasets`).
Each record is the role's (`meridian.roles.data`), extended with Databento's
fields; each template is the role's, with Databento's nuance at its blocks
only. A data plugin shows no market data: prices, trades and quotes are the
reading roles' to read, under their entitlement.

- **Connection** (`/`): one group at a time under the role's tab row --
  "Key" (the mode, the key set or not, never shown, what Databento answers
  it, each asset class's plan as the settings say it), "Limits and use"
  (the pace kept, the calls, the records returned, the replays, the last
  error), "Stream" (each dataset's live session) and "Sessions" once
  measured.
- **Datasets** (`/datasets`): "Served" -- the four datasets as the catalogue
  declares them, licensed or not, the source behind each and the Databento
  dataset it reads; "Wants", the latest few drawn (the tool answers every
  one); "Subjects".

Each page fits one screen at 375, 582 and 1280 pixels wide: one-line rows,
the details a click away.
"""

from __future__ import annotations

import datetime as dt
import urllib.parse
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import meridian
from meridian.plugin.v1 import operations_pb2 as ops
from meridian.roles import Latency, data

from .chains import OPTIONS
from .convert import NEW_YORK
from .declared import CATALOGUE, SOURCES
from .lake import DAILY, LIVE, PRICE, QUOTE, TRADES, Lake
from .records import ConnectionRead, DatasetsRead, Served, SessionRead, Want
from .settings import REAL_TIME, SAID, Config
from .source import DatabentoSource
from .stream import Streamer
from .vendor import NS, PER_MINUTE, Historical, Usage

TITLE = "Databento"
CONNECTION = "/"
DATASETS = "/datasets"
#: The kit the pages are built on: 0.12.0's "Facts" and "Next step".
KIT = "0.12.0"
#: The standing wants a browser is shown, the latest; the tool answers all.
SHOWN_WANTS = 3
TABS = ("Key", "Limits and use", "Stream")

pages = meridian.Pages(TITLE, templates=Path(__file__).parent / "templates", kit=KIT)


@dataclass
class State:
    """What the pages read: the settings, the lake's half, the source and
    the streams."""

    config: Config = field(default_factory=Config)
    lake: Lake | None = None
    source: DatabentoSource | None = None
    historical: Historical | None = None
    stocks: Streamer | None = None
    options: Streamer | None = None
    #: The datasets a deployment admin has licensed (contract v20), as read.
    licensed: frozenset[str] = frozenset()
    #: The sessions' latency (contract v27) and each live row's age at the
    #: edge (contract v28), reported every ten seconds.
    latency: Latency | None = None


_state = State()


def current() -> State:
    return _state


def use(state: State) -> None:
    """The state the pages read from now on."""
    global _state
    _state = state


def _when(at_ns: int) -> str:
    if not at_ns:
        return ""
    moment = dt.datetime.fromtimestamp(at_ns / NS, dt.UTC)
    return moment.isoformat(timespec="seconds").replace("+00:00", "Z")


def shown(at: str) -> str:
    """A moment as the page shows it: New York's time, to the minute."""
    if not at:
        return ""
    moment = dt.datetime.fromisoformat(at.replace("Z", "+00:00")).astimezone(NEW_YORK)
    return moment.strftime("%b %d %H:%M ET")


pages.environment.filters["shown"] = shown


def host(state: State) -> str:
    """The host Databento is asked at, or none: sample data asks none, and
    there is no client while there is no key."""
    if state.config.sample_data or state.historical is None:
        return ""
    return urllib.parse.urlparse(state.historical.base).hostname or ""


def _stream(asset: str, streamer: Streamer | None, plan: str, sample: bool) -> SessionRead:
    if plan != REAL_TIME:
        return SessionRead(asset, "", "does not stream on a historical-only plan")
    if streamer is None:
        return SessionRead(asset)
    held = streamer.session
    return SessionRead(
        asset_class=asset,
        host="" if sample else streamer.host,
        state=held.state,
        subscribed=held.subscribed,
        opened=held.opened,
        reconnects=held.reconnects,
        messages=held.messages,
        since=_when(held.since_ns),
        last_message_at=_when(held.last_message_at_ns),
        last_error=held.last_error,
    )


def connection_read(state: State) -> ConnectionRead:
    config = state.config
    usage = state.historical.usage if state.historical is not None else Usage()
    source = state.source
    known = source.known if source is not None else None
    stocks, options = config.timeliness("stocks"), config.timeliness("options")
    if config.sample_data:
        mode, detail = "Sample data", "Recorded records; Databento is not called."
    elif config.credentials is None:
        mode, detail = "No key", "Enter the API key in Settings."
    elif known is None:
        mode, detail = "Live", "Not asked yet: Databento is asked when the key is saved."
    elif not known.answered:
        mode, detail = "Live", f"Databento refused the key: {known.detail}"
    else:
        mode, detail = "Live", "Databento answers the key."
    answered = config.sample_data or bool(known and known.answered)
    entitled = (
        f"Stocks: {SAID[stocks].lower()}; options: {SAID[options].lower()}, as the plans "
        "are set in Settings"
        if answered
        else ""
    )
    streams = [
        _stream("stocks", state.stocks, stocks, config.sample_data),
        _stream("options", state.options, options, config.sample_data),
    ]
    open_sessions = sum(1 for s in streams if s.state == "streaming")
    errors = [s.last_error for s in streams if s.last_error and s.state != "streaming"]
    lake = state.lake
    replay_error = source.replay_error if source is not None else ""
    return ConnectionRead(
        mode=mode,
        credentials=[data.Credential("API key", config.token_set)],
        entitled_at_vendor=entitled,
        limit=(
            f"At most {PER_MINUTE} historical calls a minute: Databento allows 100 time "
            "series requests a second; historical data is billed by volume"
        ),
        calls=usage.calls,
        rate_limited=usage.rate_limited,
        last_call_at=_when(usage.last_call_at_ns),
        last_error=usage.last_error
        or (lake.last_error if lake else "")
        or replay_error
        or next(iter(errors), ""),
        sessions=None if stocks != REAL_TIME and options != REAL_TIME else open_sessions,
        session_latency=state.latency.session_latency() if state.latency is not None else [],
        host=host(state),
        detail=detail,
        stocks_plan=SAID[stocks],
        options_plan=SAID[options],
        records=usage.records,
        replays=source.replays if source is not None else 0,
        last_status=usage.last_status,
        streams=streams,
    )


SERVES = {
    DAILY: "closes and daily bars",
    LIVE: "last, bid and ask; quotes",
    TRADES: "trades",
    OPTIONS: "option chains: series, open interest, volume; quotes and trades in real time",
}
#: Each dataset's asset class.
ASSET = {DAILY: "stocks", LIVE: "stocks", TRADES: "stocks", OPTIONS: "options"}


def _terms(declared: Any) -> str:
    licence = declared.licence_default
    if licence is None or not licence.kept:
        kept = "served, not kept"
    else:
        kept = f"kept {licence.retention_days} days" if licence.retention_days else "kept"
    return kept + ("; one person's" if licence is not None and licence.personal_use else "")


def _mode(mode: object) -> str:
    if isinstance(mode, int):
        return str(ops.ObservationMode.Name(mode)).removeprefix("OBSERVATION_MODE_").lower()
    return str(mode)


def at_vendor(state: State, key: str, connection: ConnectionRead) -> str:
    """Whether the plans reach a dataset, in a word or two."""
    plan = state.config.timeliness(ASSET[key])
    if key == LIVE and plan != REAL_TIME:
        return "No: the Stocks plan is historical only"
    if connection.mode == "Sample data":
        return "Yes"
    if connection.mode == "No key" or not connection.entitled_at_vendor:
        return "Not asked yet"
    if key == TRADES and plan != REAL_TIME:
        return "History only: the plan is historical only"
    if key == OPTIONS and plan != REAL_TIME:
        return "Series and statistics only: the plan is historical only"
    return "Yes, as the plan is set"


def datasets_read(state: State) -> DatasetsRead:
    lake = state.lake
    connection = connection_read(state)
    served: list[Served] = []
    for declared in CATALOGUE:
        counts = lake.counts.get(declared.key) if lake is not None else None
        dataset = lake.named(declared.key) if lake is not None else declared.key
        served.append(
            Served(
                dataset=dataset,
                data=SERVES.get(declared.key, declared.key),
                arrives=", ".join(_mode(m) for m in declared.modes),
                day="New York's, to 16:00",
                default_terms=_terms(declared),
                entitled_at_vendor=at_vendor(state, declared.key, connection),
                recorded=counts.recorded if counts else 0,
                declined=counts.declined if counts else 0,
                last_recorded=_when(counts.last_recorded_at_ns) if counts else "",
                licensed=dataset in state.licensed,
                key=declared.key,
                feed=SOURCES[declared.key][1],
                source=SOURCES[declared.key][0],
                restated=counts.restated if counts else 0,
                unchanged=counts.unchanged if counts else 0,
                unconverted=counts.unconverted if counts else 0,
            )
        )
    wants: list[Want] = []
    if lake is not None:
        streamed = set(lake.streamed)
        for standing in reversed(list(lake.standing.values())):
            listings = [lake.products.symbols.get(s, s) for s in standing.subjects]
            on_stream = bool(listings) and all(s in streamed for s in listings)
            answered = lake.by_want.get(standing.want_id, [0, 0])
            if standing.data_type == PRICE and standing.dataset == LIVE:
                said = f"Kept current, {shown(_when(standing.last_at_ns))}"
            else:
                said = "Streamed" if on_stream else "Not streamed"
            wants.append(
                Want(
                    dataset=lake.dataset(standing.dataset),
                    asked=_asks(standing.data_type, standing.kinds),
                    subjects=len(standing.subjects),
                    recorded=answered[0],
                    declined=answered[1],
                    state=said,
                    want_id=standing.want_id,
                    listings=listings,
                    streamed=on_stream,
                )
            )
        chained: dict[str, tuple[list[str], bool]] = {}
        for held in lake.chains.underlyings.values():
            ids = {*held.standing, *held.trades, *held.series, *held.statistics}
            for want_id in sorted(ids):
                symbols, streams = chained.get(want_id, ([], False))
                chained[want_id] = ([*symbols, held.symbol], streams or want_id in held.trades)
        for want_id, (symbols, streams) in chained.items():
            read = [
                held.quoted_at
                for held in lake.chains.underlyings.values()
                if want_id in held.standing
            ]
            answered = lake.by_want.get(want_id, [0, 0])
            wants.append(
                Want(
                    dataset=lake.chains.dataset(),
                    asked="option trades" if streams else "option chains",
                    subjects=len(symbols),
                    recorded=answered[0],
                    declined=answered[1],
                    state="Streamed"
                    if streams
                    else f"Kept current, {shown(_when(max(read, default=0)))}",
                    want_id=want_id,
                    listings=sorted(symbols),
                    streamed=streams,
                )
            )
    return DatasetsRead(
        served=served,
        wants=wants,
        reads_instruments=lake.reads_instruments if lake is not None else True,
        listings=len(lake.products.symbols) if lake is not None else 0,
        forming=len(lake.forming) if lake is not None else 0,
        streamed=len(lake.streamed) if lake is not None else 0,
        series_streamed=len(lake.chains.streamed) if lake is not None else 0,
        chains=len(lake.chains.underlyings) if lake is not None else 0,
        backfills_a_restart=True,
        calendar_error=lake.chains.calendar_error if lake is not None else "",
    )


def _asks(data_type: str, kinds: tuple[str, ...]) -> str:
    if data_type == PRICE:
        return ", ".join(kinds) or "prices"
    return "quotes" if data_type == QUOTE else "trades"


def status(read: ConnectionRead) -> dict[str, str]:
    """The head's status, the role's Connection words."""
    if read.mode == "Sample data":
        return {"state": "ok", "label": "Sample data"}
    if read.mode == "No key":
        return {"state": "warn", "label": "No key"}
    if not read.entitled_at_vendor:
        return {"state": "error", "label": "Not answering"}
    return {"state": "warn" if read.last_error else "ok", "label": "Answering"}


def figures(state: State) -> list[meridian.Figure]:
    """The data role's figures, in its order (`meridian.roles.data`);
    Databento reports none of its own."""
    read = connection_read(state)
    word = status(read)["label"]
    connection: Any = word
    if word == "Not answering":
        connection = meridian.Figure("", word, state="error", why=read.detail[:200])
    elif word == "Answering" and read.last_error:
        connection = meridian.Figure("", word, state="warn", why=read.last_error[:200])
    served = datasets_read(state).served
    ok = [s for s in served if not s.entitled_at_vendor.startswith(("No", "Not"))]
    entitled: Any = None
    if read.mode != "No key":
        entitled = f"{len(ok)} of {len(served)} datasets"
    streams = [s for s in read.streams if s.state == "streaming"]
    if read.sessions is None:
        streaming: Any = "Does not stream"
    elif streams:
        streaming = f"{sum(s.subscribed for s in streams)} streamed"
    else:
        broken = next((s for s in read.streams if s.state in ("waiting", "refused")), None)
        streaming = (
            meridian.Figure(
                "", broken.state.capitalize(), state="warn", why=broken.last_error[:200]
            )
            if broken is not None
            else "Off"
        )
    last = max((s.last_recorded for s in served), default="")
    used = state.historical.usage if state.historical is not None else Usage()
    now = state.historical.clock_ns() if state.historical is not None else 0
    spent = used.last_minute(now) if now else 0
    return data.figures(
        connection=connection,
        entitled_at_vendor=entitled,
        stream=streaming,
        last_recorded=dt.datetime.fromisoformat(last.replace("Z", "+00:00")) if last else None,
        rate_use=meridian.Figure(
            "",
            f"{spent} of {PER_MINUTE} a minute",
            state="warn" if spent * 5 >= PER_MINUTE * 4 else None,
            why=f"{used.rate_limited} answered 429 and waited out" if used.rate_limited else "",
        ),
    )


pages.environment.globals["connection_status"] = lambda read: status(read)


@pages.page(
    CONNECTION,
    "Connection",
    levels="admin",
    roles="data",
    answers=ConnectionRead,
    name="read_connection",
    description=(
        "How this plugin reaches Databento, as every data plugin's Connection answers it: the "
        "mode (Live, Sample data or No key), the API key set or not (never shown), what the "
        "plans reach as Settings say them, the pace kept and the calls made, the last call "
        "and error, the streams open. Then Databento's own: the historical host, each asset "
        "class's plan, the records returned and the live replays made, the last status, and "
        "each live session. No price."
    ),
)
async def connection(request: meridian.Request) -> meridian.Response:
    read = connection_read(_state)
    tabs = [*TABS, *(["Sessions"] if read.session_latency else [])]
    asked = request.query.get("tab", "").replace("-", " ").lower()
    tab = next((t for t in tabs if t.lower() == asked), tabs[0])
    return pages.answer("connection.html", read, tabs=tabs, tab=tab)


@pages.page(
    DATASETS,
    "Datasets",
    levels="admin",
    roles="data",
    answers=DatasetsRead,
    name="read_datasets",
    description=(
        "The datasets the plugin serves the lake, as every data plugin's Datasets answers "
        "them: each one as its catalogue declares it, whether a deployment admin has "
        "licensed it, whether the plans reach it, and what it recorded and declined since "
        "the plugin started; the standing wants it keeps current; whether it may read an "
        "instrument's record. Then Databento's own: the source behind each dataset and the "
        "Databento dataset it reads, the listings known and streamed, the option chains "
        "kept and series streamed, and that a restart's gap is caught up. No prices, trades "
        "or quotes."
    ),
)
async def datasets(request: meridian.Request) -> meridian.Response:
    read = datasets_read(_state)
    if request.tool_name:
        return pages.answer("datasets.html", read, wants=len(read.wants), served=len(read.served))
    # A browser draws the latest few wants, so the page fits one screen at
    # 375 pixels; the tool answers every one.
    drawn = replace(read, wants=read.wants[:SHOWN_WANTS])
    return pages.answer("datasets.html", drawn, wants=len(read.wants), served=len(read.served))


def serve(plugin: meridian.Plugin, state: State, port: int) -> Any:
    """Serve the pages on loopback beside the sidecar, reading `state`."""
    use(state)
    return pages.serve(plugin, port)
