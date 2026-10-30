"""What the pages' read tools answer (a page and its tool read one source,
`pages.answer`): the data role's common records (`meridian.roles.data`,
spec/plugin-pages-follow-their-role), each extended with Databento's own
fields beside the common ones. Configuration and the plugin's own work only:
never a price, which is the reading roles' to read under their entitlement,
and never a key, of which only whether it is set."""

from __future__ import annotations

from dataclasses import dataclass, field

from meridian.roles import data


@dataclass(frozen=True)
class SessionRead:
    """One of Databento's live sessions: its asset class and gateway, its
    state (idle, connecting, streaming, waiting, refused, no key), what it
    streams, the sessions opened and reopened (each gap caught up), the
    messages heard, and the last message and error. Never a key."""

    asset_class: str
    host: str = ""
    state: str = "idle"
    subscribed: int = 0
    opened: int = 0
    reconnects: int = 0
    messages: int = 0
    since: str = ""
    last_message_at: str = ""
    last_error: str = ""


@dataclass(frozen=True)
class ConnectionRead(data.ConnectionRead):
    """Connection's common fields, then Databento's: the historical host
    asked, what the mode means now, each asset class's plan as the settings
    say it, the records the historical API returned and the replays the
    live gateway served, the last call's HTTP status, and each stream's
    session."""

    host: str = ""
    detail: str = ""
    stocks_plan: str = ""
    options_plan: str = ""
    records: int = 0
    replays: int = 0
    last_status: int = 0
    streams: list[SessionRead] = field(default_factory=list)


@dataclass(frozen=True)
class Served(data.Served):
    """One dataset as "Served" shows it, then Databento's: its key, the
    Databento dataset it reads, the source behind it, and what it restated,
    kept unchanged and could not convert since the plugin started."""

    key: str = ""
    feed: str = ""
    source: str = ""
    restated: int = 0
    unchanged: int = 0
    unconverted: int = 0


@dataclass(frozen=True)
class Want(data.Want):
    """One standing want as "Wants" shows it, then Databento's: its ID, the
    listings or underlyings it names, and whether they are streamed."""

    want_id: str = ""
    listings: list[str] = field(default_factory=list)
    streamed: bool = False


@dataclass(frozen=True)
class DatasetsRead(data.DatasetsRead):
    """Datasets' common fields, then Databento's: the listings it knows, the
    days still forming, the listings and option series streamed, the
    underlyings whose chains it keeps, whether a restart's gap is caught up
    (yes: Databento states a trade alike from the history and the live
    gateway, so a caught-up trade is the row the stream recorded), and why
    open interest is not dated, where the calendar did not answer."""

    served: list[Served] = field(default_factory=list)  # type: ignore[assignment]
    wants: list[Want] = field(default_factory=list)  # type: ignore[assignment]
    listings: int = 0
    forming: int = 0
    streamed: int = 0
    series_streamed: int = 0
    chains: int = 0
    backfills_a_restart: bool = True
    calendar_error: str = ""
