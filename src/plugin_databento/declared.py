"""What this version declares beside its role, `data` (W8.1, W4.1): its
catalogue and its storage, built from code and used twice -- `__main__`
registers with them, and `meridian plugin upload` reads them from the built
image (declaration.py puts them together with the settings, the pages and
the documentation).

**The catalogue: four datasets**, each kind one dataset (ruling 382: one
dataset per timeliness, and Databento sells real time and history, never a
delayed feed, so none declares a delay):

- `daily`: closes and daily bars, from Databento US Equities Summary
  (`EQUS.SUMMARY`, `ohlcv-1d`), Nasdaq NLS Plus's end-of-day summary,
  consolidated across the US exchanges, unadjusted; New York's day, ending
  at the 16:00 close; pulled for a read and pushed once a session's close is
  final. From the historical API, on either plan.
- `live`: the last sale and the best bid and offer as prices, and the quote
  with its sizes, from Databento US Equities Mini (`EQUS.MINI`), Databento's
  aggregate of the venues' own feeds (not the SIPs' NBBO); pulled for a read
  (a short replay of `bbo-1m`), a standing want's quotes streamed (`bbo-1s`).
  Real-time Stocks plan only.
- `trades`: each trade of `EQUS.MINI`, streamed for the listings under a
  standing want (real time), and pulled for a range from the history (either
  plan), the gap after a reconnect or a restart caught up.
- `options`: an underlying's option chain from OPRA (`OPRA.PILLAR`) -- each
  series' terms (`meridian.v1.ListedSeries`), its open interest and volume
  (`meridian.v1.Statistic`), and on a real-time Options plan its trades,
  streamed for a standing want, and its consolidated best bid and offer as
  a quote -- apart from the equity datasets, so an admin licenses OPRA's data
  apart from the equity venues'.

**Who originated it, who carries it** (the dictionary's
`DatasetDeclaration.vendor` and `.aggregator`; rulings 387, 865 and 908's
pattern): each dataset names the source behind it as its vendor -- Nasdaq's
NLS Plus for the daily summary, the US equity venues' own feeds for Mini,
OPRA for the chains -- and Databento as its aggregator. When the SDK carries
the terms' `underlying_source` (ruling 896), each also names it there; its
`price_source_class` (906) is TRADED for daily and trades and FIRM_QUOTE for
the quotes, once the amendment carries it.

**The terms Databento's licensing imposes** (`licence_default`), which the
deployment's admin confirms or replaces on Data sources -- never whether a
deployment meets them. Databento passes each venue's licence through:
Databento US Equities Mini carries no venue licence fee, for personal and
commercial use alike; OPRA's live data is licensed per device for a
person's own use, and for a firm by OPRA's own agreement. The defaults:
shown to people (`display`), derived data for internal use only
(`internal_only`), never redistributed (`not_allowed`); OPRA's one person's
(`personal_use`), the self-served licence; the daily and chain datasets
kept, the live one served and not kept, the trades kept two days (a reader
that lost some catches up from the lake). Databento's name is not
affiliated (`not_affiliated_notice`). Who may take it (`eligibility`) is
not declared yet: core's declaration parser does not carry it, so a
registration declaring it is refused as differing from its upload. A
summary, not legal advice; docs/terms.md says the same.

**Storage**: one kind of raw record, Databento's answers and stream lines as
received (never the key), a month by default.
"""

from __future__ import annotations

from meridian import DatasetDeclaration, DatasetLicence, RecordKind

from .chains import CADENCE_SECONDS as CHAIN_CADENCE_SECONDS
from .chains import DATA_TYPES as CHAIN_DATA_TYPES
from .chains import OPTIONS
from .convert import DAY_END_MINUTE
from .lake import DAILY, LIVE, LIVE_CADENCE_SECONDS, TRADES, US_EQUITY_CALENDAR
from .vendor import EQUS_MINI, EQUS_SUMMARY, OPRA

#: Who originated each dataset's data, and who carries it (approved 915-917,
#: after ruling 908's pattern for Massive).
SUMMARY_SOURCE = "Nasdaq NLS Plus (consolidated)"
EQUITY_SOURCE = "US equity venues' own feeds (exchanges, ATSs)"
OPTIONS_SOURCE = "OPRA"
AGGREGATOR = "Databento"
#: Each dataset's source and the Databento dataset it reads, by its key.
SOURCES = {
    DAILY: (SUMMARY_SOURCE, EQUS_SUMMARY),
    LIVE: (EQUITY_SOURCE, EQUS_MINI),
    TRADES: (EQUITY_SOURCE, EQUS_MINI),
    OPTIONS: (OPTIONS_SOURCE, OPRA),
}
#: How long the lake keeps a trade by default: two days (the plan's Q40).
TRADES_KEPT_DAYS = 2
#: How far back Databento's history reaches is the dataset's, read at run.
HISTORY_DAYS = 0
DAY_TIME_ZONE = "America/New_York"

#: Its one kind of raw record, Databento's answers as received.
KIND = "responses"
LABEL = "Raw responses"
WINDOW_DAYS = 30
RESPONSES_KIND = RecordKind(KIND, LABEL, window_days=WINDOW_DAYS)

NOT_AFFILIATED = (
    "Databento is a trademark of its owner. This plugin is not affiliated with or endorsed by "
    "Databento, Inc."
)


def licence(kept: bool, retention_days: int = 0, personal: bool = False) -> DatasetLicence:
    """Databento's pass-through terms, as each dataset's default."""
    return DatasetLicence(
        kept=kept,
        retention_days=retention_days,
        derived_use_terms="internal_only",
        display=True,
        personal_use=personal,
        redistribution="not_allowed",
        not_affiliated_notice=NOT_AFFILIATED,
    )


def _declared(
    key: str,
    data_types: list[str],
    modes: list[str],
    terms: DatasetLicence,
    *,
    cadence: int = 0,
    calendar: bool = False,
) -> DatasetDeclaration:
    return DatasetDeclaration(
        key=key,
        vendor=SOURCES[key][0],
        aggregator=AGGREGATOR,
        data_types=data_types,
        modes=modes,
        cadence=cadence,
        history=HISTORY_DAYS,
        licence_default=terms,
        day_time_zone=DAY_TIME_ZONE,
        day_end_minute=DAY_END_MINUTE,
        calendar_id=US_EQUITY_CALENDAR if calendar else "",
    )


CATALOGUE = (
    _declared(
        DAILY,
        ["meridian.v1.Price", "meridian.v1.Bar"],
        ["pull", "push"],
        licence(kept=True),
        cadence=86_400,
        calendar=True,
    ),
    _declared(
        LIVE,
        ["meridian.v1.Price", "meridian.v1.Quote"],
        ["pull", "stream"],
        licence(kept=False),
        cadence=LIVE_CADENCE_SECONDS,
    ),
    _declared(TRADES, ["meridian.v1.Trade"], ["pull", "stream"], licence(True, TRADES_KEPT_DAYS)),
    _declared(
        OPTIONS,
        list(CHAIN_DATA_TYPES[OPTIONS]),
        ["pull", "stream"],
        licence(kept=True, personal=True),
        cadence=CHAIN_CADENCE_SECONDS,
        calendar=True,
    ),
)
