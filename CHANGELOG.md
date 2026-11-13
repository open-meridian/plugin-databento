# Changelog

## 0.3.1

On open-meridian 0.36.3, contract 30, on core aae501d's harness (chart
0.1.538); `meridian` 0.1.60 in the check. The row-key rule (spec/the-lake rule
5, ruled 2026-11-12), before core enforces it from contract 30's stage H.

- **A statistic valid from the start of its day**: open interest and volume
  are valid from the start of the market day they describe, in New York
  (open-meridian 0.36.2's `helpers.chains.statistic`), never the time they
  were read or published; the plugin no longer passes a valid time of its own,
  so a re-read restates the value alone.
- **A series' terms under their key's first valid time**: `record_series`
  builds each series through the SDK's `option_chains.SeriesTimes` (0.36.3),
  held in the plugin's storage (`series.json`, beside its raw records) and
  never cleared when its options plan changes, so a restart sends no series
  under a new valid time. Terms changed since are an adjustment, a new key
  valid from that day (`<OSI>:<date>:ADJ`). A series the lake already holds
  under another first valid time is taken as recorded, said once; one asked
  under another underlying than its first is skipped and counted. A reader
  waiting on a series it already holds is answered under the held time
  (before, unchanged terms were never sent again).
- **Tests**: a statistic read again, and every series after a restart a day
  later, keep their valid time.
- **open-meridian 0.36.3** (`meridian plugin migrate --to 0.36.2`, then `--to
  0.36.3`): pins and the base by digest; its by-hand findings
  (`valid-time-from-the-clock`, `series-without-a-holder`) done as above.

## 0.3.0

On open-meridian 0.36.0, contract 30, on core aae501d's harness (chart
0.1.538); `meridian` 0.1.60 in the check. What the plugin records is
unchanged.

- **Databento's terms and pricing in words** (ruling 1032): each dataset's
  `licence_default` is gone, and `terms_and_pricing` names its source and
  what that source's licence asks (Nasdaq NLS Plus's summary; the equity
  venues' own feeds through US Equities Mini, no venue fee; OPRA, per
  device or by a firm's own agreement), then Databento's plans, the venue
  fees it passes through and distribution as a separate entitlement, and
  that the plugin is not affiliated. Shown on Data sources and in the
  Datasets page's row details (was "kept; one person's"). The licence a
  deployment admin sets is the one enforced; the plugin states none, so
  "trades kept two days" is the admin's to set now.
- **How a gap is recovered** (`gap_recovery`): `daily`, `trades` and
  `options` by `backfill` (Databento's historical API serves each again);
  `live` `stream_only`. No `sessions_calendar_id`: the platform's calendar
  master holds no US equity or OPRA trading-sessions calendar yet.
- **Row keys checked against the lake's rule 5** (ruled 2026-11-12): a
  close and daily bar keyed by symbol and business date, a live price,
  quote and trade by its own times, each under its record (`@INS-...`), so
  none is restated under another valid time or subject. **Two cases
  change valid time:** an option series' open interest or volume is keyed
  by its series, its day and its kind (the SDK's `helpers.statistic`) but
  valid from OPRA's publication time, so a value republished later the same
  day restates the key under another valid time; and a series' terms
  (`helpers.option_series`) are keyed by its OCC symbol but valid from when
  the plugin read them, so terms that change (a deliverable adjusted)
  restate the key under another valid time. Neither a statistic nor a
  series has a cancellation; listed for step 3d, unchanged.
- e2e: core lists each dataset's terms in words and how a gap is recovered
  (was: the default licences).
- `meridian plugin migrate --to 0.36.0` moved the pins (the base by digest,
  `meridian` 0.1.60); its `licence-default-left` findings done by hand.
- Core's runtime and harness images at `aae501d`, by digest.

## 0.2.0

open-meridian 0.35.2 (each role one class; rulings 1003 to 1005 and 1009 to
1029), contract v29, on core v29's harness.

- **The pins:** open-meridian 0.33.4 to 0.35.2, plugin-python:0.35.2 by tag
  and digest, meridian 0.1.57 in the check, and the e2e's runtime and
  harness at core 1ae36f0 (contract v29) by digest.
- **One class for its role:** `Market`, the data role's class (lake.py's
  listings feed and chains), registered with `meridian.run`: the settings
  applied as delivered, and in its `run` the two streams, the timetable,
  the licences and the latency reports, restarted after a backoff should
  one stop. The chains' acts (liveness, the calendar, each record) are
  made through the class, never the connection. The raw answers are tended
  from the settings as delivered (`RawStore.tend(settings=...)`).
- **The streams on the SDK's supervisor** (`helpers.sessions.Supervisor`,
  ruling 1027): its own reconnect loop is gone; the live gateway's client
  stays the plugin's (gateway.py, ruling 1038). Each wait between sessions
  is now jittered, a stocks reconnect's gap is caught up from when each
  listing was last heard (the history and the gateway's replay), and each
  stream's signs of life and reconnects are on Connection's Sessions. A
  line that converts to nothing (a heartbeat) is still handed on, the
  feed's sign of life.
- **The edge** is the SDK's listings feed's (`listings.at_the_edge`); the
  plugin's temporary copy is gone, a chain's standing wants told to it.
  The Lake's own `named` (the SDK's, unchanged) is gone.
- **Each row once** through the SDK's `SentRows`, in place of its own copy.
- **The suite at contract v29:** a derived series and a pool's factor
  named not presented, with why.
- **The e2e's reader** is three role classes, `custody`, `reporting` and
  `signal`, on `meridian.run_async`.

## 0.1.2

SDK 0.33.4: raw-record writes and enum conversion fixes (ruling 937).

- **The pins:** open-meridian 0.33.3 to 0.33.4, and plugin-python:0.33.4 by tag
  and digest (`meridian plugin migrate --to 0.33.4`: only the pins move).

## 0.1.1

A bug fix (ruling 926), until the SDK's own calendar helper replaces it
(924).

- **A failed calendar read is tried again:** where the deployment's US
  equity calendar did not answer (or was not found), the read is made again
  after 30 seconds, the wait doubling to at most 15 minutes, until it
  answers. Before, one failed read left that span's open interest and
  volume undated until the next day.

## 0.1.0

The first version: Databento's US equity and OPRA option market data from
its public historical API and live gateway, on open-meridian 0.33.3
(contract v28) and meridian 0.1.53 (rulings 489, 865, 866, 868 and 902;
382, 387, 551, 660 and 908).

- **Four datasets, one a kind:** `daily`, `live`, `trades` and `options`.
  Databento sells real time and history and no delayed feed, so none
  declares a delay. Each names the source behind it as its vendor -- Nasdaq
  NLS Plus (consolidated), the US equity venues' own feeds, or OPRA -- and
  Databento as its aggregator; each licence default Databento's
  pass-through terms, conservatively.
- **Each asset class's plan** (`stocks_timeliness`, `options_timeliness`):
  real time or historical only, as the account holder says; a want the plan
  does not reach declined not covered.
- **Stocks:** daily bars from Databento US Equities Summary, unadjusted;
  live prices and quotes from a short replay of Databento US Equities Mini's
  one-minute BBO; trades and one-second quotes streamed from the live
  gateway; a range of trades from the history and, past its reach, the
  gateway's intraday replay; a trade keyed by its two clocks and its place,
  alike from either, so a reconnect's or restart's gap is caught up; the
  side Databento states as the aggressor; no venue (Mini anonymizes them).
- **Option chains:** each series' terms by OCC OSI symbol under its
  underlying, never resolved, its style and settlement from its
  definition's CFI code; open interest and volume dated by the deployment's
  calendar, a past date's read from the history; consolidated quotes on a
  real-time plan; trades streamed for a standing want's series.
- **The key:** a secret setting, sent to Databento as the historical API's
  Basic authentication, never to the live gateway, which takes a SHA-256
  answer to its challenge.
- **Sample data:** Databento's documented record shapes with every value
  invented, its historical API and live gateway both. No key needed.
- **Its Python package and command are `plugin_databento`** (rulings
  915 to 917), so they never shadow Databento's own client library; the
  plugin, its datasets and its source strings keep the name `databento`.
- The data role's two pages, Connection and Datasets, each a read tool; the
  data suite, 22 cases presented; the e2e on the plugin harness of core
  8def82a.
