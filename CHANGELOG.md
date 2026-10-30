# Changelog

## 0.1.0 (2026-10-30)

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
