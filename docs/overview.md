# Overview

The Databento plugin reads Databento's US equity and US equity option market
data and records it in your deployment's lake, where a plugin that values
your book, shows prices or analyses option chains reads it under the licence
and entitlements you set. Databento carries each venue's data as the venue
publishes it; each dataset names that source as its vendor and Databento as
its aggregator.

It holds one role, `data`: the first-use path, the common pages and the
Summary figures are described once, on [Using a data
plugin](https://open-meridian.dev/how-to/using-a-data-plugin/). These
sections cover what is Databento's own.

## Two services, one dataset per kind

Databento serves history from its historical API and real time from its
live gateway; it sells no delayed feed. So each kind is one dataset, none
declaring a delay. What your plan reaches of each asset class -- **Real
time** or **Historical only** -- you set in Settings; a read your plan does
not reach is declined, not covered.

## Its stock datasets

- **Daily** (`databento:daily`): each listing's close and daily bar for a
  business date -- open, high, low, close and volume -- from Databento US
  Equities Summary, Nasdaq's NLS Plus end-of-day summary, consolidated
  across the US exchanges, as traded, not adjusted for splits. A date is New
  York's trading day; the summary is published once the day is over (its
  last after 20:15 New York). Read when a plugin asks, and each evening the
  closes of every listing already served are recorded unasked. Either plan.
- **Live** (`databento:live`): the last sale and the best bid and offer, and
  the quote with its sizes in shares, from Databento US Equities Mini:
  Databento's own aggregate of the exchanges' and trading systems' own
  feeds, not the SIPs' national best bid and offer. A read takes the latest
  one-minute record from the live gateway's replay of the last minutes;
  while something reads a listing its quote is streamed, once a second.
  Real-time plan only.
- **Trades** (`databento:trades`): each trade of Databento US Equities Mini,
  streamed while something reads the listing (real time), and, on either
  plan, a range read from the history; past the history's reach, on a
  real-time plan, from the live gateway's replay of the last 24 hours. If
  the stream drops or the plugin restarts, the gap is caught up the same
  way, a trade never recorded twice. Where Databento states which side
  initiated a trade, it is recorded as the aggressor. Kept two days by
  default.

Databento US Equities Mini anonymizes its venues, so a stock's price, quote
or trade names none: it is recorded as consolidated. Databento normalizes no
trade condition, so what a trade may count for in a day's high, low, open,
close and volume is not known, and is recorded so.

## Its option chains

- **Options** (`databento:options`): an underlying's option chain from OPRA
  -- each series' terms (right, strike, expiration, style, settlement,
  multiplier and, for a standard contract, its 100 shares), its open
  interest and volume (either plan, from the history); on a real-time plan,
  its trades streamed while something reads them, and its consolidated best
  bid and offer with sizes in contracts, read at most once a minute.

Each series is named by its OCC symbol under its underlying, never added to
your instruments: a series gets its own record only when a plugin asks for
one, for an order, under your deployment's series request rule. A chain is
recorded for the expirations or series a plugin asks for.

A series' style and settlement come from the code its definition states
(ISO 10962); a series whose definition states none is not recorded, and is
counted on **Datasets** as not converted: no style is guessed.

Open interest and volume are OPRA's, published before the open for the
market day before, and dated that day by your deployment's US equity
calendar; if the calendar does not answer, none is recorded rather than
misdated. A past date's figures are read from the history. Greeks and
implied volatility are not read: your deployment computes its own.

## How it records

Every price is exact, as Databento stated it. Every answer and stream line
Databento sent is kept in the plugin's own storage, without your key, so any
row can be traced to what Databento said.

An instrument your deployment knows only from another source, such as a
custodian, is priced under its symbol.
