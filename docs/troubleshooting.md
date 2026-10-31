# Troubleshooting

## No key yet

Enter the API key on **Settings**. Until then nothing is asked of Databento,
and every read is declined as the source silent.

## Not answering

Databento refused the key or did not answer. **Connection** shows
Databento's words under Last error: a refused key says so; a dataset your
team is not licensed or billed for answers 402 or 403 -- set **Stocks plan**
or **Options plan** to what your plan reaches.

## A read is declined, not covered

Your plan does not reach it (live prices, quotes, streamed trades, or
option quotes and trades on a Historical only plan), or the listing is one
Databento does not carry. **Datasets** says which datasets your plans reach.

## Rate-limited

Databento allows 100 historical requests a second per address; the plugin
spends far less, and a 429 is waited out. **Connection**'s Limits and use
counts both.

## The stream is waiting or refused

A live session that drops, or says nothing for a heartbeat and ten seconds,
is opened again after a second, then up to a minute; the trades in between
are caught up from the history and the gateway's replay. Refused means
Databento refused the key on the gateway: it is tried again after five
minutes. Databento restarts its gateways each Sunday; the session is opened
again after.

## A series is not recorded

Its definition states no ISO 10962 code, so its style and settlement are not
known; **Datasets** counts it as not converted.

## Open interest is not recorded

The deployment's US equity calendar did not answer, so the market day it is
for is not known. **Datasets** says why. Series and quotes are recorded
meanwhile. The plugin asks the calendar again after 30 seconds, then less
often, at least every 15 minutes, until it answers.
