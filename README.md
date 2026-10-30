# plugin-databento

Databento's US equity and OPRA option market data for
[Open Meridian](https://open-meridian.com), as a data plugin: Databento's
historical API and its live gateway, read with the account holder's own
Databento API key, into your deployment's lake. Databento carries each
venue's data as the venue publishes it; each dataset names the source
behind it -- Nasdaq's NLS Plus summary, the US equity venues' own feeds, or
OPRA -- and Databento as its aggregator.

| Dataset | What it holds |
|---|---|
| `daily` | Each listing's close and daily bar from Databento US Equities Summary, consolidated, unadjusted |
| `live` | The last sale and the best bid and offer, and the quote with its sizes, from Databento US Equities Mini; streamed for a standing want |
| `trades` | Each trade of Databento US Equities Mini, streamed for a standing want, a range read from the history, a reconnect's or restart's gap caught up |
| `options` | An underlying's OPRA option chain: each series' terms by OCC symbol, its open interest and volume; in real time, its quotes and streamed trades |

**One dataset per kind.** Databento serves history and real time and no
delayed feed; you say in Settings what your plan reaches of stocks and of
options -- real time, or historical only -- and a read your plan does not
reach is declined.

**A Databento API key is needed**, from your Databento portal. It is a
secret setting: sent to Databento alone, as the historical API's
authentication, and never to the live gateway, which is answered a
challenge instead; never logged or shown. Databento's and its venues' terms
are your deployment's to settle: see [docs/terms.md](docs/terms.md).

The documentation the plugin ships with -- overview, setup, pages,
troubleshooting and terms -- is in [docs/](docs/README.md); `AGENTS.md` is
for whoever works on it next.

## Develop

    make ci-remote     # lint and the tests, in a container
    make check         # meridian plugin check --verified --run-tests
    make e2e           # the plugin on the plugin harness, on sample data
    make preview       # the pages on sample data, in preview/

Every gate runs in a container; plugins land CI-first, through a pull
request.

Apache-2.0. Databento is a trademark of its owner; this project is not
affiliated with or endorsed by Databento, Inc.
