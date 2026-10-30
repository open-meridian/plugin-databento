# Setting it up

Each step by an admin of the plugin, unless it says otherwise. The plugin
must already be launched, its role approved by a deployment admin.

## 1. The API key

In your Databento portal, under API keys, copy a key. Historical data is
billed by Databento by volume on its usage-based plan; live data needs a
Standard, Plus or Unlimited plan and each dataset's live licence, which
Databento arranges with the venue (Databento US Equities Mini carries no
venue fee; OPRA's is per device for a person's own use).

## 2. Enter it, and say your plans

**Manage → Settings.**

1. **API key**: a secret setting. Once saved it is never shown again,
   logged, put on a page or sent anywhere but Databento, and never to the
   live gateway at all, which is answered a challenge instead.
2. **Stocks plan** and **Options plan**: what your Databento plan reaches
   of each -- **Real time** (live data, its licence in place) or
   **Historical only**. Databento's answers do not say whether a live
   licence is in place, so you say it here; each is Historical only until
   you choose, so no live session is opened unless you say so.

**Connection** then says under Key whether the key is set, never its value,
whether Databento answered it, and each plan as you set it. With none, "No
key yet", linking to Settings.

## 3. License and entitle its datasets

A deployment admin, on the deployment's **Data sources** page, licenses the
datasets your plans reach -- `daily`, `live`, `trades`, `options` (option
chains, licensed apart from the stock data) -- each starting from
Databento's pass-through terms, and entitles the plugins that read prices,
trades, quotes and option chains. Nothing is read from Databento until a
plugin entitled to a dataset asks for it.

## 4. Watch it work

**Datasets** shows each dataset with whether your plans reach it and what
was recorded and declined; **Connection** the calls made, the records
returned and each live session.
