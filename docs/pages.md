# Pages

Both under **Manage**, for an admin of the plugin; each is also a read tool
for agents on your deployment's MCP, answering what the page shows. Neither
shows a price, a trade or a quote, nor your key.

## Connection

How the plugin reaches Databento, one group at a time:

- **Key**: the mode (Live, Sample data or No key), whether the API key is
  set, what the plans reach as you set them, and each plan.
- **Limits and use**: the pace kept, the historical calls made and those
  rate-limited and waited out, the records returned and the live replays
  made, the last call and error.
- **Stream**: each live session, stocks and options -- its state, what it
  streams, since when, and its last error.
- **Sessions**: each session's latency, once measured.

## Datasets

What the plugin serves your lake:

- **Served**: each of the four datasets as declared, whether your plans
  reach it, whether a deployment admin has licensed it, what it recorded and
  declined, and the source behind it (Nasdaq's NLS Plus summary, the equity
  venues' own feeds, or OPRA).
- **Wants**: the standing wants it keeps current, the latest few (the tool
  answers every one), streamed or kept current.
- **Subjects**: whether your deployment lets it read an instrument's record.
