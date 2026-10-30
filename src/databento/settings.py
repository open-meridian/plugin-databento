"""What the plugin needs from the deployment, declared as settings (W4.7, W6.11).

In the data role's order (spec/plugin-pages-follow-their-role): no
`environment`, since Databento has one; the credential; the vendor's own;
sample data last.

- **API key** (`databento_api_key`), secret and required: a key of the
  account holder's own Databento team, from its portal (API keys). Entered by
  a person in the dashboard's Settings form, sent only to Databento -- as the
  user name of a historical call's HTTP Basic authentication -- and never to
  the live gateway at all, which takes a SHA-256 answer to its challenge
  (CRAM) instead; never in an address, a log line, a page or a raw record.
  `Credentials` keeps it out of its repr; the Connection page says whether it
  is set, never what it is. Its name follows the data plugins' (the vendor's,
  then what it is: `massive_api_key`, `tiingo_api_token`). Name proposed.
- **Stocks plan** (`stocks_timeliness`) and **Options plan**
  (`options_timeliness`), each a choice: what the key reaches of that asset
  class -- `real_time` (the live gateway: a Standard, Plus or Unlimited plan
  with the dataset's live licence in place) or `end_of_day` (the historical
  API alone: the usage-based plan, or no live licence for that dataset).
  Databento sells no 15-minute delayed feed, so `delayed`, the third of the
  values ruling 908 approved for Massive, is not offered. Databento licenses
  each dataset's live data apart (US equities and OPRA by separate venue
  licences), and its answers do not say whether a key's live licence is in
  place, so the account holder says it here. `end_of_day` by default: no
  live session is opened unless a person said so.
- **Sample data** (`sample_data`, `meridian.sample_data`), a developer's
  setting: answers committed with the plugin in Databento's documented
  shapes, every value invented (synthetic.py), in place of Databento, with
  no key, for the e2e and a first look.

The SDK declares the storage's two settings from the declaration's kind:
`responses_window_days` (30 days by default) and `responses_past_window`.
"""

from __future__ import annotations

import base64
import hashlib
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field

import meridian
from meridian import Choice

TOKEN = "databento_api_key"
STOCKS_TIMELINESS = "stocks_timeliness"
OPTIONS_TIMELINESS = "options_timeliness"
SAMPLE_DATA = "sample_data"
REAL_TIME, END_OF_DAY = "real_time", "end_of_day"
TIMELINESS = (REAL_TIME, END_OF_DAY)
#: What each timeliness is called wherever a page says it.
SAID = {
    REAL_TIME: "Real time",
    END_OF_DAY: "Historical only",
}
CHOICES = tuple(Choice(value, SAID[value]) for value in TIMELINESS)
#: How many of a key's last characters name its bucket, which the live
#: gateway's challenge answer carries after the hash (Databento's CRAM).
BUCKET_ID_LENGTH = 5


@meridian.settings
class Declared:
    """The settings as declared, in the data role's order: one typed class,
    the SDK's form, which `connect` and the declaration register."""

    databento_api_key: str = meridian.setting(
        "API key",
        required=True,
        secret=True,
        description=(
            "From your Databento portal (API keys). Sent only to Databento, as the historical "
            "API's Basic authentication; the live gateway is answered a challenge instead. "
            "Each dataset's licence is between your team, Databento and its venues."
        ),
    )
    stocks_timeliness: str = meridian.setting(
        "Stocks plan",
        default=END_OF_DAY,
        choices=CHOICES,
        description=(
            "What your Databento plan reaches of US equities: Real time (a Standard, Plus or "
            "Unlimited plan with live data for Databento US Equities Mini), or Historical only "
            "(daily bars and past trades from the historical API, billed by Databento by "
            "volume). Live prices, quotes and streamed trades need Real time."
        ),
    )
    options_timeliness: str = meridian.setting(
        "Options plan",
        default=END_OF_DAY,
        choices=CHOICES,
        description=(
            "What your Databento plan reaches of OPRA: Real time (a live plan with OPRA's live "
            "licence in place), or Historical only (option series, open interest and volume "
            "from the historical API). Option quotes and streamed option trades need Real time."
        ),
    )
    sample_data: bool = meridian.sample_data(
        description=(
            "Answer from records committed with the plugin in Databento's documented shapes, "
            "every value invented, instead of calling Databento. No key needed. Development "
            "deployments only."
        )
    )


DECLARED = Declared
#: Each setting it declares, in its order.
SETTINGS: tuple[meridian.Setting, ...] = Declared.declared()  # type: ignore[attr-defined]


@dataclass(frozen=True)
class Credentials:
    """What Databento is called with. Never in a repr, a log line, a page or
    a raw record."""

    key: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("a Databento API key is needed")

    def headers(self) -> dict[str, str]:
        """The historical API's Basic authentication: the key as the user
        name, no password. For the request alone."""
        token = base64.b64encode(f"{self.key}:".encode()).decode("ascii")
        return {"Authorization": f"Basic {token}"}

    def answer(self, challenge: str) -> str:
        """The live gateway's challenge answered (CRAM): the SHA-256 of the
        challenge and the key joined by `|`, in hex, then `-` and the key's
        bucket, its last five characters. The key itself never leaves."""
        digest = hashlib.sha256(f"{challenge}|{self.key}".encode()).hexdigest()
        return f"{digest}-{self.key[-BUCKET_ID_LENGTH:]}"

    def secrets(self) -> tuple[str, ...]:
        """The values never to be kept anywhere, for raw records to look for."""
        return (self.key,)


def _timeliness(value: object) -> str:
    return str(value) if value in TIMELINESS else END_OF_DAY


@dataclass(frozen=True)
class Config:
    """The settings as the plugin uses them: the key, if given, whether it is
    set (never its value), each asset class's timeliness, and sample data."""

    credentials: Credentials | None = None
    token_set: bool = False
    stocks: str = END_OF_DAY
    options: str = END_OF_DAY
    sample_data: bool = False
    #: The settings the deployment holds nothing for, as delivered.
    missing: tuple[str, ...] = ()

    @property
    def ready(self) -> bool:
        """Whether there is something to ask: sample data, or the key."""
        return self.sample_data or self.credentials is not None

    def timeliness(self, asset_class: str) -> str:
        """What the plan reaches of `stocks` or `options`: sample data serves
        every dataset in real time."""
        if self.sample_data:
            return REAL_TIME
        return self.options if asset_class == "options" else self.stocks


def config_from(values: Mapping[str, object], unset: Collection[str] = ()) -> Config:
    """The settings as delivered, typed. A timeliness the deployment holds
    nothing for is `end_of_day`, the default."""
    key = values.get(TOKEN)
    text = key.strip() if isinstance(key, str) else ""
    return Config(
        credentials=Credentials(text) if text else None,
        token_set=bool(text),
        stocks=END_OF_DAY
        if STOCKS_TIMELINESS in unset
        else _timeliness(values.get(STOCKS_TIMELINESS)),
        options=(
            END_OF_DAY
            if OPTIONS_TIMELINESS in unset
            else _timeliness(values.get(OPTIONS_TIMELINESS))
        ),
        sample_data=values.get(SAMPLE_DATA) is True,
        missing=tuple(sorted(unset)),
    )
