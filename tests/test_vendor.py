"""The historical API (vendor.py): the key as Basic authentication and
nowhere else, the form asked, a 429 waited out, an error said, the key
redacted from what is kept, the reach read, and prices exact."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path

import pytest
from meridian.edge import RawStore

from plugin_databento.settings import Credentials
from plugin_databento.vendor import Historical, RateLimiter, Response, VendorError, iso_ns

from conftest import KEY


class Asked:
    def __init__(self, *answers: Response) -> None:
        self.answers = list(answers)
        self.calls: list[tuple[str, str, dict[str, str], bytes | None]] = []

    def __call__(
        self, method: str, url: str, headers: Mapping[str, str], body: bytes | None
    ) -> Response:
        self.calls.append((method, url, dict(headers), body))
        return self.answers.pop(0)


def client(tmp: Path, *answers: Response) -> tuple[Historical, Asked]:
    asked = Asked(*answers)
    held = Historical(
        RawStore(tmp, kind="responses"), Credentials(KEY), transport=asked,
        sleep=lambda _: None,
    )  # fmt: skip
    held.limiter = RateLimiter(per_minute=6_000_000, sleep=lambda _: None)
    return held, asked


LINE = (
    '{"ts_recv":"1791572400000210000","hd":{"ts_event":"1791572400000000000","rtype":0,'
    '"publisher_id":91,"instrument_id":15144},"action":"T","side":"N","depth":0,'
    '"price":"673.800000000","size":100,"flags":0,"ts_in_delta":0,"sequence":0,"symbol":"SPY"}\n'
)


def test_a_range_is_a_form_post_with_basic_authentication(tmp_path: Path) -> None:
    held, asked = client(tmp_path, Response(200, {}, LINE.encode()))
    fetched = held.get_range("EQUS.MINI", "trades", ["SPY"], 1, 2, call="trades", name="SPY")
    method, url, headers, body = asked.calls[0]
    assert method == "POST" and url == "https://hist.databento.com/v0/timeseries.get_range"
    assert headers["Authorization"] == "Basic " + base64.b64encode(f"{KEY}:".encode()).decode()
    assert KEY not in url and body is not None and KEY.encode() not in body
    form = dict(pair.split("=") for pair in body.decode().split("&"))
    assert form["encoding"] == "json" and form["pretty_px"] == "true"
    assert form["map_symbols"] == "true" and form["stype_in"] == "raw_symbol"
    (record,) = fetched.value
    assert record["price"] == "673.800000000" and record["symbol"] == "SPY"
    kept = json.loads(next(tmp_path.rglob("*.json")).read_text())
    assert KEY not in json.dumps(kept)


def test_a_429_is_waited_out(tmp_path: Path) -> None:
    held, asked = client(
        tmp_path, Response(429, {"retry-after": "2"}, b"{}"), Response(200, {}, LINE.encode())
    )
    held.get_range("EQUS.MINI", "trades", ["SPY"], 1, 2, call="trades", name="SPY")
    assert len(asked.calls) == 2 and held.usage.rate_limited == 1


def test_an_error_is_said(tmp_path: Path) -> None:
    held, _ = client(tmp_path, Response(403, {}, b'{"detail":"Not entitled to OPRA.PILLAR"}'))
    with pytest.raises(VendorError) as refused:
        held.get_range("OPRA.PILLAR", "trades", ["X"], 1, 2, call="trades", name="X")
    assert refused.value.not_entitled and "Not entitled" in refused.value.detail


def test_the_key_is_redacted_from_what_is_kept(tmp_path: Path) -> None:
    held, _ = client(tmp_path, Response(401, {}, f'{{"detail":"bad key {KEY}"}}'.encode()))
    with pytest.raises(VendorError):
        held.reach("EQUS.MINI")
    for kept in tmp_path.rglob("*.json"):
        assert KEY not in kept.read_text()


def test_the_reach_read_and_held(tmp_path: Path) -> None:
    said = {"start": "2023-03-28T00:00:00.000000000Z", "end": "2026-10-09T00:00:00.000000000Z",
            "schema": {"trades": {"start": "2023-03-28T00:00:00.000000000Z",
                                  "end": "2026-10-08T23:59:59.123456789Z"}}}  # fmt: skip
    held, asked = client(tmp_path, Response(200, {}, json.dumps(said).encode()))
    reach = held.reach("EQUS.MINI")
    assert reach.end("trades") == iso_ns("2026-10-08T23:59:59.123456789Z")
    assert reach.end("trades") % 1_000_000_000 == 123_456_789
    assert reach.end("ohlcv-1d") == reach.end_ns
    held.reach("EQUS.MINI")
    assert len(asked.calls) == 1 and asked.calls[0][0] == "GET"


def test_prices_are_never_floats(tmp_path: Path) -> None:
    held, _ = client(tmp_path, Response(200, {}, b'{"hd":{"rtype":35},"close":93.123456789}\n'))
    fetched = held.get_range("EQUS.SUMMARY", "ohlcv-1d", ["TQQQ"], 1, 2, call="d", name="d")
    assert fetched.value[0]["close"] == Decimal("93.123456789")
