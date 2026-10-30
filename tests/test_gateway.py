"""The live gateway's Raw API (gateway.py): the challenge answered and never
the key, the control lines as Databento documents them, a replay heard to
its completion, the errors that end a session; and the stream (stream.py)
kept open on the synthetic gateway."""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from typing import Any

import pytest
from meridian.edge import RawStore

from plugin_databento import gateway
from plugin_databento.settings import Credentials
from plugin_databento.stream import DatabentoFeed, Streamer
from plugin_databento.synthetic import SyntheticGateway, connect, connect_async

from conftest import KEY

NOW = 1_791_572_400_000_000_000


def test_the_challenge_is_answered_with_a_hash_and_the_keys_bucket() -> None:
    answer = Credentials(KEY).answer("j5pwMHz6vwXruJM4cOwQrQeQE0bImIzT")
    digest = hashlib.sha256(f"j5pwMHz6vwXruJM4cOwQrQeQE0bImIzT|{KEY}".encode()).hexdigest()
    assert answer == f"{digest}-{KEY[-5:]}"
    line = gateway.auth_line(Credentials(KEY), "j5pw", "OPRA.PILLAR").decode()
    assert KEY not in line and line.endswith("\n")
    said = gateway.fields(line)
    assert said["dataset"] == "OPRA.PILLAR" and said["encoding"] == "json"
    assert said["pretty_px"] == "1" and said["pretty_ts"] == "0"


def test_the_gateway_of_a_dataset() -> None:
    assert gateway.host_of("OPRA.PILLAR") == "opra-pillar.lsg.databento.com"
    assert gateway.host_of("EQUS.MINI") == "equs-mini.lsg.databento.com"


def test_a_subscription_names_at_most_500_symbols_a_line() -> None:
    symbols = tuple(f"S{n}" for n in range(1_001))
    lines = gateway.subscription_lines(gateway.Subscription("trades", symbols, start_ns=5), 7)
    said = [gateway.fields(line.decode()) for line in lines]
    assert [len(s["symbols"].split(",")) for s in said] == [500, 500, 1]
    assert [s["is_last"] for s in said] == ["0", "0", "1"]
    assert all(s["start"] == "5" and s["id"] == "7" and s["schema"] == "trades" for s in said)


def test_authentication_refused() -> None:
    with pytest.raises(gateway.Refused, match="bad key"):
        gateway.authenticated("success=0|error=bad key\n")
    assert gateway.authenticated("success=1|session_id=135567\n") == "135567"


@pytest.mark.parametrize(
    ("code", "fatal"), [(1, gateway.Refused), (3, gateway.GatewayError), (4, None), (7, None)]
)
def test_which_errors_end_a_session(code: int, fatal: Any) -> None:
    said = {"hd": {"rtype": 21, "ts_event": "1", "publisher_id": 0, "instrument_id": 0},
            "err": "said", "code": code, "is_last": True}  # fmt: skip
    stopped = gateway.failed(said)
    if fatal is None:
        assert stopped is None
    else:
        assert isinstance(stopped, fatal) and stopped.code == code


def test_a_replay_is_heard_to_its_completion() -> None:
    held = SyntheticGateway(clock_ns=lambda: NOW)
    heard = gateway.replay(
        Credentials(KEY),
        "EQUS.MINI",
        [gateway.Subscription("bbo-1m", ("SPY", "BRK.B"), start_ns=NOW - 60 * 10**9)],
        connect=connect(held),
    )
    assert heard.completed
    assert sorted(symbol for symbol, _ in heard.records) == ["BRK.B", "SPY"]
    # The key never crossed: only the answer to the challenge.
    assert all(KEY not in line for line in held.heard)
    assert any(line.startswith("auth=") for line in held.heard)


def test_a_replay_that_never_completes_times_out() -> None:
    class Silent:
        def __init__(self) -> None:
            self.lines = [b"lsg_version=0\n", b"cram=abc\n", b"success=1|session_id=1\n"]

        def readline(self) -> bytes:
            return self.lines.pop(0) if self.lines else b'{"hd": {"rtype": 23}, "code": 0}\n'

        def write(self, data: bytes) -> None:
            pass

        def close(self) -> None:
            pass

    ticks = iter(range(0, 1000, 10))
    with pytest.raises(TimeoutError):
        gateway.replay(
            Credentials(KEY), "EQUS.MINI",
            [gateway.Subscription("trades", ("SPY",), start_ns=1)],
            connect=lambda host, port, timeout: Silent(), seconds=30,
            clock=lambda: float(next(ticks)),
        )  # fmt: skip


async def test_the_stream_hears_trades_and_quotes(tmp_path: Path) -> None:
    held = SyntheticGateway(clock_ns=lambda: NOW)
    store = RawStore(tmp_path, kind="responses")
    heard: list[Any] = []

    async def hear(stated: list[Any]) -> None:
        heard.extend(stated)

    async def flush() -> None:
        return None

    def feed() -> DatabentoFeed:
        return DatabentoFeed(
            "stocks", "EQUS.MINI", Credentials(KEY), store, connect=connect_async(held),
            clock_ns=lambda: NOW,
        )  # fmt: skip

    async def no_sleep(_: float) -> None:
        return None

    streamer = Streamer(feed, hear, flush, clock_ns=lambda: NOW, sleep=no_sleep, silent_seconds=0.3)
    streamer.want(["SPY"])
    # One session: heard, then silent past the heartbeat's time, so broken.
    await asyncio.wait_for(streamer.run(sessions=1), 5.0)
    assert "nothing heard" in streamer.session.last_error
    trades = [h for h in heard if not hasattr(h, "bid")]
    quotes = [h for h in heard if hasattr(h, "bid")]
    assert len(trades) == 3 and len(quotes) == 1
    # Two prints with the same clocks: numbered apart.
    assert trades[0].place == 0 and trades[1].place == 1
    assert streamer.session.opened == 1 and streamer.session.messages > 0
    subscribed = [line for line in held.heard if line.startswith("schema=")]
    assert {gateway.fields(s)["schema"] for s in subscribed} == {"trades", "bbo-1s"}
    assert all("start" not in gateway.fields(s) for s in subscribed)
    assert all(KEY not in line for line in held.heard)


async def test_letting_a_listing_go_opens_the_session_again(tmp_path: Path) -> None:
    held = SyntheticGateway(clock_ns=lambda: NOW)
    feed = DatabentoFeed(
        "stocks", "EQUS.MINI", Credentials(KEY), RawStore(tmp_path, kind="responses"),
        connect=connect_async(held),
    )  # fmt: skip
    await feed.open()
    await feed.subscribe(["BRK.B", "SPY"])
    await feed.subscribe(["BRK.B", "SPY", "TQQQ"])
    assert held.sessions == 1
    await feed.subscribe(["SPY"])
    assert held.sessions == 2 and feed.subscribed == ["SPY"]
    await feed.close()
