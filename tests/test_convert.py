"""Databento's records converted (convert.py): the null sentinels, an
option's CFI code, the numbering of trades stated with the same clocks, and
the shapes of Databento's JSON encoding."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

from plugin_databento import convert
from plugin_databento.convert import Contract, Numbering

HD = {"ts_event": "1791572400000000000", "rtype": 0, "publisher_id": 22, "instrument_id": 7}


def test_null_sentinels_are_no_value() -> None:
    assert convert.exact(convert.UNDEF_PRICE_TEXT) is None
    assert convert.exact(None) is None
    assert convert.whole("18446744073709551615") is None
    assert convert.whole(str(2**63 - 1)) is None
    assert convert.exact("673.880000000") == Decimal("673.880000000")
    assert convert.whole("1791572400000000000") == 1791572400000000000


def definition(**fields: object) -> tuple[str, dict[str, object]]:
    said: dict[str, object] = {
        "ts_recv": "1", "hd": HD, "raw_symbol": "SPY   261016C00670000",
        "security_update_action": "A", "instrument_class": "C", "cfi": "OCAXPS",
        "unit_of_measure_qty": "100.000000000", "underlying": "SPY",
        "strike_price": "670.000000000", "expiration": "1792108800000000000",
    }  # fmt: skip
    said.update(fields)
    return str(said["raw_symbol"]), said


def test_a_series_terms_from_its_definition() -> None:
    (made,) = list(convert.contracts([definition()], "raw/d"))
    assert isinstance(made, Contract)
    assert made.expiration == dt.date(2026, 10, 16) and made.strike == Decimal(670)
    assert made.exercise_style == "EXERCISE_STYLE_AMERICAN"
    assert made.settlement_method == "SETTLEMENT_METHOD_PHYSICAL"
    assert made.standard and made.multiplier == Decimal(100)


def test_a_european_cash_settled_series() -> None:
    (made,) = list(
        convert.contracts(
            [definition(raw_symbol="SPXW  261016P05800000", cfi="OPEICS", underlying="SPX")],
            "raw/d",
        )
    )
    assert isinstance(made, Contract)
    assert made.exercise_style == "EXERCISE_STYLE_EUROPEAN"
    assert made.settlement_method == "SETTLEMENT_METHOD_CASH" and not made.standard


def test_no_cfi_no_style_guessed() -> None:
    (made,) = list(convert.contracts([definition(cfi="")], "raw/d"))
    assert not isinstance(made, Contract) and made.scheme == "databento:definition.cfi"


def test_a_deleted_definition_is_left_out_and_the_latest_kept() -> None:
    old = definition(ts_recv="1", unit_of_measure_qty="10.000000000")
    new = definition(ts_recv="2")
    (made,) = list(convert.contracts([old, new], "raw/d"))
    assert isinstance(made, Contract) and made.multiplier == Decimal(100)
    assert list(convert.contracts([definition(security_update_action="D")], "raw/d")) == []


def test_trades_with_the_same_clocks_are_numbered_apart() -> None:
    numbering = Numbering()
    said = {"ts_recv": "10", "hd": HD, "price": "1.000000000", "size": 1, "side": "N"}
    first = convert.trade("SPY", said, "k#0", numbering)
    second = convert.trade("SPY", said, "k#1", numbering)
    third = convert.trade("SPY", said | {"ts_recv": "11"}, "k#2", numbering)
    assert first and second and third
    assert (first.place, second.place, third.place) == (0, 1, 0)
    assert first.row_key != second.row_key
    assert first.row_key == f"SPY:t:10:{HD['ts_event']}"


def test_a_bbo_states_the_last_sale_and_the_sides() -> None:
    said = {"ts_recv": "20", "hd": HD, "price": "5.000000000", "size": 1,
            "levels": [{"bid_px": "4.900000000", "ask_px": convert.UNDEF_PRICE_TEXT,
                        "bid_sz": 3, "ask_sz": 0}]}  # fmt: skip
    prices = {q.kind: (q.amount, q.at_ns) for q in convert.bbo_live("SPY", said, "k")}
    assert prices == {
        "last": (Decimal("5.000000000"), int(str(HD["ts_event"]))),
        "bid": (Decimal("4.900000000"), 20),
    }
    quote = convert.quote("SPY", said, "k", "bbo")
    assert quote is not None and quote.ask is None and quote.bid_size == Decimal(3)


def test_the_flat_csv_names_are_read_too() -> None:
    said = {"ts_recv": "20", "hd": HD, "bid_px_00": "1.000000000", "ask_px_00": "1.100000000",
            "bid_sz_00": 5, "ask_sz_00": 6}  # fmt: skip
    quote = convert.quote("SPY", said, "k", "bbo")
    assert quote is not None and quote.ask == Decimal("1.100000000")


def test_an_options_trade_names_its_venue_by_mic() -> None:
    said = {"ts_recv": "10", "hd": HD, "price": "8.150000000", "size": 3, "side": "N"}
    made = convert.trade("x", said, "k", Numbering(), osi="SPY   261016C00670000")
    assert made is not None and made.venue_code == "XCBO"


def test_statistics_open_interest_and_volume() -> None:
    def stat(kind: int, quantity: object, action: int = 1) -> tuple[str, dict[str, object]]:
        return "SPY   261016C00670000", {
            "ts_recv": "5",
            "hd": HD,
            "stat_type": kind,
            "quantity": quantity,
            "update_action": action,
        }

    made = list(
        convert.statistics(
            [stat(9, 1543), stat(6, 868), stat(1, 4), stat(9, 7, action=2), stat(9, "x")], "k"
        )
    )
    held = [m for m in made if isinstance(m, convert.ChainStatistic)]
    assert [(m.kind, m.value) for m in held] == [("open_interest", 1543), ("volume", 868)]
    assert len(made) == 3


def test_the_one_spelling_and_the_option_root() -> None:
    assert convert.spelled("brk.b") == "BRK.B" and convert.option_root("BRK.B") == "BRKB"
    assert convert.osi_of("SPY   261016C00670000") == "SPY   261016C00670000"
    assert convert.osi_of("SPY261016C00670000") is None
