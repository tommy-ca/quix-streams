import pytest

from quixstreams.sources.community.crypto.utils import (
    TopicBuilder,
    default_key_fn,
    default_ts_fn,
    normalize_trade,
    normalize_ticker,
    normalize_klines,
    normalize_orderbook,
    exponential_backoff,
)


def test_topic_builder_variants():
    assert TopicBuilder("binance", datatype="trades") == "crypto.source.binance.trades"
    assert (
        TopicBuilder("cryptofeed", exchange="binance", datatype="trades")
        == "crypto.source.cryptofeed.binance.trades"
    )
    assert (
        TopicBuilder("ccxt", datatype="klines", interval="1m")
        == "crypto.source.ccxt.klines_1m"
    )


def test_default_key_ts():
    event = {"exchange": "binance", "symbol": "BTCUSDT", "ts_event": 123}
    assert default_key_fn(event) == "binance:BTCUSDT"
    assert default_ts_fn(event) == 123


def test_normalize_trade_and_ticker():
    raw_trade = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "id": "t1",
        "side": "buy",
        "price": 100.0,
        "amount": 0.1,
        "timestamp": 111,
    }
    t = normalize_trade(raw_trade)
    assert t["trade_id"] == "t1"
    assert t["price"] == 100.0
    assert t["qty"] == 0.1
    assert t["ts_event"] == 111

    raw_ticker = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "bid": 99.0,
        "ask": 101.0,
        "last": 100.0,
        "timestamp": 222,
    }
    k = normalize_ticker(raw_ticker)
    assert k["bid"] == 99.0 and k["ask"] == 101.0 and k["last"] == 100.0
    assert k["ts_event"] == 222


def test_normalize_klines_list_and_dict():
    # CCXT-like tuple
    tup = [1700000000000, 10.0, 12.0, 9.0, 11.0, 100.0]
    out = normalize_klines(tup, "1m")
    assert out["open"] == 10.0 and out["close"] == 11.0
    assert out["interval"] == "1m"
    # dict-like
    d = {
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "open_time": 1700000100000,
        "close_time": 1700000160000,
        "open": 11.0,
        "high": 13.0,
        "low": 10.5,
        "close": 12.0,
        "volume": 120.0,
    }
    out2 = normalize_klines(d, "1m")
    assert out2["open_time"] == 1700000100000 and out2["close_time"] == 1700000160000


def test_normalize_orderbook():
    ob = {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "bids": [[100.0, 1.0]],
        "asks": [[101.0, 1.2]],
        "timestamp": 333,
        "type": "snapshot",
    }
    out = normalize_orderbook(ob)
    assert out["bids"][0][0] == 100.0 and out["asks"][0][0] == 101.0
    assert out["ts_event"] == 333


def test_exponential_backoff_caps():
    gen = exponential_backoff(base=1.0, factor=2.0, max_sleep=3.0)
    vals = [next(gen) for _ in range(5)]
    assert vals == [1.0, 2.0, 3.0, 3.0, 3.0]