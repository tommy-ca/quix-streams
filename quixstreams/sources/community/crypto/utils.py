from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, Iterator, Optional


def TopicBuilder(provider: str, *, exchange: Optional[str] = None, datatype: str = "trades", interval: Optional[str] = None, suffix: Optional[str] = None) -> str:
    parts = ["crypto", "source", provider]
    if exchange:
        parts.append(exchange.lower())
    name = ".".join(parts + [datatype])
    if interval:
        name = f"{name}_{interval}"
    if suffix:
        name = f"{name}.{suffix}"
    return name


def default_key_fn(event: Dict[str, Any]) -> str:
    exchange = event.get("exchange") or "unknown"
    symbol = event.get("symbol") or "unknown"
    return f"{exchange}:{symbol}"


def default_ts_fn(event: Dict[str, Any]) -> Optional[int]:
    return event.get("ts_event")


# Normalizers are intentionally minimal; sources can enrich upstream

def normalize_trade(event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "exchange": event.get("exchange"),
        "symbol": event.get("symbol"),
        "trade_id": event.get("trade_id") or event.get("id"),
        "side": event.get("side"),
        "price": event.get("price"),
        "qty": event.get("qty") or event.get("amount") or event.get("size"),
        "ts_event": event.get("ts_event") or event.get("timestamp") or event.get("ts"),
    }


def normalize_ticker(event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "exchange": event.get("exchange"),
        "symbol": event.get("symbol"),
        "bid": event.get("bid"),
        "ask": event.get("ask"),
        "last": event.get("last"),
        "ts_event": event.get("ts_event") or event.get("timestamp") or event.get("ts"),
    }


def normalize_klines(item: Dict[str, Any], interval: str) -> Dict[str, Any]:
    # Accepts either CCXT tuple-like or dict with OHLCV
    if isinstance(item, (list, tuple)) and len(item) >= 6:
        open_time, open_, high, low, close, volume = item[:6]
        close_time = item[6] if len(item) > 6 else None
        symbol = item[7] if len(item) > 7 else None
        exchange = item[8] if len(item) > 8 else None
    else:
        open_time = item.get("open_time")
        close_time = item.get("close_time")
        open_ = item.get("open")
        high = item.get("high")
        low = item.get("low")
        close = item.get("close")
        volume = item.get("volume")
        symbol = item.get("symbol")
        exchange = item.get("exchange")

    return {
        "exchange": exchange,
        "symbol": symbol,
        "interval": interval,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "open_time": open_time,
        "close_time": close_time,
    }


def normalize_orderbook(snapshot_or_delta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "exchange": snapshot_or_delta.get("exchange"),
        "symbol": snapshot_or_delta.get("symbol"),
        "bids": snapshot_or_delta.get("bids"),
        "asks": snapshot_or_delta.get("asks"),
        "ts_event": snapshot_or_delta.get("ts_event") or snapshot_or_delta.get("timestamp"),
        "type": snapshot_or_delta.get("type", "snapshot"),
    }


def exponential_backoff(base: float = 0.5, factor: float = 2.0, max_sleep: float = 30.0) -> Iterator[float]:
    sleep = base
    while True:
        yield min(sleep, max_sleep)
        sleep *= factor