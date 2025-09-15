# Crypto Source: CCXT (Draft Spec)

Status: Draft
Owner: quix-streams community

1. Problem
- Ingest historical and near-realtime data via CCXT REST (and optionally WS via ccxt.pro) for exchanges; publish to Kafka for lakehouse ingestion.

2. Goals
- Modes: klines (OHLCV), trades, orderbook snapshot polling
- Stateful cursors per symbol to avoid duplication and ensure continuity
- Respect exchange rate limits; handle retries/backoff

3. Non-goals (v1)
- Complex order book diff assembly
- Multi-thread fanout per symbol (can be added later)

4. Installation
- pip install quixstreams[crypto]
- Optional WS: pip install quixstreams[crypto_ws]

5. Configuration (proposed)
- exchange: str = "binance"
- mode: {"klines","trades","orderbook"} = "klines"
- interval (for klines): str = "1m"
- symbols: list[str] (e.g., ["BTC/USDT"]) – CCXT notation
- use_ws: bool = False (requires ccxt.pro)
- rest_poll_interval: float | None (for orderbook)
- rate_limit: bool = True
- normalize: bool = True
- key_setter/timestamp_setter overrides
- state persistence: in-memory (v1); future: external store

6. Topics and schema
- Topics: crypto.source.ccxt.{datatype}[_{interval}] (e.g., crypto.source.ccxt.klines_1m)
- Kline schema: { exchange, symbol, interval, open, high, low, close, volume, open_time, close_time }
- Trade schema: { exchange, symbol, trade_id?, side?, price, qty, ts_event }
- Orderbook schema: { exchange, symbol, bids, asks, ts_event, type="snapshot" }

7. Behavior
- setup(): lazy import ccxt (and ccxt.pro if use_ws); create exchange client; verify capabilities
- run(): per mode loop over symbols; fetch window using cursor (since/last close); emit normalized records; update cursor; sleep per rateLimit
- Backoff: exponential on HTTP/network errors and 429/5xx
- stop(): halt loops and flush

8. Testing strategy
- Fake exchange object in tests; verify cursor advance and idempotency; rate limit sleeps honored
- WS mode optional; can be NotImplementedError initially if deferred

9. Example
```python
from quixstreams import Application
from quixstreams.sources.community.crypto import CCXTSource

app = Application(broker_address="localhost:19092")
source = CCXTSource(exchange="binance", mode="klines", interval="1m", symbols=["BTC/USDT"]) 

sdf = app.dataframe(source=source)
sdf.print(metadata=True)
app.run()
```