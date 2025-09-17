# Crypto Source: Cryptofeed (Draft Spec)

Status: Ready
Owner: quix-streams community

1. Problem
- Ingest realtime crypto market data over websockets from multiple exchanges and channels using cryptofeed; publish into Kafka.

2. Goals
- Support core channels initially: trades, ticker (optionally book diffs later)
- Support at least Binance initially; allow multi-exchange config
- Provide normalized minimal schemas; configurable topic naming
- Robust reconnection/backoff and graceful shutdown

3. Non-goals (v1)
- Full order book L3 assembly; deep snapshot consistency across restarts
- Cross-exchange field parity beyond minimal normalized fields

4. Installation
- pip install quixstreams[crypto]

5. Configuration (proposed)
- exchanges: list[str] = ["BINANCE"]
- channels: list[str] = ["trades","ticker"]
- symbols: list[str] | None (e.g., ["BTC-USDT"]) – exchange-specific notation
- normalize: bool = True
- reconnect: bool = True
- max_retries: int | None = None
- backoff: base/factor/max_sleep configurable
- topic_builder: callable to form topic names; default crypto.source.cryptofeed.{exchange}.{datatype}
- key_setter/timestamp_setter overrides

6. Topics and schema
- Default topics per channel per exchange
- Trade schema: { exchange, symbol, trade_id?, side?, price, qty, ts_event }
- Ticker schema: { exchange, symbol, bid?, ask?, last?, ts_event }

7. Behavior
- setup(): lazy import cryptofeed; create FeedHandler; register callbacks per exchange/channel/symbol
- run(): start FeedHandler; bridge callbacks to producer; handle reconnect loop with backoff if enabled
- stop(): stop handler; flush

8. Error handling
- Network/WS disconnects: reconnect with exponential backoff up to max_retries (or forever if None)
- Callback exceptions logged with context; processing continues

9. Testing strategy
- Unit tests with mocks for FeedHandler and callback invocation; reconnection/backoff tests with controlled failures
- Integration test optional and gated

10. Example
```python
from quixstreams import Application
from quixstreams.sources.community.crypto import CryptofeedSource

app = Application(broker_address="localhost:19092")
source = CryptofeedSource(exchanges=["BINANCE"], channels=["trades","ticker"], symbols=["BTC-USDT"]) 

sdf = app.dataframe(source=source)
sdf.print(metadata=True)
app.run()
```

11. Installation and extras
- pip install quixstreams[crypto]

12. Configuration summary
- exchanges (list[str], default: ["BINANCE"]) 
- channels (list[str], default: ["trades","ticker"]) 
- symbols (list[str] | None)
- normalize (bool, default: true)
- reconnect (bool, default: true)
- max_retries (int | None, default: None)
- backoff (implicit exponential, capped)
- key_setter, timestamp_setter (callables)
