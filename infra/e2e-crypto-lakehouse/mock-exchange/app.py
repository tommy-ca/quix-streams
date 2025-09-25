"""
Mock crypto exchange server for E2E testing.

Provides WebSocket and REST API endpoints that simulate crypto exchange behavior
for testing crypto sources without requiring real exchange API keys or data.
"""

import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
import numpy as np

# Configuration
WS_PORT = int(os.getenv("WS_PORT", "8888"))
REST_PORT = int(os.getenv("REST_PORT", "8889"))
SYMBOLS = os.getenv("SYMBOLS", "BTC-USD,ETH-USD").split(",")
CHANNELS = os.getenv("CHANNELS", "trades,ticker").split(",")

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Mock Crypto Exchange", version="1.0.0")


class MockDataGenerator:
    """Generates realistic mock crypto data."""
    
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.prices = {symbol: self._initial_price(symbol) for symbol in symbols}
        self.volumes = {symbol: 0.0 for symbol in symbols}
        self.last_trades = {symbol: [] for symbol in symbols}
        self.order_books = {symbol: self._init_orderbook(symbol) for symbol in symbols}
    
    def _initial_price(self, symbol: str) -> float:
        """Get initial price based on symbol."""
        if "BTC" in symbol:
            return 45000.0 + random.uniform(-5000, 5000)
        elif "ETH" in symbol:
            return 2800.0 + random.uniform(-300, 300)
        elif "DOT" in symbol:
            return 7.5 + random.uniform(-2, 2)
        else:
            return 100.0 + random.uniform(-50, 50)
    
    def _init_orderbook(self, symbol: str) -> Dict:
        """Initialize order book for symbol."""
        price = self.prices[symbol]
        tick_size = price * 0.0001  # 0.01% tick size
        
        bids = []
        asks = []
        
        # Generate 10 levels each side
        for i in range(10):
            bid_price = price - (i + 1) * tick_size
            ask_price = price + (i + 1) * tick_size
            bid_qty = random.uniform(0.1, 10.0)
            ask_qty = random.uniform(0.1, 10.0)
            
            bids.append([bid_price, bid_qty])
            asks.append([ask_price, ask_qty])
        
        return {"bids": bids, "asks": asks}
    
    def generate_trade(self, symbol: str) -> Dict:
        """Generate a realistic trade."""
        current_price = self.prices[symbol]
        
        # Add some price movement (random walk)
        price_change = random.uniform(-0.005, 0.005)  # ±0.5%
        new_price = max(current_price * (1 + price_change), 0.01)
        self.prices[symbol] = new_price
        
        # Generate trade details
        qty = random.uniform(0.001, 5.0)
        side = random.choice(["buy", "sell"])
        trade_id = int(time.time() * 1000000) + random.randint(1000, 9999)
        
        trade = {
            "exchange": "mock",
            "symbol": symbol,
            "trade_id": trade_id,
            "price": round(new_price, 8),
            "qty": round(qty, 8),
            "side": side,
            "ts_event": int(time.time() * 1000),
            "timestamp": int(time.time() * 1000)
        }
        
        # Update volume
        self.volumes[symbol] += qty
        
        # Keep last trades
        self.last_trades[symbol].append(trade)
        if len(self.last_trades[symbol]) > 100:
            self.last_trades[symbol] = self.last_trades[symbol][-100:]
        
        return trade
    
    def generate_ticker(self, symbol: str) -> Dict:
        """Generate ticker data."""
        current_price = self.prices[symbol]
        
        # Calculate 24h stats (simplified)
        open_price = current_price * random.uniform(0.95, 1.05)
        high_price = max(current_price, open_price) * random.uniform(1.0, 1.02)
        low_price = min(current_price, open_price) * random.uniform(0.98, 1.0)
        volume_24h = self.volumes[symbol] + random.uniform(1000, 10000)
        
        return {
            "exchange": "mock",
            "symbol": symbol,
            "bid": round(current_price * 0.9999, 8),
            "ask": round(current_price * 1.0001, 8),
            "last": round(current_price, 8),
            "open": round(open_price, 8),
            "high": round(high_price, 8),
            "low": round(low_price, 8),
            "volume": round(volume_24h, 8),
            "ts_event": int(time.time() * 1000),
            "timestamp": int(time.time() * 1000)
        }
    
    def generate_orderbook(self, symbol: str) -> Dict:
        """Generate order book snapshot."""
        book = self.order_books[symbol]
        current_price = self.prices[symbol]
        
        # Update order book around current price
        tick_size = current_price * 0.0001
        
        # Regenerate levels
        bids = []
        asks = []
        
        for i in range(10):
            bid_price = current_price - (i + 1) * tick_size
            ask_price = current_price + (i + 1) * tick_size
            bid_qty = random.uniform(0.1, 10.0)
            ask_qty = random.uniform(0.1, 10.0)
            
            bids.append([round(bid_price, 8), round(bid_qty, 8)])
            asks.append([round(ask_price, 8), round(ask_qty, 8)])
        
        self.order_books[symbol] = {"bids": bids, "asks": asks}
        
        return {
            "exchange": "mock",
            "symbol": symbol,
            "bids": bids,
            "asks": asks,
            "ts_event": int(time.time() * 1000),
            "timestamp": int(time.time() * 1000),
            "type": "snapshot"
        }
    
    def generate_klines(self, symbol: str, interval: str = "1m", limit: int = 100) -> List[List]:
        """Generate OHLCV kline data."""
        current_price = self.prices[symbol]
        klines = []
        
        # Calculate interval in seconds
        interval_seconds = {
            "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
            "1h": 3600, "2h": 7200, "4h": 14400, "6h": 21600,
            "8h": 28800, "12h": 43200, "1d": 86400
        }.get(interval, 60)
        
        now = int(time.time())
        
        for i in range(limit):
            open_time = (now - (limit - i) * interval_seconds) * 1000
            close_time = open_time + interval_seconds * 1000 - 1
            
            # Generate OHLCV with some randomness
            base_price = current_price * (0.95 + 0.1 * random.random())
            open_price = base_price * (0.99 + 0.02 * random.random())
            close_price = base_price * (0.99 + 0.02 * random.random())
            high_price = max(open_price, close_price) * (1.0 + 0.01 * random.random())
            low_price = min(open_price, close_price) * (1.0 - 0.01 * random.random())
            volume = random.uniform(10, 1000)
            
            klines.append([
                int(open_time),
                round(open_price, 8),
                round(high_price, 8),
                round(low_price, 8),
                round(close_price, 8),
                round(volume, 8),
                int(close_time),
                symbol,
                "mock"
            ])
        
        return klines


# Global data generator
data_generator = MockDataGenerator(SYMBOLS)

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[WebSocket, Dict] = {}
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[websocket] = {"subscriptions": set()}
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            del self.active_connections[websocket]
        logger.info(f"Client disconnected. Total connections: {len(self.active_connections)}")
    
    async def send_personal_message(self, message: dict, websocket: WebSocket):
        try:
            await websocket.send_text(json.dumps(message))
        except Exception as e:
            logger.error(f"Error sending message: {e}")
    
    async def broadcast_to_subscribers(self, message: dict, channel: str, symbol: str):
        """Broadcast message to subscribers of specific channel/symbol."""
        subscription_key = f"{channel}:{symbol}"
        disconnected = []
        
        for websocket, data in self.active_connections.items():
            if subscription_key in data["subscriptions"]:
                try:
                    await websocket.send_text(json.dumps(message))
                except Exception as e:
                    logger.error(f"Error broadcasting to client: {e}")
                    disconnected.append(websocket)
        
        # Clean up disconnected clients
        for ws in disconnected:
            self.disconnect(ws)


manager = ConnectionManager()

# REST API Endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "timestamp": int(time.time())}

@app.get("/symbols")
async def get_symbols():
    """Get available symbols."""
    return {"symbols": SYMBOLS}

@app.get("/ticker/{symbol}")
async def get_ticker(symbol: str):
    """Get ticker for symbol."""
    if symbol not in SYMBOLS:
        return {"error": f"Symbol {symbol} not found"}
    
    ticker = data_generator.generate_ticker(symbol)
    return ticker

@app.get("/orderbook/{symbol}")
async def get_orderbook(symbol: str):
    """Get order book for symbol."""
    if symbol not in SYMBOLS:
        return {"error": f"Symbol {symbol} not found"}
    
    orderbook = data_generator.generate_orderbook(symbol)
    return orderbook

@app.get("/trades/{symbol}")
async def get_recent_trades(symbol: str, limit: int = 50):
    """Get recent trades for symbol."""
    if symbol not in SYMBOLS:
        return {"error": f"Symbol {symbol} not found"}
    
    trades = data_generator.last_trades.get(symbol, [])[-limit:]
    return {"trades": trades}

@app.get("/klines/{symbol}")
async def get_klines(symbol: str, interval: str = "1m", limit: int = 100):
    """Get kline/candlestick data for symbol."""
    if symbol not in SYMBOLS:
        return {"error": f"Symbol {symbol} not found"}
    
    klines = data_generator.generate_klines(symbol, interval, limit)
    return {"klines": klines}

# WebSocket endpoints
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Main WebSocket endpoint for real-time data."""
    await manager.connect(websocket)
    
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            
            # Handle subscription requests
            if message.get("method") == "subscribe":
                params = message.get("params", [])
                for param in params:
                    # Parse subscription (e.g., "trades:BTC-USD")
                    if ":" in param:
                        channel, symbol = param.split(":", 1)
                        if channel in CHANNELS and symbol in SYMBOLS:
                            manager.active_connections[websocket]["subscriptions"].add(param)
                            await manager.send_personal_message({
                                "id": message.get("id"),
                                "result": f"Subscribed to {param}",
                                "method": "subscribe"
                            }, websocket)
                        else:
                            await manager.send_personal_message({
                                "id": message.get("id"),
                                "error": f"Invalid subscription: {param}",
                                "method": "subscribe"
                            }, websocket)
            
            elif message.get("method") == "unsubscribe":
                params = message.get("params", [])
                for param in params:
                    if param in manager.active_connections[websocket]["subscriptions"]:
                        manager.active_connections[websocket]["subscriptions"].remove(param)
                        await manager.send_personal_message({
                            "id": message.get("id"),
                            "result": f"Unsubscribed from {param}",
                            "method": "unsubscribe"
                        }, websocket)
    
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

# Background task to generate and broadcast data
async def data_broadcaster():
    """Background task that generates and broadcasts data."""
    logger.info("Starting data broadcaster...")
    
    while True:
        try:
            for symbol in SYMBOLS:
                # Generate trades periodically (every 1-5 seconds)
                if random.random() < 0.3:  # 30% chance each second
                    trade = data_generator.generate_trade(symbol)
                    await manager.broadcast_to_subscribers(trade, "trades", symbol)
                
                # Generate ticker updates (every 5-10 seconds)
                if random.random() < 0.1:  # 10% chance each second  
                    ticker = data_generator.generate_ticker(symbol)
                    await manager.broadcast_to_subscribers(ticker, "ticker", symbol)
                
                # Generate order book updates (every 2-3 seconds)
                if random.random() < 0.2:  # 20% chance each second
                    orderbook = data_generator.generate_orderbook(symbol)
                    await manager.broadcast_to_subscribers(orderbook, "book", symbol)
            
            await asyncio.sleep(1)  # Check every second
        
        except Exception as e:
            logger.error(f"Error in data broadcaster: {e}")
            await asyncio.sleep(5)  # Wait before retrying

@app.on_event("startup")
async def startup_event():
    """Start background tasks."""
    asyncio.create_task(data_broadcaster())
    logger.info(f"Mock crypto exchange started on ports {WS_PORT} (WS) and {REST_PORT} (REST)")
    logger.info(f"Available symbols: {SYMBOLS}")
    logger.info(f"Available channels: {CHANNELS}")

if __name__ == "__main__":
    # Run the FastAPI app with uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=REST_PORT,
        log_level="info",
        access_log=True
    )