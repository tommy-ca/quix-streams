"""
Greenfield Test Data Factory

NO MOCKS, NO LEGACY, START SMALL, SOLID, KISS, DRY, CONSISTENT NAMING

Creates realistic test data for end-to-end crypto pipeline testing.
"""

from dataclasses import dataclass
from typing import List, Dict, Any
import time
import random


@dataclass
class CryptoTradeData:
    """Simple, consistent trade data structure."""
    symbol: str
    price: float
    volume: float
    timestamp: int
    exchange: str
    side: str = "buy"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for pipeline processing."""
        return {
            "symbol": self.symbol,
            "price": self.price,
            "volume": self.volume,
            "timestamp": self.timestamp,
            "exchange": self.exchange,
            "side": self.side
        }


@dataclass
class CryptoTickerData:
    """Simple, consistent ticker data structure."""
    symbol: str
    bid: float
    ask: float
    timestamp: int
    exchange: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for pipeline processing."""
        return {
            "symbol": self.symbol,
            "bid": self.bid,
            "ask": self.ask,
            "timestamp": self.timestamp,
            "exchange": self.exchange
        }


class TestDataFactory:
    """
    Factory for creating realistic crypto test data.
    
    Follows SOLID principles:
    - Single Responsibility: Creates test data only
    - Open/Closed: Easy to extend with new data types
    - DRY: Reusable data creation logic
    """
    
    # Common crypto symbols for realistic testing
    SYMBOLS = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "ADA-USDT", "DOT-USDT"]
    EXCHANGES = ["binance", "kraken", "coinbase", "bitfinex"]
    SIDES = ["buy", "sell"]
    
    @staticmethod
    def create_trade_data(
        symbol: str = None,
        exchange: str = None,
        timestamp: int = None
    ) -> CryptoTradeData:
        """Create single realistic trade data."""
        return CryptoTradeData(
            symbol=symbol or random.choice(TestDataFactory.SYMBOLS),
            price=random.uniform(1000.0, 100000.0),  # Realistic crypto prices
            volume=random.uniform(0.001, 10.0),      # Realistic trade volumes
            timestamp=timestamp or int(time.time() * 1000),  # Current timestamp in ms
            exchange=exchange or random.choice(TestDataFactory.EXCHANGES),
            side=random.choice(TestDataFactory.SIDES)
        )
    
    @staticmethod
    def create_ticker_data(
        symbol: str = None,
        exchange: str = None,
        timestamp: int = None
    ) -> CryptoTickerData:
        """Create single realistic ticker data."""
        base_price = random.uniform(1000.0, 100000.0)
        spread = base_price * 0.001  # 0.1% spread
        
        return CryptoTickerData(
            symbol=symbol or random.choice(TestDataFactory.SYMBOLS),
            bid=base_price - spread/2,
            ask=base_price + spread/2,
            timestamp=timestamp or int(time.time() * 1000),
            exchange=exchange or random.choice(TestDataFactory.EXCHANGES)
        )
    
    @staticmethod
    def create_trade_batch(
        size: int,
        symbol: str = None,
        exchange: str = None
    ) -> List[CryptoTradeData]:
        """Create batch of realistic trade data."""
        if size <= 0:
            raise ValueError("Batch size must be positive")
            
        batch = []
        base_timestamp = int(time.time() * 1000)
        
        for i in range(size):
            # Increment timestamps to simulate realistic sequence
            timestamp = base_timestamp + (i * 1000)  # 1 second intervals
            batch.append(TestDataFactory.create_trade_data(
                symbol=symbol,
                exchange=exchange,
                timestamp=timestamp
            ))
        
        return batch
    
    @staticmethod
    def create_ticker_batch(
        size: int,
        symbol: str = None,
        exchange: str = None
    ) -> List[CryptoTickerData]:
        """Create batch of realistic ticker data."""
        if size <= 0:
            raise ValueError("Batch size must be positive")
            
        batch = []
        base_timestamp = int(time.time() * 1000)
        
        for i in range(size):
            timestamp = base_timestamp + (i * 5000)  # 5 second intervals for tickers
            batch.append(TestDataFactory.create_ticker_data(
                symbol=symbol,
                exchange=exchange,
                timestamp=timestamp
            ))
        
        return batch
    
    @staticmethod
    def create_mixed_data_batch(size: int) -> List[Dict[str, Any]]:
        """Create mixed batch of trades and tickers as dictionaries."""
        if size <= 0:
            raise ValueError("Batch size must be positive")
            
        batch = []
        for i in range(size):
            # Mix trades and tickers (70% trades, 30% tickers)
            if random.random() < 0.7:
                data = TestDataFactory.create_trade_data()
                data_dict = data.to_dict()
                data_dict["data_type"] = "trade"
            else:
                data = TestDataFactory.create_ticker_data()
                data_dict = data.to_dict()
                data_dict["data_type"] = "ticker"
                
            batch.append(data_dict)
        
        return batch


class TestDataValidator:
    """Validates test data integrity (SOLID - Single Responsibility)."""
    
    @staticmethod
    def validate_trade_data(data: CryptoTradeData) -> bool:
        """Validate trade data structure and values."""
        if not data.symbol or not data.exchange:
            return False
        if data.price <= 0 or data.volume <= 0:
            return False
        if data.timestamp <= 0:
            return False
        if data.side not in ["buy", "sell"]:
            return False
        return True
    
    @staticmethod
    def validate_ticker_data(data: CryptoTickerData) -> bool:
        """Validate ticker data structure and values."""
        if not data.symbol or not data.exchange:
            return False
        if data.bid <= 0 or data.ask <= 0:
            return False
        if data.ask <= data.bid:  # Ask should be higher than bid
            return False
        if data.timestamp <= 0:
            return False
        return True
    
    @staticmethod
    def validate_batch_consistency(batch: List[Dict[str, Any]]) -> bool:
        """Validate that batch data is consistent."""
        if not batch:
            return True
            
        # Check timestamps are in order
        timestamps = [item.get("timestamp", 0) for item in batch]
        return timestamps == sorted(timestamps)