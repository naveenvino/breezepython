"""
Options Historical Data Model
SQLAlchemy model for storing options historical data
"""
from sqlalchemy import Column, String, DateTime, DECIMAL, BigInteger, Integer, Index
from sqlalchemy.sql import func
from datetime import datetime
import uuid

from ..base import Base


class OptionsHistoricalData(Base):
    """
    Model for options historical data
    Matches the SQL Server table structure
    """
    __tablename__ = 'OptionsHistoricalData'
    
    # Primary key (GUID)
    id = Column('Id', String(50), primary_key=True, default=lambda: str(uuid.uuid4()))
    
    # Option identification
    trading_symbol = Column('TradingSymbol', String(50), nullable=False)
    timestamp = Column('Timestamp', DateTime, nullable=False)
    exchange = Column('Exchange', String(10), nullable=False, default='NFO')
    underlying = Column('Underlying', String(10), nullable=False, default='NIFTY')
    strike = Column('Strike', Integer, nullable=False)
    option_type = Column('OptionType', String(2), nullable=False)  # CE or PE
    expiry_date = Column('ExpiryDate', DateTime, nullable=False)
    
    # OHLC data
    open = Column('Open', DECIMAL(18, 2), nullable=False)
    high = Column('High', DECIMAL(18, 2), nullable=False)
    low = Column('Low', DECIMAL(18, 2), nullable=False)
    close = Column('Close', DECIMAL(18, 2), nullable=False)
    last_price = Column('LastPrice', DECIMAL(18, 2), nullable=False)
    
    # Volume data
    volume = Column('Volume', BigInteger, nullable=False)
    open_interest = Column('OpenInterest', BigInteger, nullable=False)
    
    # Greeks
    delta = Column('Delta', DECIMAL(8, 4), nullable=True)
    gamma = Column('Gamma', DECIMAL(8, 4), nullable=True)
    theta = Column('Theta', DECIMAL(8, 4), nullable=True)
    vega = Column('Vega', DECIMAL(8, 4), nullable=True)
    implied_volatility = Column('ImpliedVolatility', DECIMAL(8, 4), nullable=True)
    
    # Bid/Ask
    bid_price = Column('BidPrice', DECIMAL(18, 2), nullable=True)
    ask_price = Column('AskPrice', DECIMAL(18, 2), nullable=True)
    bid_ask_spread = Column('BidAskSpread', DECIMAL(18, 2), nullable=True)
    
    # Metadata
    created_at = Column('CreatedAt', DateTime, nullable=False, server_default=func.now())
    data_source = Column('DataSource', String(50), nullable=False, default='BreezeConnect')
    interval = Column('Interval', String(20), nullable=False, default='1minute')
    
    # Indexes
    __table_args__ = (
        Index('IX_OptionsData_Symbol_Timestamp', 'TradingSymbol', 'Timestamp'),
        Index('IX_OptionsData_Expiry_Strike', 'ExpiryDate', 'Strike', 'OptionType'),
        Index('IX_OptionsData_Underlying_Timestamp', 'Underlying', 'Timestamp'),
    )
    
    def __repr__(self):
        return f"<OptionsData({self.trading_symbol} @ {self.timestamp}: {self.last_price})>"
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'id': self.id,
            'trading_symbol': self.trading_symbol,
            'timestamp': self.timestamp.isoformat() if self.timestamp else None,
            'strike': self.strike,
            'option_type': self.option_type,
            'expiry_date': self.expiry_date.isoformat() if self.expiry_date else None,
            'open': float(self.open),
            'high': float(self.high),
            'low': float(self.low),
            'close': float(self.close),
            'last_price': float(self.last_price),
            'volume': self.volume,
            'open_interest': self.open_interest,
            'delta': float(self.delta) if self.delta else None,
            'gamma': float(self.gamma) if self.gamma else None,
            'theta': float(self.theta) if self.theta else None,
            'vega': float(self.vega) if self.vega else None,
            'implied_volatility': float(self.implied_volatility) if self.implied_volatility else None,
            'bid_price': float(self.bid_price) if self.bid_price else None,
            'ask_price': float(self.ask_price) if self.ask_price else None,
            'bid_ask_spread': float(self.bid_ask_spread) if self.bid_ask_spread else None
        }
    
    @classmethod
    def from_breeze_data(cls, breeze_data: dict):
        """Create instance from Breeze API response"""
        import pytz
        from ....utils.market_hours import is_within_market_hours
        IST = pytz.timezone('Asia/Kolkata')
        
        # Convert UTC to IST for storage
        utc_timestamp = datetime.fromisoformat(breeze_data['datetime'].replace('Z', '+00:00'))
        timestamp = utc_timestamp.astimezone(IST).replace(tzinfo=None)
        
        # Filter out data outside market hours (9:15 AM - 3:30 PM IST)
        if not is_within_market_hours(timestamp, include_pre_market=False):
            return None  # This record will be skipped
        
        utc_expiry = datetime.fromisoformat(breeze_data['expiry_date'].replace('Z', '+00:00'))
        expiry = utc_expiry.astimezone(IST).replace(tzinfo=None)
        
        # Calculate bid-ask spread if both are available
        bid_ask_spread = None
        if breeze_data.get('best_bid_price') and breeze_data.get('best_offer_price'):
            bid_ask_spread = float(breeze_data['best_offer_price']) - float(breeze_data['best_bid_price'])
        
        return cls(
            trading_symbol=breeze_data.get('trading_symbol', breeze_data['stock_code']),
            timestamp=timestamp,
            exchange=breeze_data.get('exchange_code', 'NFO'),
            underlying=breeze_data.get('underlying', 'NIFTY'),
            strike=int(breeze_data.get('strike_price', 0)),
            option_type=breeze_data.get('right', 'CE'),  # CE or PE
            expiry_date=expiry,
            open=float(breeze_data['open']),
            high=float(breeze_data['high']),
            low=float(breeze_data['low']),
            close=float(breeze_data['close']),
            last_price=float(breeze_data['close']),
            volume=int(breeze_data.get('volume', 0)) if breeze_data.get('volume') else 0,
            open_interest=int(breeze_data.get('open_interest', 0)) if breeze_data.get('open_interest') else 0,
            bid_price=float(breeze_data['best_bid_price']) if breeze_data.get('best_bid_price') else None,
            ask_price=float(breeze_data['best_offer_price']) if breeze_data.get('best_offer_price') else None,
            bid_ask_spread=bid_ask_spread,
            data_source="BreezeConnect"
        )