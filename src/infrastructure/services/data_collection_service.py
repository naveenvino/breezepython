"""
Data Collection Service
Service for fetching and storing historical NIFTY and options data
"""
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, func

from ..database.models import NiftyIndexData, OptionsHistoricalData
from ..database.database_manager import get_db_manager
from .breeze_service import BreezeService
from .hourly_aggregation_service import HourlyAggregationService


logger = logging.getLogger(__name__)


class DataCollectionService:
    """
    Service for collecting and managing historical market data
    """
    
    def __init__(self, breeze_service: BreezeService, db_manager=None):
        self.breeze_service = breeze_service
        self.db_manager = db_manager or get_db_manager()
        self.hourly_aggregation_service = HourlyAggregationService(self.db_manager)
    
    async def ensure_nifty_data_available(
        self, 
        from_date: datetime, 
        to_date: datetime,
        symbol: str = "NIFTY",
        fetch_missing: bool = True
    ) -> int:
        """
        Ensure NIFTY index data is available for the given date range
        Fetches missing data if necessary
        
        Args:
            from_date: Start date
            to_date: End date
            symbol: Symbol to fetch (default: NIFTY)
            fetch_missing: If False, don't fetch from API (for backtesting)
        
        Returns: Number of records added
        """
        logger.info(f"Ensuring NIFTY data available from {from_date} to {to_date}")
        
        # Find missing date ranges
        missing_ranges = await self._find_missing_nifty_ranges(from_date, to_date, symbol)
        
        if not missing_ranges:
            logger.info("All NIFTY data already available")
            return 0
        
        if not fetch_missing:
            # Don't fetch from Breeze API during backtesting - use only existing data
            logger.warning(f"NIFTY data missing for {len(missing_ranges)} ranges, but skipping API fetch (backtesting mode)")
            return 0
        
        total_added = 0
        
        # Fetch and store missing data
        for start, end in missing_ranges:
            logger.info(f"Fetching NIFTY data from {start} to {end}")
            
            try:
                # Fetch from Breeze API
                data = await self.breeze_service.get_historical_data(
                    interval="5minute",  # Changed to 5-minute for hourly aggregation
                    from_date=start,
                    to_date=end,
                    stock_code=symbol,
                    exchange_code="NSE",
                    product_type="cash"
                )
                
                if data and 'Success' in data:
                    records = data['Success']
                    
                    # Log sample timestamp format
                    if records and len(records) > 0:
                        sample_dt = records[0].get('datetime', '')
                        logger.info(f"Breeze API timestamp format for NIFTY: '{sample_dt}'")
                    
                    added = await self._store_nifty_data(records, symbol)
                    total_added += added
                    logger.info(f"Added {added} NIFTY records")
                else:
                    logger.warning(f"No data returned for period {start} to {end}")
                    
            except Exception as e:
                logger.error(f"Error fetching NIFTY data: {e}")
                # Continue with next range
        
        # After fetching all 5-minute data, create hourly candles
        if total_added > 0:
            logger.info("Creating hourly candles from 5-minute data")
            hourly_created = await self.create_hourly_data_from_5min(from_date, to_date, symbol)
            logger.info(f"Created {hourly_created} hourly candles")
        
        return total_added
    
    async def ensure_options_data_available(
        self,
        from_date: datetime,
        to_date: datetime,
        strikes: List[int],
        expiry_dates: List[datetime],
        fetch_missing: bool = True
    ) -> int:
        """
        Ensure options data is available for given strikes and expiries
        
        Args:
            from_date: Start date
            to_date: End date
            strikes: List of strike prices
            expiry_dates: List of expiry dates
            fetch_missing: If False, don't fetch from API (for backtesting)
        
        Returns: Number of records added
        """
        logger.info(f"Ensuring options data for {len(strikes)} strikes and {len(expiry_dates)} expiries")
        
        total_added = 0
        
        for expiry in expiry_dates:
            for strike in strikes:
                for option_type in ['CE', 'PE']:
                    # Check if data exists
                    exists = await self._check_option_data_exists(
                        strike, option_type, expiry, from_date, to_date
                    )
                    
                    if not exists:
                        if not fetch_missing:
                            # Skip fetching from Breeze API - use only existing historical data
                            logger.warning(f"Options data not found for {strike} {option_type} expiry {expiry.date()} (backtesting mode)")
                            continue
                        else:
                            # Fetch and store
                            added = await self._fetch_and_store_option_data(
                                strike, option_type, expiry, from_date, to_date
                            )
                            total_added += added
        
        return total_added
    
    async def _find_missing_nifty_ranges(
        self, 
        from_date: datetime, 
        to_date: datetime,
        symbol: str
    ) -> List[Tuple[datetime, datetime]]:
        """Find date ranges where NIFTY data is missing"""
        missing_ranges = []
        
        with self.db_manager.get_session() as session:
            # Get existing data timestamps
            existing = session.query(NiftyIndexData.timestamp).filter(
                and_(
                    NiftyIndexData.symbol == symbol,
                    NiftyIndexData.timestamp >= from_date,
                    NiftyIndexData.timestamp <= to_date,
                    NiftyIndexData.interval == "5minute"
                )
            ).order_by(NiftyIndexData.timestamp).all()
            
            existing_timestamps = {row[0] for row in existing}
            
            # Generate expected hourly timestamps (market hours only) in IST
            current = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
            expected_timestamps = []
            
            while current <= to_date:
                # Skip weekends
                if current.weekday() < 5:  # Monday=0, Friday=4
                    # Market hours: 9:15 AM to 3:30 PM IST
                    market_open = current.replace(hour=9, minute=15)
                    market_close = current.replace(hour=15, minute=30)
                    
                    # Generate hourly timestamps
                    hour_time = market_open
                    while hour_time <= market_close:
                        if hour_time not in existing_timestamps:
                            expected_timestamps.append(hour_time)
                        
                        # Next hour
                        hour_time += timedelta(hours=1)
                        if hour_time.hour == 15 and hour_time.minute > 30:
                            break
                
                current += timedelta(days=1)
            
            # Group missing timestamps into ranges
            if expected_timestamps:
                expected_timestamps.sort()
                start = expected_timestamps[0]
                prev = start
                
                for ts in expected_timestamps[1:]:
                    if ts - prev > timedelta(hours=24):  # Gap found
                        missing_ranges.append((start, prev))
                        start = ts
                    prev = ts
                
                # Add last range
                missing_ranges.append((start, prev))
        
        return missing_ranges
    
    async def _store_nifty_data(self, records: List[Dict], symbol: str) -> int:
        """Store NIFTY data records in database"""
        added = 0
        
        with self.db_manager.get_session() as session:
            for record in records:
                try:
                    # Create NiftyIndexData object using from_breeze_data which handles timezone correctly
                    nifty_data = NiftyIndexData.from_breeze_data(record, symbol)
                    
                    # Skip if None (outside market hours)
                    if nifty_data is None:
                        continue
                    
                    # Check if already exists
                    exists = session.query(NiftyIndexData).filter(
                        and_(
                            NiftyIndexData.symbol == symbol,
                            NiftyIndexData.timestamp == nifty_data.timestamp,
                            NiftyIndexData.interval == nifty_data.interval
                        )
                    ).first()
                    
                    if not exists:
                        session.add(nifty_data)
                        added += 1
                        
                except Exception as e:
                    logger.error(f"Error storing NIFTY record: {e}")
                    continue
            
            session.commit()
        
        return added
    
    async def _check_option_data_exists(
        self,
        strike: int,
        option_type: str,
        expiry: datetime,
        from_date: datetime,
        to_date: datetime
    ) -> bool:
        """Check if option data exists for given parameters"""
        with self.db_manager.get_session() as session:
            # Check both date and datetime to handle timezone differences
            # Use date range comparison for SQL Server compatibility
            expiry_start = expiry.replace(hour=0, minute=0, second=0, microsecond=0)
            expiry_end = expiry.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            count = session.query(func.count(OptionsHistoricalData.id)).filter(
                and_(
                    OptionsHistoricalData.strike == strike,
                    OptionsHistoricalData.option_type == option_type,
                    OptionsHistoricalData.expiry_date.between(expiry_start, expiry_end),
                    OptionsHistoricalData.timestamp >= from_date,
                    OptionsHistoricalData.timestamp <= to_date
                )
            ).scalar()
            
            # Expect at least some data points
            return count > 0
    
    async def _fetch_and_store_option_data(
        self,
        strike: int,
        option_type: str,
        expiry: datetime,
        from_date: datetime,
        to_date: datetime
    ) -> int:
        """Fetch and store option data"""
        try:
            # Generate stock code for option
            expiry_str = expiry.strftime("%y%b").upper()  # e.g., "24JAN"
            stock_code = f"NIFTY{expiry_str}{strike}{option_type}"
            
            logger.info(f"Fetching option data for {stock_code}")
            
            # Fetch from Breeze
            data = await self.breeze_service.get_historical_data(
                interval="1hour",
                from_date=from_date,
                to_date=to_date,
                stock_code=stock_code,
                exchange_code="NFO",
                product_type="options",
                expiry_date=expiry.strftime("%Y-%m-%dT07:00:00.000Z"),
                right=option_type,
                strike_price=str(strike)
            )
            
            if data and 'Success' in data:
                records = data['Success']
                return await self._store_option_data(records)
            else:
                logger.warning(f"No option data returned for {stock_code}")
                return 0
                
        except Exception as e:
            logger.error(f"Error fetching option data: {e}")
            return 0
    
    async def _store_option_data(self, records: List[Dict]) -> int:
        """Store option data records in database"""
        added = 0
        
        with self.db_manager.get_session() as session:
            for record in records:
                try:
                    option_data = OptionsHistoricalData.from_breeze_data(record)
                    
                    # Skip if None (outside market hours)
                    if option_data is None:
                        continue
                    
                    # Check if exists
                    exists = session.query(OptionsHistoricalData).filter(
                        and_(
                            OptionsHistoricalData.trading_symbol == option_data.trading_symbol,
                            OptionsHistoricalData.timestamp == option_data.timestamp
                        )
                    ).first()
                    
                    if not exists:
                        session.add(option_data)
                        added += 1
                        
                except Exception as e:
                    logger.error(f"Error storing option record: {e}")
                    continue
            
            session.commit()
        
        return added
    
    async def get_nifty_data(
        self,
        from_date: datetime,
        to_date: datetime,
        symbol: str = "NIFTY"
    ) -> List[NiftyIndexData]:
        """Get NIFTY data from database - HOURLY DATA ONLY for backtesting"""
        with self.db_manager.get_session() as session:
            return session.query(NiftyIndexData).filter(
                and_(
                    NiftyIndexData.symbol == symbol,
                    NiftyIndexData.timestamp >= from_date,
                    NiftyIndexData.timestamp <= to_date,
                    NiftyIndexData.interval == 'hourly'  # Only get hourly data for signals
                )
            ).order_by(NiftyIndexData.timestamp).all()
    
    async def get_option_data(
        self,
        timestamp: datetime,
        strike: int,
        option_type: str,
        expiry: datetime
    ) -> Optional[OptionsHistoricalData]:
        """Get option data at specific timestamp"""
        with self.db_manager.get_session() as session:
            # Get closest data point within 1 hour
            # Handle expiry time mismatch - DB has 05:30:00 but we might look for 15:30:00
            # First try exact match
            result = session.query(OptionsHistoricalData).filter(
                and_(
                    OptionsHistoricalData.strike == strike,
                    OptionsHistoricalData.option_type == option_type,
                    OptionsHistoricalData.expiry_date == expiry,
                    OptionsHistoricalData.timestamp >= timestamp - timedelta(hours=1),
                    OptionsHistoricalData.timestamp <= timestamp + timedelta(hours=1)
                )
            ).order_by(OptionsHistoricalData.timestamp).first()
            
            # If not found and expiry time is 15:30, try with 05:30
            if not result and expiry.hour == 15 and expiry.minute == 30:
                expiry_with_db_time = expiry.replace(hour=5, minute=30)
                result = session.query(OptionsHistoricalData).filter(
                    and_(
                        OptionsHistoricalData.strike == strike,
                        OptionsHistoricalData.option_type == option_type,
                        OptionsHistoricalData.expiry_date == expiry_with_db_time,
                        OptionsHistoricalData.timestamp >= timestamp - timedelta(hours=1),
                        OptionsHistoricalData.timestamp <= timestamp + timedelta(hours=1)
                    )
                ).order_by(OptionsHistoricalData.timestamp).first()
            
            return result
    
    async def get_available_strikes(
        self,
        expiry: datetime,
        underlying: str = "NIFTY"
    ) -> List[int]:
        """Get available strikes for an expiry"""
        with self.db_manager.get_session() as session:
            strikes = session.query(OptionsHistoricalData.strike).filter(
                and_(
                    OptionsHistoricalData.underlying == underlying,
                    OptionsHistoricalData.expiry_date == expiry
                )
            ).distinct().all()
            
            return sorted([s[0] for s in strikes])
    
    async def get_nearest_expiry(self, date: datetime) -> Optional[datetime]:
        """Get nearest weekly expiry from given date"""
        # NIFTY weekly expiry is on Thursday
        days_ahead = 3 - date.weekday()  # Thursday is 3
        if days_ahead <= 0:  # Target day already happened this week
            days_ahead += 7
        
        expiry = date + timedelta(days=days_ahead)
        return expiry.replace(hour=15, minute=30, second=0, microsecond=0)
    
    async def create_hourly_data_from_5min(
        self,
        from_date: datetime,
        to_date: datetime,
        symbol: str = "NIFTY"
    ) -> int:
        """
        Create hourly candles from 5-minute data
        
        Args:
            from_date: Start date
            to_date: End date
            symbol: Symbol to process
            
        Returns:
            Number of hourly candles created
        """
        logger.info(f"Creating hourly candles from {from_date} to {to_date}")
        
        # Get all 5-minute data for the date range
        five_min_data = await self.get_nifty_data(from_date, to_date, symbol)
        
        if not five_min_data:
            logger.warning("No 5-minute data found to aggregate")
            return 0
        
        # Group by date and create hourly candles
        current_date = from_date.date()
        hourly_count = 0
        
        while current_date <= to_date.date():
            # Get 5-minute data for this day
            day_data = [d for d in five_min_data if d.timestamp.date() == current_date]
            
            if day_data:
                # Create hourly candles
                hourly_candles = self.hourly_aggregation_service.create_hourly_bars_from_5min(day_data)
                
                # Store each hourly candle
                for candle in hourly_candles:
                    stored = self.hourly_aggregation_service.store_hourly_candle(candle)
                    if stored:
                        hourly_count += 1
            
            current_date += timedelta(days=1)
        
        logger.info(f"Created {hourly_count} hourly candles")
        return hourly_count