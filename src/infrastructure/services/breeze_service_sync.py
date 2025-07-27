"""
Breeze API Service - Synchronous wrapper for async context
This works by running synchronous Breeze calls in a thread pool executor
"""
import logging
from datetime import datetime
from typing import Dict, Optional, Any
import asyncio
from concurrent.futures import ThreadPoolExecutor
from breeze_connect import BreezeConnect

from ...config.settings import get_settings

logger = logging.getLogger(__name__)

# Global thread pool for Breeze calls
_executor = ThreadPoolExecutor(max_workers=5)


class BreezeServiceSync:
    """Breeze service that properly handles sync calls in async context"""
    
    def __init__(self):
        self.settings = get_settings()
        self._breeze = None
        self._initialized = False
    
    def _init_breeze_sync(self):
        """Initialize Breeze connection - synchronous"""
        if self._initialized:
            return
            
        try:
            logger.info(f"Initializing Breeze with API key: {self.settings.breeze.api_key[:10]}...")
            self._breeze = BreezeConnect(api_key=self.settings.breeze.api_key)
            
            # Check if object created properly
            logger.info(f"Breeze object created: {self._breeze}")
            logger.info(f"Has get_historical_data_v2: {hasattr(self._breeze, 'get_historical_data_v2')}")
            
            try:
                self._breeze.generate_session(
                    api_secret=self.settings.breeze.api_secret,
                    session_token=self.settings.breeze.session_token
                )
                logger.info("Breeze session generated successfully")
            except Exception as e:
                # Customer details error is normal, continue
                logger.info(f"Session generation notice (normal): {e}")
            
            self._initialized = True
            
        except Exception as e:
            logger.error(f"Failed to initialize Breeze: {e}")
            raise
    
    def _get_historical_data_sync(
        self,
        interval: str,
        from_date: str,
        to_date: str,
        stock_code: str,
        exchange_code: str = "NSE",
        product_type: str = "cash",
        expiry_date: Optional[str] = None,
        right: Optional[str] = None,
        strike_price: Optional[str] = None
    ) -> Dict[str, Any]:
        """Synchronous method to get historical data"""
        # Ensure initialized
        self._init_breeze_sync()
        
        try:
            logger.info(f"Making API call for {stock_code} from {from_date} to {to_date}")
            
            # Direct synchronous call
            if product_type == "options" and all([expiry_date, right, strike_price]):
                result = self._breeze.get_historical_data_v2(
                    interval=interval,
                    from_date=from_date,
                    to_date=to_date,
                    stock_code=stock_code,
                    exchange_code=exchange_code,
                    product_type=product_type,
                    expiry_date=expiry_date,
                    right=right,
                    strike_price=strike_price
                )
            else:
                # For NIFTY cash
                result = self._breeze.get_historical_data_v2(
                    interval=interval,
                    from_date=from_date,
                    to_date=to_date,
                    stock_code=stock_code,
                    exchange_code=exchange_code,
                    product_type=product_type
                )
            
            logger.info(f"API call returned: {type(result)}")
            
            if result is None:
                logger.error("API returned None")
                return {"Error": "API returned None", "Success": []}
            
            if result and 'Success' in result:
                logger.info(f"Fetched {len(result.get('Success', []))} records for {stock_code}")
            else:
                logger.warning(f"Unexpected result format: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching data: {e}", exc_info=True)
            return {"Error": str(e), "Success": []}
    
    async def get_historical_data(
        self,
        interval: str,
        from_date: datetime,
        to_date: datetime,
        stock_code: str,
        exchange_code: str = "NSE",
        product_type: str = "cash",
        expiry_date: Optional[str] = None,
        right: Optional[str] = None,
        strike_price: Optional[str] = None
    ) -> Dict[str, Any]:
        """Async wrapper for historical data - runs sync call in thread pool"""
        
        # Format dates
        from_date_str = from_date.strftime("%Y-%m-%dT00:00:00.000Z")
        to_date_str = to_date.strftime("%Y-%m-%dT23:59:59.000Z")
        
        logger.info(f"Fetching {stock_code} {product_type} data from {from_date.date()} to {to_date.date()}")
        
        # Run synchronous call in thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            _executor,
            self._get_historical_data_sync,
            interval,
            from_date_str,
            to_date_str,
            stock_code,
            exchange_code,
            product_type,
            expiry_date,
            right,
            strike_price
        )
        
        return result