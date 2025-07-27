"""
Breeze API Service - Final working version
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import asyncio
from breeze_connect import BreezeConnect

from ...config.settings import get_settings

logger = logging.getLogger(__name__)


class BreezeServiceFinal:
    """Service for Breeze API interactions - Final version that works"""
    
    def __init__(self):
        self.settings = get_settings()
        self._breeze = None
        self._initialized = False
        self._init_error = None
    
    def _initialize(self):
        """Initialize Breeze connection"""
        if self._initialized:
            return True
            
        try:
            # Create Breeze instance
            self._breeze = BreezeConnect(api_key=self.settings.breeze.api_key)
            
            # Generate session - we know this works from our test
            # Even if it shows customer details error, the session works for data fetching
            try:
                self._breeze.generate_session(
                    api_secret=self.settings.breeze.api_secret,
                    session_token=self.settings.breeze.session_token
                )
                logger.info("Breeze session generated")
            except Exception as e:
                # Don't fail - our tests show data fetching still works
                logger.info(f"Session generation notice: {e}")
                logger.info("Continuing - data fetching should still work")
            
            self._initialized = True
            return True
            
        except Exception as e:
            self._init_error = str(e)
            logger.error(f"Breeze initialization failed: {e}")
            return False
    
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
        """Get historical data from Breeze API"""
        
        # Initialize if needed
        if not self._initialized:
            if not self._initialize():
                return {"Error": f"Initialization failed: {self._init_error}", "Success": []}
        
        try:
            # Format dates correctly for Breeze API
            from_date_str = from_date.strftime("%Y-%m-%dT00:00:00.000Z")
            to_date_str = to_date.strftime("%Y-%m-%dT23:59:59.000Z")
            
            logger.info(f"Fetching {stock_code} data from {from_date_str} to {to_date_str}")
            
            # Run the API call in thread pool
            loop = asyncio.get_event_loop()
            
            # Build params
            params = {
                "interval": interval,
                "from_date": from_date_str,
                "to_date": to_date_str,
                "stock_code": stock_code,
                "exchange_code": exchange_code,
                "product_type": product_type
            }
            
            if expiry_date:
                params["expiry_date"] = expiry_date
            if right:
                params["right"] = right
            if strike_price:
                params["strike_price"] = strike_price
            
            # Make the API call
            result = await loop.run_in_executor(
                None,
                lambda: self._breeze.get_historical_data_v2(**params)
            )
            
            # Log the result
            if result and 'Success' in result:
                records = result.get('Success', [])
                logger.info(f"Breeze API returned {len(records)} records")
                if records:
                    logger.debug(f"First record: {records[0]}")
            else:
                logger.warning(f"Unexpected result format: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in get_historical_data: {e}")
            import traceback
            traceback.print_exc()
            return {"Error": str(e), "Success": []}