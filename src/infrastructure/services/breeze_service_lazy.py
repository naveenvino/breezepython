"""
Breeze API Service - Lazy initialization version
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import asyncio
from breeze_connect import BreezeConnect

from ...config.settings import get_settings

logger = logging.getLogger(__name__)


class BreezeServiceLazy:
    """Service for Breeze API interactions - Lazy initialization"""
    
    def __init__(self):
        self.settings = get_settings()
        self._breeze = None
        self._initialized = False
        logger.info("BreezeServiceLazy created (not initialized yet)")
    
    def _ensure_initialized(self):
        """Initialize Breeze connection only when needed"""
        if self._initialized:
            return
            
        try:
            logger.info("Initializing Breeze API connection...")
            self._breeze = BreezeConnect(api_key=self.settings.breeze.api_key)
            
            # Try to generate session - log but don't fail if customer details error
            try:
                self._breeze.generate_session(
                    api_secret=self.settings.breeze.api_secret,
                    session_token=self.settings.breeze.session_token
                )
                logger.info("Breeze session generated successfully")
            except Exception as e:
                # This is often "Unable to retrieve customer details" which we can ignore
                logger.warning(f"Session generation warning (continuing anyway): {e}")
            
            self._initialized = True
            logger.info("Breeze API ready for use")
            
        except Exception as e:
            logger.error(f"Failed to initialize Breeze API: {e}")
            raise
    
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
        """
        Get historical data from Breeze API
        """
        # Initialize connection only when actually needed
        self._ensure_initialized()
        
        if not self._breeze:
            return {"Error": "Failed to initialize Breeze", "Success": []}
        
        try:
            # Build API parameters
            params = {
                "interval": interval,
                "from_date": from_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "to_date": to_date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "stock_code": stock_code,
                "exchange_code": exchange_code,
                "product_type": product_type
            }
            
            # Add optional parameters
            if expiry_date:
                params["expiry_date"] = expiry_date
            if right:
                params["right"] = right
            if strike_price:
                params["strike_price"] = strike_price
            
            logger.info(f"Fetching {interval} data for {stock_code}")
            
            # Run in thread pool since breeze_connect is synchronous
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._breeze.get_historical_data_v2(**params)
            )
            
            # Log results
            if result and 'Success' in result and result['Success']:
                logger.info(f"Fetched {len(result['Success'])} records")
            elif result and 'Error' in result:
                logger.error(f"Breeze API error: {result['Error']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            import traceback
            traceback.print_exc()
            return {"Error": str(e), "Success": []}