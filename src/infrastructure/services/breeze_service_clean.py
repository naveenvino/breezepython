"""
Breeze API Service - Real Data Only
Service for interacting with Breeze Connect API
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import asyncio
from breeze_connect import BreezeConnect

from ...config.settings import get_settings

logger = logging.getLogger(__name__)


class BreezeService:
    """Service for Breeze API interactions - Real Data Only"""
    
    def __init__(self):
        self.settings = get_settings()
        self._breeze = None
        self._initialized = False
    
    def _initialize(self):
        """Initialize Breeze connection"""
        if not self._initialized:
            try:
                self._breeze = BreezeConnect(api_key=self.settings.breeze.api_key)
                self._breeze.generate_session(
                    api_secret=self.settings.breeze.api_secret,
                    session_token=self.settings.breeze.session_token
                )
                self._initialized = True
                logger.info("Breeze API connection initialized")
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
        
        Args:
            interval: Time interval (5minute, 1hour, etc.)
            from_date: Start date
            to_date: End date
            stock_code: Stock symbol
            exchange_code: Exchange code (NSE, NFO, etc.)
            product_type: Product type (cash, futures, options)
            expiry_date: For options/futures
            right: For options (call/put)
            strike_price: For options
            
        Returns:
            Dict containing historical data
        """
        # Initialize connection
        self._initialize()
        
        try:
            # Build API parameters with correct date format
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
            
            # Run in thread pool since breeze_connect is synchronous
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._breeze.get_historical_data_v2(**params)
            )
            
            # Log response format for debugging
            if result and 'Success' in result and len(result['Success']) > 0:
                sample = result['Success'][0]
                logger.debug(f"Sample data format: {sample.keys()}")
                logger.debug(f"Sample timestamp: {sample.get('datetime', 'N/A')}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            logger.error(f"Exception type: {type(e)}")
            logger.error(f"Full error details: {repr(e)}")
            # Try to extract more specific error info
            if hasattr(e, 'response'):
                logger.error(f"Response status: {e.response.status_code if hasattr(e.response, 'status_code') else 'N/A'}")
                logger.error(f"Response text: {e.response.text if hasattr(e.response, 'text') else 'N/A'}")
            return {"Error": str(e), "Success": []}
    
    async def get_option_chain(
        self,
        stock_code: str,
        exchange_code: str = "NFO",
        product_type: str = "options",
        expiry_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get option chain data"""
        self._initialize()
        
        try:
            params = {
                "stock_code": stock_code,
                "exchange_code": exchange_code,
                "product_type": product_type
            }
            
            if expiry_date:
                params["expiry_date"] = expiry_date.strftime("%Y-%m-%d")
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._breeze.get_option_chain_quotes(**params)
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching option chain: {e}")
            return {"Error": str(e), "Success": []}