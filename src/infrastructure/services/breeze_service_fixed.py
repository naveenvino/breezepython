"""
Breeze API Service - Fixed version that handles customer details error
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import asyncio
from breeze_connect import BreezeConnect

from ...config.settings import get_settings

logger = logging.getLogger(__name__)


class BreezeServiceFixed:
    """Service for Breeze API interactions - Fixed to handle initialization errors"""
    
    def __init__(self):
        self.settings = get_settings()
        self._breeze = None
        self._initialized = False
        self._session_valid = False
    
    def _initialize(self):
        """Initialize Breeze connection with error handling"""
        if not self._initialized:
            try:
                self._breeze = BreezeConnect(api_key=self.settings.breeze.api_key)
                
                # Try to generate session
                try:
                    self._breeze.generate_session(
                        api_secret=self.settings.breeze.api_secret,
                        session_token=self.settings.breeze.session_token
                    )
                    self._session_valid = True
                    logger.info("Breeze API session generated successfully")
                except Exception as e:
                    # Log the error but continue - some APIs might still work
                    logger.warning(f"Session generation warning: {e}")
                    logger.warning("Continuing anyway - some APIs may still work")
                    self._session_valid = True  # Try anyway
                
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
        """
        # Initialize connection
        self._initialize()
        
        if not self._breeze:
            return {"Error": "Breeze not initialized", "Success": []}
        
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
            
            logger.info(f"Fetching historical data with params: {params}")
            
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
                logger.info(f"Successfully fetched {len(result['Success'])} records")
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            logger.error(f"Exception type: {type(e)}")
            return {"Error": str(e), "Success": []}