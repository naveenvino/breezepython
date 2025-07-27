"""
Breeze API Service - Working version based on successful test
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import asyncio
from breeze_connect import BreezeConnect
import time

from ...config.settings import get_settings

logger = logging.getLogger(__name__)


class BreezeServiceWorking:
    """Service for Breeze API interactions - Working version"""
    
    def __init__(self):
        self.settings = get_settings()
        self._breeze = None
        self._initialized = False
        self._init_attempts = 0
        self._max_init_attempts = 3
    
    def _initialize(self):
        """Initialize Breeze connection"""
        if self._initialized:
            return
            
        while self._init_attempts < self._max_init_attempts:
            try:
                self._init_attempts += 1
                logger.info(f"Breeze initialization attempt {self._init_attempts}")
                
                # Create new instance
                self._breeze = BreezeConnect(api_key=self.settings.breeze.api_key)
                
                # Generate session - this might fail with customer details error
                # but the session might still work for data fetching
                try:
                    self._breeze.generate_session(
                        api_secret=self.settings.breeze.api_secret,
                        session_token=self.settings.breeze.session_token
                    )
                    logger.info("Breeze session generated successfully")
                except Exception as session_error:
                    # Don't fail completely - the session might still work
                    logger.warning(f"Session generation warning (may be ignorable): {session_error}")
                
                # Mark as initialized even if session generation had warnings
                self._initialized = True
                logger.info("Breeze API initialized (session may have warnings but should work)")
                return
                
            except Exception as e:
                logger.error(f"Breeze initialization attempt {self._init_attempts} failed: {e}")
                if self._init_attempts >= self._max_init_attempts:
                    logger.error("Max initialization attempts reached")
                    raise
                else:
                    # Wait a bit before retry
                    time.sleep(0.5)
    
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
            
            logger.info(f"Fetching data for {stock_code} from {from_date} to {to_date}")
            
            # Run in thread pool since breeze_connect is synchronous
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self._breeze.get_historical_data_v2(**params)
            )
            
            # Log response format for debugging
            if result and 'Success' in result:
                if result['Success']:
                    logger.info(f"Successfully fetched {len(result['Success'])} records")
                    logger.debug(f"First timestamp: {result['Success'][0].get('datetime', 'N/A')}")
                else:
                    logger.warning("Success but no records returned")
            else:
                logger.error(f"Unexpected result format: {result}")
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching historical data: {e}")
            return {"Error": str(e), "Success": []}