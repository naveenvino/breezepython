"""
Simplified Breeze Service - Direct approach that works
"""
import os
import logging
from datetime import datetime
from typing import Dict, Optional, Any
import asyncio
from breeze_connect import BreezeConnect
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class BreezeServiceSimple:
    """Simple Breeze service that uses direct approach"""
    
    def __init__(self):
        self._breeze = None
        self._initialized = False
    
    def _ensure_initialized(self):
        """Ensure Breeze is initialized"""
        if self._initialized and self._breeze is not None:
            return
            
        try:
            # Get credentials directly from env
            api_key = os.getenv('BREEZE_API_KEY')
            api_secret = os.getenv('BREEZE_API_SECRET')
            session_token = os.getenv('BREEZE_API_SESSION')
            
            if not all([api_key, api_secret, session_token]):
                raise ValueError("Missing Breeze credentials in environment")
            
            logger.info(f"Initializing Breeze with API key: {api_key[:10]}...")
            
            # Create Breeze instance
            self._breeze = BreezeConnect(api_key=api_key)
            
            # Generate session
            try:
                self._breeze.generate_session(
                    api_secret=api_secret,
                    session_token=session_token
                )
                logger.info("Breeze session generated successfully")
            except Exception as e:
                error_msg = str(e)
                # Customer details error is expected, continue
                if "Unable to retrieve customer details" in error_msg:
                    logger.info(f"Session generation notice (expected, continuing): {e}")
                    # DO NOT RAISE - this is normal and doesn't prevent data fetching
                # Session expired needs new token
                elif "Session key is expired" in error_msg:
                    logger.error(f"Session expired - need fresh token from user: {e}")
                    raise ValueError("Session expired - please provide a fresh session token")
                else:
                    logger.warning(f"Session generation warning (continuing): {e}")
                    # DO NOT RAISE - try to continue anyway
            
            self._initialized = True
            logger.info("Breeze service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Breeze: {e}", exc_info=True)
            self._initialized = False
            raise
    
    def get_historical_data_sync(
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
        """Get historical data - synchronous version"""
        try:
            # Ensure initialized
            self._ensure_initialized()
        except Exception as init_error:
            logger.error(f"Initialization failed: {init_error}")
            return {"Error": f"Initialization failed: {str(init_error)}", "Success": []}
        
        # Format dates
        from_date_str = from_date.strftime("%Y-%m-%dT00:00:00.000Z")
        to_date_str = to_date.strftime("%Y-%m-%dT23:59:59.000Z")
        
        try:
            logger.info(f"Fetching {stock_code} data from {from_date.date()} to {to_date.date()}")
            
            # Direct API call
            if product_type == "options" and all([expiry_date, right, strike_price]):
                result = self._breeze.get_historical_data_v2(
                    interval=interval,
                    from_date=from_date_str,
                    to_date=to_date_str,
                    stock_code=stock_code,
                    exchange_code=exchange_code,
                    product_type=product_type,
                    expiry_date=expiry_date,
                    right=right,
                    strike_price=strike_price
                )
            else:
                result = self._breeze.get_historical_data_v2(
                    interval=interval,
                    from_date=from_date_str,
                    to_date=to_date_str,
                    stock_code=stock_code,
                    exchange_code=exchange_code,
                    product_type=product_type
                )
            
            if result and 'Success' in result:
                logger.info(f"Fetched {len(result.get('Success', []))} records")
            
            return result
            
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
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
        """Async wrapper - uses asyncio.to_thread for Python 3.9+"""
        try:
            # Use asyncio.to_thread if available (Python 3.9+)
            if hasattr(asyncio, 'to_thread'):
                return await asyncio.to_thread(
                    self.get_historical_data_sync,
                    interval, from_date, to_date, stock_code,
                    exchange_code, product_type, expiry_date,
                    right, strike_price
                )
            else:
                # Fallback for older Python
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None,
                    self.get_historical_data_sync,
                    interval, from_date, to_date, stock_code,
                    exchange_code, product_type, expiry_date,
                    right, strike_price
                )
        except Exception as e:
            logger.error(f"Error in async wrapper: {e}", exc_info=True)
            # Return error response instead of raising
            return {"Error": str(e), "Success": []}