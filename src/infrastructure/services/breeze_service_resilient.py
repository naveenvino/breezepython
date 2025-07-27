"""
Breeze API Service - Resilient version that works despite initialization errors
"""
import logging
from datetime import datetime
from typing import Dict, Optional, Any
import asyncio
from breeze_connect import BreezeConnect

from ...config.settings import get_settings

logger = logging.getLogger(__name__)


class BreezeServiceResilient:
    """Breeze service that continues working despite customer details error"""
    
    def __init__(self):
        self.settings = get_settings()
        self._breeze = None
        self._init_attempted = False
    
    def _ensure_breeze(self):
        """Ensure Breeze instance exists"""
        if self._breeze is None:
            try:
                self._breeze = BreezeConnect(api_key=self.settings.breeze.api_key)
                # Try session generation - ignore customer details error
                try:
                    self._breeze.generate_session(
                        api_secret=self.settings.breeze.api_secret,
                        session_token=self.settings.breeze.session_token
                    )
                    logger.info("Breeze session generated successfully")
                except Exception as e:
                    # Log but don't fail - we know data fetching still works
                    logger.info(f"Session generation notice (ignoring): {e}")
                
                self._init_attempted = True
                
            except Exception as e:
                logger.error(f"Failed to create Breeze instance: {e}")
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
        """Get historical data from Breeze API"""
        
        # Ensure Breeze is initialized
        self._ensure_breeze()
        
        if not self._breeze:
            return {"Error": "Failed to initialize Breeze", "Success": []}
        
        try:
            # Format dates
            from_date_str = from_date.strftime("%Y-%m-%dT00:00:00.000Z")
            to_date_str = to_date.strftime("%Y-%m-%dT23:59:59.000Z")
            
            logger.info(f"Fetching {stock_code} {product_type} data from {from_date_str}")
            
            # Build params
            params = {
                "interval": interval,
                "from_date": from_date_str,
                "to_date": to_date_str,
                "stock_code": stock_code,
                "exchange_code": exchange_code,
                "product_type": product_type
            }
            
            # Add optional params for options
            if expiry_date:
                params["expiry_date"] = expiry_date
            if right:
                params["right"] = right
            if strike_price:
                params["strike_price"] = strike_price
            
            # Make the API call - use partial instead of lambda
            from functools import partial
            loop = asyncio.get_event_loop()
            
            # Create the callable
            api_call = partial(self._breeze.get_historical_data_v2, **params)
            
            # Execute in thread pool
            try:
                result = await loop.run_in_executor(None, api_call)
                logger.info(f"API call completed, result type: {type(result)}")
            except Exception as api_error:
                logger.error(f"API call failed: {api_error}")
                import traceback
                traceback.print_exc()
                raise
            
            # Log results
            if result:
                if 'Success' in result and result['Success']:
                    logger.info(f"Successfully fetched {len(result['Success'])} records")
                elif 'Error' in result:
                    logger.warning(f"Breeze API error: {result['Error']}")
                else:
                    logger.warning(f"Unexpected result format: {result}")
            else:
                logger.error("Breeze API returned None")
                result = {"Error": "API returned None", "Success": []}
            
            return result
            
        except Exception as e:
            logger.error(f"Error in get_historical_data: {e}")
            import traceback
            traceback.print_exc()
            return {"Error": str(e), "Success": []}