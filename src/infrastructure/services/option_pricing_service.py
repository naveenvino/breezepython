"""
Option Pricing Service
Service for calculating option prices and managing option data
"""
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple, List
import numpy as np
from scipy.stats import norm

from ..database.models import OptionsHistoricalData
from .data_collection_service import DataCollectionService


logger = logging.getLogger(__name__)


class OptionPricingService:
    """
    Service for option pricing and strike selection
    """
    
    def __init__(self, data_collection_service: DataCollectionService, db_manager=None):
        self.data_collection = data_collection_service
        self.db_manager = db_manager  # Can be None, service will handle it
        self.risk_free_rate = 0.065  # 6.5% annual
    
    def calculate_atm_strike(self, spot_price: float, strike_interval: int = 50) -> int:
        """
        Calculate At-The-Money (ATM) strike price
        
        Args:
            spot_price: Current NIFTY spot price
            strike_interval: Strike interval (default 50 for NIFTY)
            
        Returns:
            ATM strike price
        """
        return int(round(spot_price / strike_interval) * strike_interval)
    
    def get_option_strikes_for_signal(
        self, 
        spot_price: float,
        signal_type: str,
        hedge_offset: int = 500
    ) -> Tuple[int, int]:
        """
        Get main and hedge strikes for a signal
        
        Args:
            spot_price: Current NIFTY spot price
            signal_type: Signal type (S1-S8)
            hedge_offset: Points away from main strike for hedge
            
        Returns:
            Tuple of (main_strike, hedge_strike)
        """
        atm_strike = self.calculate_atm_strike(spot_price)
        
        # Determine strikes based on signal type
        if signal_type in ['S1', 'S2', 'S4', 'S7']:  # Bullish signals
            # Sell PUT at or below ATM
            main_strike = atm_strike
            # Buy PUT further OTM for hedge
            hedge_strike = main_strike - hedge_offset
        else:  # Bearish signals (S3, S5, S6, S8)
            # Sell CALL at or above ATM
            main_strike = atm_strike
            # Buy CALL further OTM for hedge
            hedge_strike = main_strike + hedge_offset
        
        return main_strike, hedge_strike
    
    async def get_option_price_at_time(
        self,
        timestamp: datetime,
        strike: int,
        option_type: str,
        expiry: datetime
    ) -> Optional[float]:
        """
        Get option price at specific timestamp from database
        
        Returns:
            Option price or None if not found
        """
        option_data = await self.data_collection.get_option_data(
            timestamp, strike, option_type, expiry
        )
        
        if option_data:
            # Use mid price between bid and ask if available
            if option_data.bid_price and option_data.ask_price:
                return float((option_data.bid_price + option_data.ask_price) / 2)
            else:
                return float(option_data.last_price)
        
        # If no data, estimate using Black-Scholes
        return await self._estimate_option_price(
            timestamp, strike, option_type, expiry
        )
    
    async def _estimate_option_price(
        self,
        timestamp: datetime,
        strike: int,
        option_type: str,
        expiry: datetime
    ) -> Optional[float]:
        """
        Estimate option price using Black-Scholes model
        """
        try:
            # Get NIFTY spot price at timestamp
            nifty_data = await self.data_collection.get_nifty_data(
                timestamp - timedelta(minutes=30),
                timestamp + timedelta(minutes=30)
            )
            
            if not nifty_data:
                logger.warning(f"No NIFTY data found for {timestamp}")
                return None
            
            spot_price = float(nifty_data[0].close)
            
            # Calculate time to expiry in years
            time_to_expiry = (expiry - timestamp).total_seconds() / (365 * 24 * 3600)
            
            if time_to_expiry <= 0:
                # Option expired
                if option_type == 'CE':
                    return max(0, spot_price - strike)
                else:  # PE
                    return max(0, strike - spot_price)
            
            # Estimate IV based on moneyness and time to expiry
            moneyness = spot_price / strike
            base_iv = 0.15  # 15% base IV
            
            # Adjust IV based on moneyness
            if 0.95 <= moneyness <= 1.05:  # ATM
                iv = base_iv
            elif moneyness > 1.05:  # ITM for CE, OTM for PE
                if option_type == 'CE':
                    iv = base_iv * 0.8
                else:
                    iv = base_iv * 1.2
            else:  # OTM for CE, ITM for PE
                if option_type == 'CE':
                    iv = base_iv * 1.2
                else:
                    iv = base_iv * 0.8
            
            # Black-Scholes calculation
            price = self._black_scholes(
                spot_price, strike, time_to_expiry, 
                self.risk_free_rate, iv, option_type
            )
            
            return price
            
        except Exception as e:
            logger.error(f"Error estimating option price: {e}")
            return None
    
    def _black_scholes(
        self,
        S: float,  # Spot price
        K: float,  # Strike price
        T: float,  # Time to expiry in years
        r: float,  # Risk-free rate
        sigma: float,  # Volatility
        option_type: str  # 'CE' or 'PE'
    ) -> float:
        """
        Calculate option price using Black-Scholes formula
        """
        if T <= 0:
            if option_type == 'CE':
                return max(0, S - K)
            else:
                return max(0, K - S)
        
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        if option_type == 'CE':
            price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else:  # PE
            price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
        
        return max(0, price)
    
    def calculate_option_payoff(
        self,
        option_type: str,
        strike: int,
        spot_at_expiry: float,
        premium: float,
        quantity: int,  # Negative for sell
        commission: float = 40
    ) -> float:
        """
        Calculate option P&L at expiry
        
        Args:
            option_type: 'CE' or 'PE'
            strike: Strike price
            spot_at_expiry: NIFTY price at expiry
            premium: Premium paid/received per unit
            quantity: Quantity (negative for sell)
            commission: Commission per lot
            
        Returns:
            Net P&L including commission
        """
        # Calculate intrinsic value at expiry
        if option_type == 'CE':
            intrinsic_value = max(0, spot_at_expiry - strike)
        else:  # PE
            intrinsic_value = max(0, strike - spot_at_expiry)
        
        # P&L calculation
        if quantity < 0:  # Sold option
            # Received premium, pay intrinsic value if ITM
            pnl = abs(quantity) * (premium - intrinsic_value)
        else:  # Bought option
            # Paid premium, receive intrinsic value if ITM
            pnl = quantity * (intrinsic_value - premium)
        
        # Subtract commission
        lots = abs(quantity) // 50  # Assuming lot size of 50
        total_commission = lots * commission * 2  # Entry and exit
        
        return pnl - total_commission
    
    async def get_option_chain_at_time(
        self,
        timestamp: datetime,
        expiry: datetime,
        strikes: List[int]
    ) -> dict:
        """
        Get option chain data at specific timestamp
        
        Returns:
            Dictionary with strike as key and CE/PE prices
        """
        chain = {}
        
        for strike in strikes:
            ce_price = await self.get_option_price_at_time(
                timestamp, strike, 'CE', expiry
            )
            pe_price = await self.get_option_price_at_time(
                timestamp, strike, 'PE', expiry
            )
            
            chain[strike] = {
                'CE': ce_price,
                'PE': pe_price,
                'total': (ce_price or 0) + (pe_price or 0)
            }
        
        return chain
    
    def calculate_margin_required(
        self,
        spot_price: float,
        strike: int,
        option_type: str,
        quantity: int,
        lot_size: int = 50
    ) -> float:
        """
        Calculate margin required for option position
        
        Simplified calculation - actual margin varies by broker
        """
        # For option selling, approximate margin requirement
        if quantity < 0:  # Selling option
            # Roughly 15% of notional value
            notional_value = abs(quantity) * spot_price
            margin = notional_value * 0.15
            
            # Add some buffer for OTM options
            moneyness = spot_price / strike if option_type == 'CE' else strike / spot_price
            if moneyness < 0.95:  # OTM
                margin *= 0.8
            
            return margin
        else:  # Buying option
            # Only premium required
            return 0  # Premium handled separately
    
    def get_strike_list(self, spot_price: float, num_strikes: int = 20) -> List[int]:
        """
        Get list of strikes around current spot price
        
        Args:
            spot_price: Current NIFTY spot price
            num_strikes: Number of strikes on each side of ATM
            
        Returns:
            List of strike prices
        """
        atm_strike = self.calculate_atm_strike(spot_price)
        strike_interval = 50
        
        strikes = []
        for i in range(-num_strikes, num_strikes + 1):
            strike = atm_strike + (i * strike_interval)
            if strike > 0:  # Ensure positive strikes
                strikes.append(strike)
        
        return sorted(strikes)