"""Debug why trades are not being created"""
import asyncio
import logging
from datetime import datetime, timedelta
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.services.breeze_service import BreezeService
from src.infrastructure.services.data_collection_service import DataCollectionService
from src.infrastructure.services.option_pricing_service import OptionPricingService
from src.domain.services.signal_evaluator import SignalEvaluator
from src.domain.services.weekly_context_manager import WeeklyContextManager
from src.infrastructure.database.models import NiftyIndexDataHourly, OptionsHistoricalData

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def debug_trade_creation():
    """Debug trade creation process"""
    db_manager = get_db_manager()
    breeze_service = BreezeService()
    data_collection = DataCollectionService(breeze_service, db_manager)
    option_pricing = OptionPricingService(data_collection, db_manager)
    signal_evaluator = SignalEvaluator()
    context_manager = WeeklyContextManager()
    
    # Test date when we know S5 signal occurs
    test_date = datetime(2025, 7, 14, 12, 15, 0)  # S5 signal time
    
    # Get the weekly context
    buffer_start = test_date - timedelta(days=7)
    nifty_data = await data_collection.get_nifty_data(buffer_start, test_date)
    
    # Find the exact bar
    for i, data_point in enumerate(nifty_data):
        if data_point.timestamp == test_date:
            current_bar = context_manager.create_bar_from_nifty_data(data_point)
            prev_week_data = context_manager.get_previous_week_data(
                current_bar.timestamp, nifty_data[:i]
            )
            context = context_manager.update_context(current_bar, prev_week_data)
            
            # Evaluate signals
            signal_result = signal_evaluator.evaluate_all_signals(
                current_bar, context, current_bar.timestamp
            )
            
            logger.info(f"Signal triggered: {signal_result.is_triggered}")
            if signal_result.is_triggered:
                logger.info(f"Signal type: {signal_result.signal_type}")
                logger.info(f"Strike price: {signal_result.strike_price}")
                logger.info(f"Stop loss: {signal_result.stop_loss}")
                
                # Get expiry
                week_start = context_manager.current_week_start
                logger.info(f"Week start: {week_start}")
                expiry = context_manager.get_expiry_for_week(week_start)
                logger.info(f"Expiry calculated: {expiry}")
                
                # Check option data availability
                main_strike = signal_result.strike_price
                option_type = signal_result.option_type
                
                # Look for option data
                option_price = await option_pricing.get_option_price_at_time(
                    current_bar.timestamp, main_strike, option_type, expiry
                )
                logger.info(f"Option price found: {option_price}")
                
                # Check database directly
                with db_manager.get_session() as session:
                    # Check with calculated expiry
                    opt1 = session.query(OptionsHistoricalData).filter(
                        OptionsHistoricalData.strike == main_strike,
                        OptionsHistoricalData.option_type == option_type,
                        OptionsHistoricalData.expiry_date == expiry,
                        OptionsHistoricalData.timestamp >= current_bar.timestamp - timedelta(hours=1),
                        OptionsHistoricalData.timestamp <= current_bar.timestamp + timedelta(hours=1)
                    ).first()
                    logger.info(f"Option data with expiry {expiry}: {opt1 is not None}")
                    
                    # Check with DB expiry time (5:30 AM)
                    expiry_db = expiry.replace(hour=5, minute=30)
                    opt2 = session.query(OptionsHistoricalData).filter(
                        OptionsHistoricalData.strike == main_strike,
                        OptionsHistoricalData.option_type == option_type,
                        OptionsHistoricalData.expiry_date == expiry_db,
                        OptionsHistoricalData.timestamp >= current_bar.timestamp - timedelta(hours=1),
                        OptionsHistoricalData.timestamp <= current_bar.timestamp + timedelta(hours=1)
                    ).first()
                    logger.info(f"Option data with expiry {expiry_db}: {opt2 is not None}")
                    
                    if opt2:
                        logger.info(f"Option details: Symbol={opt2.trading_symbol}, Price={opt2.close}")
            
            break

if __name__ == "__main__":
    asyncio.run(debug_trade_creation())