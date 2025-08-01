"""Test signal detection with debug output"""
import asyncio
import logging
from datetime import datetime, timedelta
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.services.breeze_service import BreezeService
from src.infrastructure.services.data_collection_service import DataCollectionService
from src.domain.services.signal_evaluator import SignalEvaluator
from src.domain.services.weekly_context_manager import WeeklyContextManager
from src.infrastructure.database.models import NiftyIndexDataHourly

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def test_signals():
    """Test signal detection for a specific period"""
    db_manager = get_db_manager()
    breeze_service = BreezeService()
    data_collection = DataCollectionService(breeze_service, db_manager)
    signal_evaluator = SignalEvaluator()
    context_manager = WeeklyContextManager()
    
    # Test period
    from_date = datetime(2025, 7, 14, 9, 0, 0)
    to_date = datetime(2025, 7, 18, 16, 0, 0)
    
    # Get NIFTY data including buffer for previous week
    buffer_start = from_date - timedelta(days=7)
    nifty_data = await data_collection.get_nifty_data(buffer_start, to_date)
    
    if not nifty_data:
        logger.error("No NIFTY data available")
        return
    
    logger.info(f"Found {len(nifty_data)} data points")
    
    signals_detected = []
    
    # Process each hourly bar
    for i, data_point in enumerate(nifty_data):
        current_bar = context_manager.create_bar_from_nifty_data(data_point)
        
        # Skip non-market hours
        if not context_manager.is_market_hours(current_bar.timestamp):
            continue
        
        # Need previous week data
        if i < 35:  # Approx 1 week of hourly data
            continue
        
        prev_week_data = context_manager.get_previous_week_data(
            current_bar.timestamp, nifty_data[:i]
        )
        
        if not prev_week_data:
            continue
        
        # Update context
        context = context_manager.update_context(current_bar, prev_week_data)
        
        # Log context details for first few bars
        if current_bar.timestamp.date() == from_date.date() and len(signals_detected) == 0:
            logger.info(f"\nBar at {current_bar.timestamp}:")
            logger.info(f"  NIFTY: O={current_bar.open:.2f}, H={current_bar.high:.2f}, L={current_bar.low:.2f}, C={current_bar.close:.2f}")
            if context.zones:
                logger.info(f"  Resistance Zone: {context.zones.upper_zone_bottom:.2f} - {context.zones.upper_zone_top:.2f}")
                logger.info(f"  Support Zone: {context.zones.lower_zone_bottom:.2f} - {context.zones.lower_zone_top:.2f}")
                logger.info(f"  Bias: {context.bias.bias.name}")
            logger.info(f"  Week Start: {context_manager.current_week_start}")
            logger.info(f"  Weekly bars count: {len(context.weekly_bars)}")
            if context.first_hour_bar:
                logger.info(f"  First Hour Bar: O={context.first_hour_bar.open:.2f}, C={context.first_hour_bar.close:.2f}")
        
        # Evaluate signals
        if not context.signal_triggered_this_week:
            signal_result = signal_evaluator.evaluate_all_signals(
                current_bar, context, current_bar.timestamp
            )
            
            if signal_result.is_triggered:
                logger.info(f"\n*** SIGNAL DETECTED! ***")
                logger.info(f"  Type: {signal_result.signal_type.name}")
                logger.info(f"  Time: {current_bar.timestamp}")
                logger.info(f"  Strike Price: {signal_result.strike_price}")
                logger.info(f"  Stop Loss: {signal_result.stop_loss}")
                logger.info(f"  Direction: {signal_result.direction.name}")
                
                signals_detected.append({
                    'signal': signal_result.signal_type.name,
                    'timestamp': current_bar.timestamp,
                    'strike': signal_result.strike_price,
                    'stop_loss': signal_result.stop_loss
                })
    
    logger.info(f"\n\nTotal signals detected: {len(signals_detected)}")
    for sig in signals_detected:
        logger.info(f"  {sig['signal']} at {sig['timestamp']}, Strike: {sig['strike']}")

if __name__ == "__main__":
    asyncio.run(test_signals())