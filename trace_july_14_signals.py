"""Trace signal evaluation for July 14"""
import asyncio
import logging
from datetime import datetime, timedelta
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.services.breeze_service import BreezeService
from src.infrastructure.services.data_collection_service import DataCollectionService
from src.domain.services.signal_evaluator import SignalEvaluator
from src.domain.services.weekly_context_manager import WeeklyContextManager

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def trace_july_14():
    """Trace signal evaluation hour by hour on July 14"""
    db_manager = get_db_manager()
    breeze_service = BreezeService()
    data_collection = DataCollectionService(breeze_service, db_manager)
    signal_evaluator = SignalEvaluator()
    context_manager = WeeklyContextManager()
    
    # Get data for July 14
    from_date = datetime(2025, 7, 14, 9, 0, 0)
    to_date = datetime(2025, 7, 14, 16, 0, 0)
    buffer_start = from_date - timedelta(days=7)
    
    nifty_data = await data_collection.get_nifty_data(buffer_start, to_date)
    logger.info(f"Total data points: {len(nifty_data)}")
    
    # Process each hour on July 14
    signals_found = []
    for i, data_point in enumerate(nifty_data):
        if data_point.timestamp.date() != datetime(2025, 7, 14).date():
            continue
            
        current_bar = context_manager.create_bar_from_nifty_data(data_point)
        
        # Skip non-market hours
        if not context_manager.is_market_hours(current_bar.timestamp):
            continue
        
        # Get previous week data
        prev_week_data = context_manager.get_previous_week_data(
            current_bar.timestamp, nifty_data[:i]
        )
        
        if not prev_week_data:
            logger.warning(f"No previous week data for {current_bar.timestamp}")
            continue
        
        # Update context
        context = context_manager.update_context(current_bar, prev_week_data)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Time: {current_bar.timestamp}")
        logger.info(f"NIFTY: O={current_bar.open:.2f}, H={current_bar.high:.2f}, L={current_bar.low:.2f}, C={current_bar.close:.2f}")
        logger.info(f"Weekly bars count: {len(context.weekly_bars)}")
        logger.info(f"Week start: {context_manager.current_week_start}")
        
        if context.first_hour_bar:
            logger.info(f"First hour bar: O={context.first_hour_bar.open:.2f}, C={context.first_hour_bar.close:.2f}")
        else:
            logger.info("First hour bar: Not set")
        
        # Evaluate signals
        if not context.signal_triggered_this_week:
            signal_result = signal_evaluator.evaluate_all_signals(
                current_bar, context, current_bar.timestamp
            )
            
            if signal_result.is_triggered:
                logger.info(f"\n*** SIGNAL TRIGGERED: {signal_result.signal_type.name} ***")
                signals_found.append({
                    'time': current_bar.timestamp,
                    'signal': signal_result.signal_type.name,
                    'strike': signal_result.strike_price
                })
    
    logger.info(f"\n\nTotal signals found on July 14: {len(signals_found)}")
    for sig in signals_found:
        logger.info(f"  {sig['signal']} at {sig['time']}")

if __name__ == "__main__":
    asyncio.run(trace_july_14())