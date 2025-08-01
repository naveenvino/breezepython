"""Debug S1 evaluation on July 14"""
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

async def debug_july14():
    """Debug S1 signal on July 14"""
    db_manager = get_db_manager()
    breeze_service = BreezeService()
    data_collection = DataCollectionService(breeze_service, db_manager)
    signal_evaluator = SignalEvaluator()
    context_manager = WeeklyContextManager()
    
    # Get data for July 14
    from_date = datetime(2025, 7, 14, 9, 0, 0)
    to_date = datetime(2025, 7, 14, 12, 0, 0)
    buffer_start = from_date - timedelta(days=7)
    
    nifty_data = await data_collection.get_nifty_data(buffer_start, to_date)
    
    # Process up to 10:15 (second bar)
    for i, data_point in enumerate(nifty_data):
        if data_point.timestamp.date() != datetime(2025, 7, 14).date():
            continue
            
        current_bar = context_manager.create_bar_from_nifty_data(data_point)
        
        if not context_manager.is_market_hours(current_bar.timestamp):
            continue
        
        # Get previous week data
        prev_week_data = context_manager.get_previous_week_data(
            current_bar.timestamp, nifty_data[:i]
        )
        
        if not prev_week_data:
            continue
        
        # Update context
        context = context_manager.update_context(current_bar, prev_week_data)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Time: {current_bar.timestamp}")
        logger.info(f"Bar #{len(context.weekly_bars)}")
        logger.info(f"NIFTY: O={current_bar.open:.2f}, H={current_bar.high:.2f}, L={current_bar.low:.2f}, C={current_bar.close:.2f}")
        
        if context.first_hour_bar:
            logger.info(f"First bar: O={context.first_hour_bar.open:.2f}, C={context.first_hour_bar.close:.2f}, L={context.first_hour_bar.low:.2f}")
        
        if context.zones:
            logger.info(f"Support Zone Bottom: {context.zones.lower_zone_bottom:.2f}")
            logger.info(f"Bias: {context.bias.bias.name}")
        
        # Check S1 specifically at 10:15 (second bar)
        if current_bar.timestamp.hour == 10 and current_bar.timestamp.minute == 15:
            logger.info("\n*** CHECKING S1 CONDITIONS AT 10:15 ***")
            
            first_bar = context.weekly_bars[0] if context.weekly_bars else None
            is_second_bar = len(context.weekly_bars) == 2
            
            logger.info(f"Is second bar: {is_second_bar}")
            
            if first_bar:
                # S1 conditions from SP data
                cond1 = first_bar.open >= context.zones.lower_zone_bottom
                cond2 = first_bar.close < context.zones.lower_zone_bottom
                cond3 = current_bar.close > first_bar.low
                
                logger.info(f"\nS1 Conditions:")
                logger.info(f"  1. First bar open ({first_bar.open:.2f}) >= Support bottom ({context.zones.lower_zone_bottom:.2f}): {cond1}")
                logger.info(f"  2. First bar close ({first_bar.close:.2f}) < Support bottom ({context.zones.lower_zone_bottom:.2f}): {cond2}")
                logger.info(f"  3. Current close ({current_bar.close:.2f}) > First bar low ({first_bar.low:.2f}): {cond3}")
                logger.info(f"  All conditions met: {cond1 and cond2 and cond3}")
                
                # Also check what the signal evaluator returns
                signal_result = signal_evaluator.evaluate_all_signals(
                    current_bar, context, current_bar.timestamp
                )
                
                logger.info(f"\nSignal evaluator result: {signal_result.signal_type.name if signal_result.is_triggered else 'No signal'}")

if __name__ == "__main__":
    asyncio.run(debug_july14())