"""Debug why January signals are not being detected"""
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

async def debug_january():
    """Debug January signal detection"""
    db_manager = get_db_manager()
    breeze_service = BreezeService()
    data_collection = DataCollectionService(breeze_service, db_manager)
    signal_evaluator = SignalEvaluator()
    context_manager = WeeklyContextManager()
    
    # Check January 13 specifically (SP shows S5 signal)
    target_date = datetime(2025, 1, 13, 13, 15, 0)  # Signal candle time from SP
    buffer_start = target_date - timedelta(days=7)
    
    # Get NIFTY data
    nifty_data = await data_collection.get_nifty_data(buffer_start, target_date + timedelta(hours=2))
    
    logger.info(f"Found {len(nifty_data)} NIFTY data points")
    
    # Find the specific time
    for i, data_point in enumerate(nifty_data):
        if data_point.timestamp.date() != datetime(2025, 1, 13).date():
            continue
            
        current_bar = context_manager.create_bar_from_nifty_data(data_point)
        
        # Skip non-market hours
        if not context_manager.is_market_hours(current_bar.timestamp):
            continue
        
        # Get previous week data
        if i < 35:  # Need enough history
            continue
            
        prev_week_data = context_manager.get_previous_week_data(
            current_bar.timestamp, nifty_data[:i]
        )
        
        if not prev_week_data:
            logger.warning(f"No previous week data for {current_bar.timestamp}")
            continue
        
        # Update context
        context = context_manager.update_context(current_bar, prev_week_data)
        
        logger.info(f"\nTime: {current_bar.timestamp}")
        logger.info(f"NIFTY: O={current_bar.open:.2f}, C={current_bar.close:.2f}")
        
        if current_bar.timestamp.hour == 13 and current_bar.timestamp.minute == 15:
            logger.info("\n*** CHECKING 13:15 CANDLE (SP Signal Time) ***")
            logger.info(f"Week start: {context_manager.current_week_start}")
            logger.info(f"Weekly bars count: {len(context.weekly_bars)}")
            
            if context.zones:
                logger.info(f"Resistance Zone: {context.zones.upper_zone_bottom:.2f} - {context.zones.upper_zone_top:.2f}")
                logger.info(f"Support Zone: {context.zones.lower_zone_bottom:.2f} - {context.zones.lower_zone_top:.2f}")
                logger.info(f"Bias: {context.bias.bias.name}")
            
            if context.first_hour_bar:
                logger.info(f"First hour bar: O={context.first_hour_bar.open:.2f}, C={context.first_hour_bar.close:.2f}")
            
            # Evaluate signals
            signal_result = signal_evaluator.evaluate_all_signals(
                current_bar, context, current_bar.timestamp
            )
            
            logger.info(f"Signal triggered: {signal_result.is_triggered}")
            if signal_result.is_triggered:
                logger.info(f"Signal type: {signal_result.signal_type.name}")
    
    # Also check NIFTY data availability
    logger.info("\nChecking NIFTY data availability for January 13:")
    with db_manager.get_session() as session:
        from src.infrastructure.database.models import NiftyIndexDataHourly
        
        count = session.query(NiftyIndexDataHourly).filter(
            NiftyIndexDataHourly.timestamp >= datetime(2025, 1, 13, 9, 0),
            NiftyIndexDataHourly.timestamp <= datetime(2025, 1, 13, 16, 0)
        ).count()
        
        logger.info(f"NIFTY hourly records for Jan 13: {count}")
        
        # Get actual data points
        data_points = session.query(NiftyIndexDataHourly).filter(
            NiftyIndexDataHourly.timestamp >= datetime(2025, 1, 13, 9, 0),
            NiftyIndexDataHourly.timestamp <= datetime(2025, 1, 13, 16, 0)
        ).order_by(NiftyIndexDataHourly.timestamp).all()
        
        if data_points:
            logger.info("\nActual NIFTY data on Jan 13:")
            for dp in data_points:
                logger.info(f"  {dp.timestamp}: O={dp.open}, C={dp.close}")

if __name__ == "__main__":
    asyncio.run(debug_january())