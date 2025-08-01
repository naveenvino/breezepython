"""Run full year backtest and compare with SP results"""
import asyncio
import logging
from datetime import datetime
from src.application.use_cases.run_backtest import RunBacktestUseCase
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.services.breeze_service import BreezeService
from sqlalchemy import text

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def run_backtest():
    """Run backtest for the full year"""
    db_manager = get_db_manager()
    breeze_service = BreezeService()
    
    # Create services
    from src.infrastructure.services.data_collection_service import DataCollectionService
    from src.infrastructure.services.option_pricing_service import OptionPricingService
    
    data_collection = DataCollectionService(breeze_service, db_manager)
    option_pricing = OptionPricingService(data_collection)
    
    # Create use case
    use_case = RunBacktestUseCase(data_collection, option_pricing)
    
    # Run backtest for full year (matching SP dates)
    from_date = datetime(2025, 1, 1, 9, 0, 0)
    to_date = datetime(2025, 7, 31, 16, 0, 0)
    
    logger.info(f"Running backtest from {from_date} to {to_date}")
    
    # Create parameters
    from src.application.use_cases.run_backtest import BacktestParameters
    
    params = BacktestParameters(
        from_date=from_date,
        to_date=to_date,
        initial_capital=500000,
        signals_to_test=["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"],
        lots_to_trade=10
    )
    
    backtest_id = await use_case.execute(params)
    
    logger.info(f"\nBacktest completed! ID: {backtest_id}")
    
    # Fetch results from database
    with db_manager.get_session() as session:
        from src.infrastructure.database.models import BacktestRun, BacktestTrade
        
        backtest_run = session.query(BacktestRun).filter_by(id=backtest_id).first()
        trades = session.query(BacktestTrade).filter_by(backtest_run_id=backtest_id).all()
        
        logger.info(f"Total trades: {len(trades)}")
        logger.info(f"Total P&L: {backtest_run.total_pnl:,.2f}")
        logger.info(f"Win rate: {backtest_run.win_rate:.2%}")
    
        # Show trades by signal type
        signal_counts = {}
        for trade in trades:
            signal = trade.signal_type
            if signal not in signal_counts:
                signal_counts[signal] = 0
            signal_counts[signal] += 1
        
        logger.info("\nTrades by signal:")
        for signal, count in sorted(signal_counts.items()):
            logger.info(f"  {signal}: {count}")
        
        # Compare with SP results
        logger.info("\n\nComparing with SP results:")
        logger.info("SP Total trades: 23")
        logger.info(f"Python Total trades: {len(trades)}")
        logger.info(f"Difference: {23 - len(trades)}")
        
        # Show first few trades
        logger.info("\n\nFirst 5 Python trades:")
        for i, trade in enumerate(trades[:5]):
            logger.info(f"{i+1}. {trade.entry_time.strftime('%Y-%m-%d %H:%M')} - {trade.signal_type} - P&L: {trade.pnl:,.2f}")
        
        # Check specific dates from SP
        sp_dates = [
            datetime(2025, 1, 13, 13, 15),  # S5
            datetime(2025, 1, 27, 11, 15),  # S5
            datetime(2025, 5, 15, 15, 15),  # S2
            datetime(2025, 7, 14, 11, 15),  # S1
        ]
        
        logger.info("\n\nChecking specific SP signal dates:")
        for sp_date in sp_dates:
            found = False
            for trade in trades:
                if trade.entry_time.date() == sp_date.date():
                    logger.info(f"{sp_date.strftime('%Y-%m-%d')}: Found - {trade.signal_type} at {trade.entry_time.strftime('%H:%M')}")
                    found = True
                    break
            if not found:
                logger.info(f"{sp_date.strftime('%Y-%m-%d')}: NOT FOUND in Python results")

if __name__ == "__main__":
    asyncio.run(run_backtest())