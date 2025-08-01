"""Test backtest directly"""
import asyncio
import logging
from datetime import datetime
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.services.breeze_service import BreezeService
from src.infrastructure.services.data_collection_service import DataCollectionService
from src.infrastructure.services.option_pricing_service import OptionPricingService
from src.application.use_cases.run_backtest import RunBacktestUseCase, BacktestParameters

# Enable debug logging
logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')

async def test_backtest():
    """Test backtest directly"""
    db_manager = get_db_manager()
    breeze_service = BreezeService()
    data_collection = DataCollectionService(breeze_service, db_manager)
    option_pricing = OptionPricingService(data_collection, db_manager)
    
    # Create use case
    backtest_uc = RunBacktestUseCase(data_collection, option_pricing, enable_risk_management=False)
    
    # Create parameters for July 14-18
    params = BacktestParameters(
        from_date=datetime(2025, 7, 14, 9, 0),
        to_date=datetime(2025, 7, 18, 16, 0),
        initial_capital=500000,
        lot_size=75,
        lots_to_trade=10,
        signals_to_test=["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"],
        use_hedging=True,
        hedge_offset=200,
        commission_per_lot=40
    )
    
    print(f"Running backtest from {params.from_date} to {params.to_date}")
    print(f"Signals to test: {params.signals_to_test}")
    
    # Run backtest
    backtest_id = await backtest_uc.execute(params)
    
    print(f"\nBacktest completed: {backtest_id}")
    
    # Get results
    with db_manager.get_session() as session:
        from src.infrastructure.database.models import BacktestRun, BacktestTrade
        run = session.query(BacktestRun).filter_by(id=backtest_id).first()
        trades = session.query(BacktestTrade).filter_by(backtest_run_id=backtest_id).all()
        
        print(f"\nResults:")
        print(f"  Total trades: {run.total_trades}")
        print(f"  Winning trades: {run.winning_trades}")
        print(f"  Losing trades: {run.losing_trades}")
        print(f"  Total P&L: {run.total_pnl}")
        
        if trades:
            print(f"\nTrades:")
            for trade in trades:
                print(f"  {trade.signal_type} at {trade.entry_time}")

if __name__ == "__main__":
    asyncio.run(test_backtest())