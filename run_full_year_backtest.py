"""Run full year backtest and compare with SP results"""
import asyncio
import logging
from datetime import datetime
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.services.breeze_service import BreezeService
from src.infrastructure.services.data_collection_service import DataCollectionService
from src.infrastructure.services.option_pricing_service import OptionPricingService
from src.application.use_cases.run_backtest import RunBacktestUseCase, BacktestParameters

# Enable info logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

async def run_full_year():
    """Run backtest for full year 2025"""
    db_manager = get_db_manager()
    breeze_service = BreezeService()
    data_collection = DataCollectionService(breeze_service, db_manager)
    option_pricing = OptionPricingService(data_collection, db_manager)
    
    # Create use case
    backtest_uc = RunBacktestUseCase(data_collection, option_pricing, enable_risk_management=False)
    
    # Create parameters for full year
    params = BacktestParameters(
        from_date=datetime(2025, 1, 1, 9, 0),
        to_date=datetime(2025, 7, 31, 16, 0),
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
    print("\nThis may take a few minutes...\n")
    
    # Run backtest
    backtest_id = await backtest_uc.execute(params)
    
    print(f"\nBacktest completed: {backtest_id}")
    
    # Get results and compare with SP
    with db_manager.get_session() as session:
        from src.infrastructure.database.models import BacktestRun, BacktestTrade
        run = session.query(BacktestRun).filter_by(id=backtest_id).first()
        trades = session.query(BacktestTrade).filter_by(backtest_run_id=backtest_id).all()
        
        print(f"\n{'='*60}")
        print(f"PYTHON RESULTS:")
        print(f"{'='*60}")
        print(f"Total trades: {run.total_trades}")
        print(f"Winning trades: {run.winning_trades}")
        print(f"Losing trades: {run.losing_trades}")
        print(f"Total P&L: {run.total_pnl:,.2f}")
        print(f"Final capital: {run.final_capital:,.2f}")
        
        print(f"\n{'='*60}")
        print(f"TRADES BREAKDOWN:")
        print(f"{'='*60}")
        
        # Count by signal type
        signal_counts = {}
        for trade in trades:
            signal_counts[trade.signal_type] = signal_counts.get(trade.signal_type, 0) + 1
        
        for signal, count in sorted(signal_counts.items()):
            print(f"{signal}: {count} trades")
        
        # Show trades by month
        print(f"\n{'='*60}")
        print(f"MONTHLY BREAKDOWN:")
        print(f"{'='*60}")
        
        monthly_trades = {}
        for trade in trades:
            month = trade.entry_time.strftime("%Y-%m")
            if month not in monthly_trades:
                monthly_trades[month] = []
            monthly_trades[month].append(trade)
        
        for month in sorted(monthly_trades.keys()):
            trades_in_month = monthly_trades[month]
            print(f"\n{month}: {len(trades_in_month)} trades")
            for trade in trades_in_month:
                outcome = "WIN" if trade.total_pnl > 0 else "LOSS"
                print(f"  {trade.signal_type} on {trade.entry_time.date()}: {outcome} P&L: {trade.total_pnl:,.2f}")
        
        print(f"\n{'='*60}")
        print(f"SP RESULTS (from user data):")
        print(f"{'='*60}")
        print(f"Total trades: 23")
        print(f"S1: 3, S2: 1, S3: 5, S4: 3, S5: 4, S7: 5, S8: 1")
        print(f"Note: SP labeled one S5 as S1 on July 14")

if __name__ == "__main__":
    asyncio.run(run_full_year())