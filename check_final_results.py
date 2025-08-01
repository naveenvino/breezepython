"""Check the final backtest results"""
from datetime import datetime
from sqlalchemy import text
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.database.models import BacktestRun, BacktestTrade

db_manager = get_db_manager()

with db_manager.get_session() as session:
    # Get the latest backtest run
    latest_run = session.query(BacktestRun).order_by(BacktestRun.created_at.desc()).first()
    
    if latest_run:
        print(f"Latest Backtest Run ID: {latest_run.id}")
        print(f"Date Range: {latest_run.from_date} to {latest_run.to_date}")
        print(f"Status: {latest_run.status}")
        print(f"Total Trades: {latest_run.total_trades}")
        print(f"Total P&L: {latest_run.total_pnl:,.2f}")
        print(f"Win Rate: {latest_run.win_rate:.2%}")
        
        # Get trades for this run
        trades = session.query(BacktestTrade).filter_by(backtest_run_id=latest_run.id).order_by(BacktestTrade.entry_time).all()
        
        # Count by signal
        signal_counts = {}
        for trade in trades:
            signal = trade.signal_type
            if signal not in signal_counts:
                signal_counts[signal] = 0
            signal_counts[signal] += 1
        
        print(f"\nTrades by Signal Type:")
        for signal, count in sorted(signal_counts.items()):
            print(f"  {signal}: {count}")
        
        print(f"\nFirst 5 trades:")
        for i, trade in enumerate(trades[:5]):
            print(f"{i+1}. {trade.entry_time.strftime('%Y-%m-%d %H:%M')} - {trade.signal_type}")
        
        print(f"\nLast 5 trades:")
        for i, trade in enumerate(trades[-5:]):
            print(f"{len(trades)-4+i}. {trade.entry_time.strftime('%Y-%m-%d %H:%M')} - {trade.signal_type}")
        
        # Compare with SP
        print("\n\nComparison with SP:")
        print("SP Total: 23 trades")
        print(f"Python Total: {len(trades)} trades") 
        print(f"Difference: {23 - len(trades)} trades")
        
        # Check specific SP dates
        sp_trades = [
            (datetime(2025, 1, 13), "S5"),
            (datetime(2025, 1, 27), "S5"),
            (datetime(2025, 5, 15), "S2"),
            (datetime(2025, 7, 14), "S1"),
        ]
        
        print("\nChecking specific SP signal dates:")
        for sp_date, expected_signal in sp_trades:
            found = False
            for trade in trades:
                if trade.entry_time.date() == sp_date.date():
                    match = "YES" if trade.signal_type == expected_signal else f"NO (got {trade.signal_type})"
                    print(f"{sp_date.strftime('%Y-%m-%d')}: Found {match}")
                    found = True
                    break
            if not found:
                print(f"{sp_date.strftime('%Y-%m-%d')}: NOT FOUND")