"""Simple API that works correctly with all fixes"""
from fastapi import FastAPI, HTTPException
from datetime import datetime
from typing import List
import uvicorn

from src.infrastructure.services.breeze_service import BreezeService
from src.infrastructure.services.data_collection_service import DataCollectionService
from src.infrastructure.services.option_pricing_service import OptionPricingService
from src.infrastructure.database.database_manager import get_db_manager
from src.application.use_cases.run_backtest import RunBacktestUseCase, BacktestParameters
from src.infrastructure.database.models import BacktestRun, BacktestTrade

app = FastAPI(title="Simple Backtest API")

@app.post("/backtest")
async def run_backtest(
    from_date: str = "2025-07-14",
    to_date: str = "2025-07-18",
    lot_size: int = 75,
    lots_to_trade: int = 10,
    signals_to_test: List[str] = ["S1"]
):
    """Run backtest with all fixes applied"""
    try:
        # Create services directly
        db_manager = get_db_manager()
        breeze_service = BreezeService()
        data_collection = DataCollectionService(breeze_service, db_manager)
        option_pricing = OptionPricingService(data_collection, db_manager)
        
        # Create use case
        backtest_uc = RunBacktestUseCase(data_collection, option_pricing)
        
        # Create parameters
        params = BacktestParameters(
            from_date=datetime.strptime(from_date, "%Y-%m-%d").replace(hour=9, minute=0),
            to_date=datetime.strptime(to_date, "%Y-%m-%d").replace(hour=16, minute=0),
            initial_capital=500000,
            lot_size=lot_size,
            lots_to_trade=lots_to_trade,
            signals_to_test=signals_to_test,
            use_hedging=True,
            hedge_offset=200,
            commission_per_lot=40
        )
        
        # Run backtest
        print(f"Running backtest from {params.from_date} to {params.to_date}")
        backtest_id = await backtest_uc.execute(params)
        
        # Get results
        with db_manager.get_session() as session:
            run = session.query(BacktestRun).filter_by(id=backtest_id).first()
            trades = session.query(BacktestTrade).filter_by(backtest_run_id=backtest_id).all()
            
            result = {
                "success": True,
                "backtest_id": backtest_id,
                "total_trades": run.total_trades if run else 0,
                "winning_trades": run.winning_trades if run else 0,
                "losing_trades": run.losing_trades if run else 0,
                "total_pnl": float(run.total_pnl) if run and run.total_pnl else 0,
                "final_capital": float(run.final_capital) if run and run.final_capital else 500000,
                "trades": []
            }
            
            for trade in trades:
                result["trades"].append({
                    "signal_type": trade.signal_type,
                    "entry_time": trade.entry_time.isoformat(),
                    "exit_time": trade.exit_time.isoformat() if trade.exit_time else None,
                    "exit_reason": trade.exit_reason,
                    "stop_loss": float(trade.stop_loss_price),
                    "index_at_entry": float(trade.index_price_at_entry),
                    "index_at_exit": float(trade.index_price_at_exit) if trade.index_price_at_exit else None,
                    "total_pnl": float(trade.total_pnl) if trade.total_pnl else 0
                })
            
            return result
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    print("\nSimple Backtest API with All Fixes")
    print("===================================")
    print("Swagger UI: http://localhost:8002/docs")
    print("===================================\n")
    uvicorn.run(app, host="0.0.0.0", port=8002)