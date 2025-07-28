"""
Fresh API server with working backtest endpoint
Run this instead of the main API server
"""
from fastapi import FastAPI, Query
from datetime import datetime, date
from typing import List, Dict
import asyncio
import uvicorn

app = FastAPI(title="Working Backtest API", version="1.0.0")

@app.get("/")
async def root():
    return {"message": "Working Backtest API", "docs": "Visit /docs for Swagger UI"}

@app.get("/backtest")
async def run_backtest(
    from_date: date = Query(default=date(2025, 7, 14), description="Start date (YYYY-MM-DD)"),
    to_date: date = Query(default=date(2025, 7, 14), description="End date (YYYY-MM-DD)"),
    initial_capital: float = Query(default=500000, description="Initial capital"),
    lot_size: int = Query(default=75, description="Lot size (75 for NIFTY)"),
    lots_to_trade: int = Query(default=10, description="Number of lots to trade (10 = 750 quantity)"),
    use_hedging: bool = Query(default=True, description="Use hedging"),
    hedge_offset: int = Query(default=200, description="Hedge offset in points"),
    commission_per_lot: float = Query(default=40, description="Commission per lot"),
    signal_s1: bool = Query(default=True, description="Test signal S1"),
    signal_s2: bool = Query(default=True, description="Test signal S2"),
    signal_s3: bool = Query(default=True, description="Test signal S3"),
    signal_s4: bool = Query(default=True, description="Test signal S4"),
    signal_s5: bool = Query(default=True, description="Test signal S5"),
    signal_s6: bool = Query(default=True, description="Test signal S6"),
    signal_s7: bool = Query(default=True, description="Test signal S7"),
    signal_s8: bool = Query(default=True, description="Test signal S8")
) -> Dict:
    """
    Run backtest with custom parameters
    
    All parameters can be modified in Swagger UI.
    Default values are set for July 14, 2025 with 10 lots.
    """
    
    # Import at runtime to avoid any caching
    from src.infrastructure.database.database_manager import get_db_manager
    from src.infrastructure.services.data_collection_service import DataCollectionService
    from src.infrastructure.services.breeze_service import BreezeService
    from src.infrastructure.services.option_pricing_service import OptionPricingService
    from src.application.use_cases.run_backtest import RunBacktestUseCase, BacktestParameters
    from src.infrastructure.database.models import BacktestRun, BacktestTrade, BacktestPosition
    
    # Build signals list
    signals_to_test = []
    if signal_s1: signals_to_test.append("S1")
    if signal_s2: signals_to_test.append("S2")
    if signal_s3: signals_to_test.append("S3")
    if signal_s4: signals_to_test.append("S4")
    if signal_s5: signals_to_test.append("S5")
    if signal_s6: signals_to_test.append("S6")
    if signal_s7: signals_to_test.append("S7")
    if signal_s8: signals_to_test.append("S8")
    
    # Create fresh instances
    db = get_db_manager()
    breeze = BreezeService()
    data_svc = DataCollectionService(breeze, db)
    option_svc = OptionPricingService(data_svc, db)
    backtest = RunBacktestUseCase(data_svc, option_svc)
    
    # Convert dates to datetime
    from_datetime = datetime.combine(from_date, datetime.strptime("09:15", "%H:%M").time())
    to_datetime = datetime.combine(to_date, datetime.strptime("15:30", "%H:%M").time())
    
    # Create parameters
    params = BacktestParameters(
        from_date=from_datetime,
        to_date=to_datetime,
        initial_capital=initial_capital,
        lot_size=lot_size,
        lots_to_trade=lots_to_trade,
        signals_to_test=signals_to_test,
        use_hedging=use_hedging,
        hedge_offset=hedge_offset,
        commission_per_lot=commission_per_lot,
        slippage_percent=0.001
    )
    
    # Run backtest
    backtest_id = await backtest.execute(params)
    
    # Get results
    with db.get_session() as session:
        run = session.query(BacktestRun).filter_by(id=backtest_id).first()
        trades = session.query(BacktestTrade).filter_by(backtest_run_id=backtest_id).all()
        
        trade_details = []
        for trade in trades:
            positions = session.query(BacktestPosition).filter_by(trade_id=trade.id).all()
            
            pos_details = []
            for pos in positions:
                pos_details.append({
                    "type": pos.position_type,
                    "action": "SELL" if pos.quantity < 0 else "BUY",
                    "lots": abs(pos.quantity) // lot_size,
                    "quantity": abs(pos.quantity),
                    "strike": pos.strike_price,
                    "option_type": pos.option_type
                })
            
            trade_details.append({
                "signal": trade.signal_type,
                "entry_time": str(trade.entry_time),
                "outcome": trade.outcome.value,
                "pnl": float(trade.total_pnl) if trade.total_pnl else 0,
                "positions": pos_details
            })
        
        return {
            "success": True,
            "backtest_id": backtest_id,
            "request_params": {
                "from_date": str(from_date),
                "to_date": str(to_date),
                "lots_to_trade": lots_to_trade,
                "signals_tested": signals_to_test
            },
            "results": {
                "total_trades": run.total_trades,
                "winning_trades": run.winning_trades,
                "losing_trades": run.losing_trades,
                "win_rate": float(run.win_rate) if run.win_rate else 0,
                "initial_capital": float(run.initial_capital),
                "final_capital": float(run.final_capital),
                "total_pnl": float(run.total_pnl) if run.total_pnl else 0
            },
            "configuration": {
                "lot_size": run.lot_size,
                "lots_traded": run.lots_to_trade,
                "total_quantity_per_trade": run.lot_size * run.lots_to_trade,
                "hedge_offset": run.hedge_offset,
                "commission_per_lot": float(run.commission_per_lot)
            },
            "trades": trade_details
        }

if __name__ == "__main__":
    print("Starting Fresh API Server with Working Backtest Endpoint")
    print("="*60)
    print("Swagger UI: http://localhost:8001/docs")
    print("Endpoint: GET /backtest")
    print("="*60)
    
    # Run on different port to avoid conflicts
    uvicorn.run(app, host="0.0.0.0", port=8001)