"""
Unified Trading API - Consolidates all trading functionality
Combines: Data Collection, Backtest (GET/POST), and Analysis
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import date, datetime, timedelta
from decimal import Decimal
import os
import sys
import logging
from sqlalchemy.orm import Session
from dotenv import load_dotenv

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import all necessary components
from src.infrastructure.database.database_manager import DatabaseManager
from src.infrastructure.services.breeze_service import BreezeService
from src.infrastructure.services.data_collection_service import DataCollectionService
from src.domain.services.signal_evaluator import SignalEvaluator
from src.domain.services.weekly_context_manager import WeeklyContextManager
from src.application.use_cases.run_backtest import RunBacktestUseCase, BacktestParameters
from src.infrastructure.services.hourly_aggregation_service import HourlyAggregationService

# Import optimization modules if available
try:
    from api.optimizations.enhanced_optimizations import collect_options_data_ultra_optimized
    from api.optimizations.nifty_optimizations import collect_nifty_data_ultra_optimized
    OPTIMIZATIONS_AVAILABLE = True
except ImportError:
    OPTIMIZATIONS_AVAILABLE = False

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Unified Trading API",
    description="Complete trading system API with data collection, backtesting, and analysis",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "Backtest",
            "description": "Backtesting operations for trading strategies"
        },
        {
            "name": "Data Collection",
            "description": "Collect NIFTY and options historical data"
        },
        {
            "name": "Data Management",
            "description": "Delete and manage stored data"
        },
        {
            "name": "Analysis",
            "description": "Analyze signals and check data availability"
        },
        {
            "name": "System",
            "description": "System health and information"
        }
    ]
)

# Initialize database
db_manager = DatabaseManager()

# ===== MODELS =====

class BacktestRequest(BaseModel):
    from_date: date = Field(..., description="Start date for backtest")
    to_date: date = Field(..., description="End date for backtest")
    initial_capital: float = Field(default=500000, description="Starting capital")
    lot_size: int = Field(default=75, description="Lot size for NIFTY")
    lots_to_trade: int = Field(default=10, description="Number of lots to trade")
    signals_to_test: List[str] = Field(default=["S1"], description="Signals to test (S1-S8)")
    use_hedging: bool = Field(default=True, description="Use hedging")
    hedge_offset: int = Field(default=200, description="Hedge offset points")
    commission_per_lot: float = Field(default=40, description="Commission per lot")

class CollectNiftyRequest(BaseModel):
    from_date: date = Field(..., description="Start date")
    to_date: date = Field(..., description="End date")
    symbol: str = Field(default="NIFTY", description="Symbol to collect")
    force_refresh: bool = Field(default=False, description="Force refresh existing data")

class CollectOptionsRequest(BaseModel):
    from_date: date = Field(..., description="Start date")
    to_date: date = Field(..., description="End date") 
    symbol: str = Field(default="NIFTY", description="Underlying symbol")
    strike_range: int = Field(default=500, description="Strike range from spot")
    use_optimization: bool = Field(default=True, description="Use optimized collection")

# ===== ENDPOINTS =====

@app.get("/", tags=["System"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Unified Trading API",
        "version": "1.0.0",
        "endpoints": {
            "backtest": {
                "POST /api/backtest": "Run backtest with JSON body",
                "GET /api/backtest": "Run backtest with query parameters"
            },
            "data_collection": {
                "POST /api/collect/nifty": "Collect NIFTY index data",
                "POST /api/collect/options": "Collect options data",
                "DELETE /api/delete/nifty": "Delete NIFTY data",
                "DELETE /api/delete/options": "Delete options data"
            },
            "analysis": {
                "GET /api/signals/available": "Get available signals",
                "GET /api/data/check": "Check data availability"
            }
        },
        "documentation": "/docs"
    }

# ===== BACKTEST ENDPOINTS =====

@app.post("/api/backtest", tags=["Backtest"], summary="Run Backtest (POST)")
async def run_backtest_post(request: BacktestRequest):
    """Run backtest with POST method (JSON body)
    
    This endpoint allows full control over backtest parameters through a JSON request body.
    """
    try:
        logger.info(f"Starting backtest from {request.from_date} to {request.to_date}")
        logger.info(f"Signals to test: {request.signals_to_test}")
        
        # Initialize services
        from src.infrastructure.services.option_pricing_service import OptionPricingService
        
        breeze_service = BreezeService()
        data_collection_service = DataCollectionService(breeze_service)
        option_pricing_service = OptionPricingService(db_manager)
        
        # Create use case
        use_case = RunBacktestUseCase(
            data_collection_service=data_collection_service,
            option_pricing_service=option_pricing_service
        )
        
        # Create parameters
        params = BacktestParameters(
            from_date=datetime.combine(request.from_date, datetime.min.time()),
            to_date=datetime.combine(request.to_date, datetime.max.time()),
            initial_capital=Decimal(str(request.initial_capital)),
            lot_size=request.lot_size,
            lots_to_trade=request.lots_to_trade,
            signals_to_test=request.signals_to_test,
            use_hedging=request.use_hedging,
            hedge_offset=request.hedge_offset,
            commission_per_lot=Decimal(str(request.commission_per_lot))
        )
        
        # Run backtest and get run_id
        run_id = await use_case.execute(params)
        
        # Get results from database
        from src.infrastructure.database.models import BacktestRun, BacktestTrade
        with db_manager.get_session() as session:
            # Get the run
            run = session.query(BacktestRun).filter_by(id=run_id).first()
            
            if not run:
                raise HTTPException(status_code=404, detail="Backtest run not found")
            
            # Get trades
            trades = session.query(BacktestTrade).filter_by(backtest_run_id=run_id).all()
            
            # Calculate summary
            summary = {
                "from_date": run.from_date.isoformat(),
                "to_date": run.to_date.isoformat(),
                "initial_capital": float(run.initial_capital),
                "final_capital": float(run.final_capital),
                "total_pnl": float(run.total_pnl),
                "total_trades": run.total_trades,
                "winning_trades": run.winning_trades if run else 0,
                "losing_trades": run.losing_trades if run else 0
            }
            
            # Signal summary
            from collections import defaultdict
            signal_stats = defaultdict(lambda: {"trades": 0, "total_pnl": 0.0})
            
            for trade in trades:
                signal_stats[trade.signal_type]["trades"] += 1
                signal_stats[trade.signal_type]["total_pnl"] += float(trade.total_pnl) if trade.total_pnl else 0
            
            for signal in signal_stats:
                if signal_stats[signal]["trades"] > 0:
                    signal_stats[signal]["avg_pnl"] = signal_stats[signal]["total_pnl"] / signal_stats[signal]["trades"]
            
            # Format trades
            trade_list = []
            for trade in trades:
                trade_list.append({
                    "signal_type": trade.signal_type,
                    "entry_time": trade.entry_time.isoformat(),
                    "exit_time": trade.exit_time.isoformat() if trade.exit_time else None,
                    "exit_reason": trade.exit_reason,
                    "stop_loss": float(trade.stop_loss_price),
                    "index_at_entry": float(trade.index_price_at_entry),
                    "index_at_exit": float(trade.index_price_at_exit) if trade.index_price_at_exit else None,
                    "total_pnl": float(trade.total_pnl) if trade.total_pnl else 0
                })
            
            result = {
                "summary": summary,
                "signal_summary": dict(signal_stats),
                "trades": trade_list
            }
            
            return {
                "status": "success",
                "result": result
            }
            
    except Exception as e:
        logger.error(f"Backtest error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/backtest", tags=["Backtest"], summary="Run Backtest (GET)")
async def run_backtest_get(
    from_date: date = Query(..., description="Start date (YYYY-MM-DD)"),
    to_date: date = Query(..., description="End date (YYYY-MM-DD)"),
    signals_to_test: str = Query("S1,S2,S3,S4,S5,S6,S7,S8", description="Comma-separated signals"),
    initial_capital: float = Query(500000, description="Initial capital"),
    lot_size: int = Query(75, description="Lot size"),
    lots_to_trade: int = Query(10, description="Lots to trade"),
    use_hedging: bool = Query(True, description="Use hedging"),
    hedge_offset: int = Query(200, description="Hedge offset"),
    commission_per_lot: float = Query(40, description="Commission per lot")
):
    """Run backtest with GET method (query parameters)"""
    # Parse signals
    signals = [s.strip() for s in signals_to_test.split(",")]
    
    # Create request object
    request = BacktestRequest(
        from_date=from_date,
        to_date=to_date,
        initial_capital=initial_capital,
        lot_size=lot_size,
        lots_to_trade=lots_to_trade,
        signals_to_test=signals,
        use_hedging=use_hedging,
        hedge_offset=hedge_offset,
        commission_per_lot=commission_per_lot
    )
    
    # Use the same logic as POST
    return await run_backtest_post(request)

# ===== DATA COLLECTION ENDPOINTS =====

@app.post("/api/collect/nifty", tags=["Data Collection"], summary="Collect NIFTY Data")
async def collect_nifty_data(request: CollectNiftyRequest):
    """Collect NIFTY index data (5-minute and hourly)
    
    Fetches historical NIFTY data and automatically creates hourly candles.
    """
    try:
        logger.info(f"Collecting NIFTY data from {request.from_date} to {request.to_date}")
        
        # Initialize services
        breeze_service = BreezeService()
        data_service = DataCollectionService(breeze_service)
        
        # Convert dates
        from_datetime = datetime.combine(request.from_date, datetime.min.time())
        to_datetime = datetime.combine(request.to_date, datetime.max.time())
        
        # Use optimized collection if available
        if OPTIMIZATIONS_AVAILABLE and request.symbol == "NIFTY":
            records = collect_nifty_data_ultra_optimized(
                breeze_service,
                from_datetime,
                to_datetime,
                db_manager
            )
            records_added = len(records) if records else 0
        else:
            # Use standard collection
            records_added = await data_service.ensure_nifty_data_available(
                from_date=from_datetime,
                to_date=to_datetime,
                symbol=request.symbol
            )
        
        return {
            "status": "success",
            "message": f"Successfully collected {request.symbol} data",
            "records_added": records_added,
            "from_date": request.from_date.isoformat(),
            "to_date": request.to_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error collecting NIFTY data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/collect/options", tags=["Data Collection"], summary="Collect Options Data")
async def collect_options_data(request: CollectOptionsRequest):
    """Collect options historical data
    
    Fetches historical options data including Greeks for the specified strike range.
    """
    try:
        logger.info(f"Collecting options data from {request.from_date} to {request.to_date}")
        
        # Initialize services
        breeze_service = BreezeService()
        
        # Convert dates
        from_datetime = datetime.combine(request.from_date, datetime.min.time())
        to_datetime = datetime.combine(request.to_date, datetime.max.time())
        
        # Use optimized collection if available
        if OPTIMIZATIONS_AVAILABLE and request.use_optimization:
            records = collect_options_data_ultra_optimized(
                breeze_service,
                request.symbol,
                from_datetime,
                to_datetime,
                request.strike_range,
                db_manager
            )
            records_added = len(records) if records else 0
        else:
            # Standard collection would go here
            raise HTTPException(status_code=501, detail="Standard collection not implemented")
        
        return {
            "status": "success",
            "message": f"Successfully collected options data",
            "records_added": records_added,
            "strike_range": request.strike_range,
            "from_date": request.from_date.isoformat(),
            "to_date": request.to_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error collecting options data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ===== DELETE ENDPOINTS =====

@app.delete("/api/delete/nifty", tags=["Data Management"], summary="Delete NIFTY Data")
async def delete_nifty_data(
    from_date: date = Query(..., description="Start date"),
    to_date: date = Query(..., description="End date"),
    symbol: str = Query("NIFTY", description="Symbol")
):
    """Delete NIFTY data for date range"""
    try:
        with db_manager.get_session() as session:
            from src.infrastructure.database.models import NiftyIndexData
            
            # Delete records
            deleted = session.query(NiftyIndexData).filter(
                NiftyIndexData.symbol == symbol,
                NiftyIndexData.timestamp >= datetime.combine(from_date, datetime.min.time()),
                NiftyIndexData.timestamp <= datetime.combine(to_date, datetime.max.time())
            ).delete()
            
            session.commit()
            
        return {
            "status": "success",
            "records_deleted": deleted,
            "symbol": symbol,
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error deleting NIFTY data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/delete/options", tags=["Data Management"], summary="Delete Options Data")
async def delete_options_data(
    from_date: date = Query(..., description="Start date"),
    to_date: date = Query(..., description="End date"),
    symbol: str = Query("NIFTY", description="Symbol")
):
    """Delete options data for date range"""
    try:
        with db_manager.get_session() as session:
            from src.infrastructure.database.models import OptionsHistoricalData
            
            # Delete records
            deleted = session.query(OptionsHistoricalData).filter(
                OptionsHistoricalData.symbol == symbol,
                OptionsHistoricalData.timestamp >= datetime.combine(from_date, datetime.min.time()),
                OptionsHistoricalData.timestamp <= datetime.combine(to_date, datetime.max.time())
            ).delete()
            
            session.commit()
            
        return {
            "status": "success",
            "records_deleted": deleted,
            "symbol": symbol,
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error deleting options data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ===== ANALYSIS ENDPOINTS =====

@app.get("/api/signals/available", tags=["Analysis"], summary="List Available Signals")
async def get_available_signals():
    """Get list of available trading signals
    
    Returns all 8 trading signals with their descriptions.
    """
    return {
        "signals": {
            "S1": "Bear Trap (Bullish) - Sell PUT",
            "S2": "Support Hold (Bullish) - Sell PUT",
            "S3": "Resistance Hold (Bearish) - Sell CALL",
            "S4": "Bias Failure Bull (Bullish) - Sell PUT",
            "S5": "Bias Failure Bear (Bearish) - Sell CALL",
            "S6": "Weakness Confirmed (Bearish) - Sell CALL",
            "S7": "Breakout Confirmed (Bullish) - Sell PUT",
            "S8": "Breakdown Confirmed (Bearish) - Sell CALL"
        }
    }

@app.get("/api/data/check", tags=["Analysis"], summary="Check Data Availability")
async def check_data_availability(
    from_date: date = Query(..., description="Start date"),
    to_date: date = Query(..., description="End date"),
    symbol: str = Query("NIFTY", description="Symbol")
):
    """Check data availability for date range"""
    try:
        with db_manager.get_session() as session:
            from src.infrastructure.database.models import NiftyIndexData, OptionsHistoricalData
            
            # Check NIFTY data
            nifty_count = session.query(NiftyIndexData).filter(
                NiftyIndexData.symbol == symbol,
                NiftyIndexData.timestamp >= datetime.combine(from_date, datetime.min.time()),
                NiftyIndexData.timestamp <= datetime.combine(to_date, datetime.max.time())
            ).count()
            
            # Check options data
            options_count = session.query(OptionsHistoricalData).filter(
                OptionsHistoricalData.symbol == symbol,
                OptionsHistoricalData.timestamp >= datetime.combine(from_date, datetime.min.time()),
                OptionsHistoricalData.timestamp <= datetime.combine(to_date, datetime.max.time())
            ).count()
            
        return {
            "symbol": symbol,
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
            "nifty_records": nifty_count,
            "options_records": options_count,
            "data_available": nifty_count > 0 and options_count > 0
        }
        
    except Exception as e:
        logger.error(f"Error checking data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ===== HEALTH CHECK =====

@app.get("/health", tags=["System"], summary="Health Check")
async def health_check():
    """Health check endpoint
    
    Checks database connectivity and optimization status.
    """
    try:
        # Test database connection
        db_healthy = db_manager.test_connection()
        
        return {
            "status": "healthy" if db_healthy else "unhealthy",
            "database": "connected" if db_healthy else "disconnected",
            "optimizations": "available" if OPTIMIZATIONS_AVAILABLE else "not available"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

# ===== MAIN =====

if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("API_PORT", "8000"))
    
    logger.info(f"Starting Unified Trading API on port {port}")
    logger.info(f"Documentation available at http://localhost:{port}/docs")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        reload=False
    )