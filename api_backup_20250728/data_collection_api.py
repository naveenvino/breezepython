"""
Simplified API with both sync and async endpoints for NIFTY data collection
"""
from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from pydantic import BaseModel
from datetime import date, datetime, timedelta
import os
from dotenv import load_dotenv
from breeze_connect import BreezeConnect
import logging
import time
from typing import Dict, Optional, List, Tuple
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import enhanced optimizations
try:
    from enhanced_optimizations import collect_options_data_ultra_optimized
    from nifty_optimizations import collect_nifty_data_ultra_optimized
    from src.infrastructure.cache.smart_cache import cache_strike_range, cache_db_query, get_cache_stats
    ULTRA_OPTIMIZED_AVAILABLE = True
except ImportError:
    ULTRA_OPTIMIZED_AVAILABLE = False

# Import database components
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.database.models import NiftyIndexData, OptionsHistoricalData
from src.infrastructure.services.hourly_aggregation_service import HourlyAggregationService

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Optimization level configuration
# 0 = Original (sequential), 1 = Optimized (5 workers), 2 = Ultra (all optimizations)
OPTIMIZATION_LEVEL = int(os.getenv('OPTIONS_OPTIMIZATION_LEVEL', '2'))
logger.info(f"Options optimization level: {OPTIMIZATION_LEVEL}")

app = FastAPI(
    title="Market Data Collection API", 
    version="4.1.0",
    description="Comprehensive API for NIFTY index and options data collection",
    openapi_tags=[
        {
            "name": "NIFTY Collection",
            "description": "Endpoints for collecting NIFTY index data (5-minute and hourly)"
        },
        {
            "name": "Options Collection", 
            "description": "Endpoints for collecting options data based on Monday's open price"
        },
        {
            "name": "Data Check",
            "description": "Endpoints for checking data availability and coverage"
        },
        {
            "name": "Data Deletion",
            "description": "Endpoints for deleting data from the database"
        },
        {
            "name": "Job Management",
            "description": "Endpoints for managing background jobs"
        }
    ]
)

# Global status tracker for background jobs
job_status: Dict[str, dict] = {}

class CollectNiftyRequest(BaseModel):
    from_date: date
    to_date: date
    symbol: str = "NIFTY"
    force_refresh: bool = False
    extended_hours: bool = False  # If True, collect 9:20-15:35 data; If False, collect 9:15-15:30 data

class CollectOptionsRequest(BaseModel):
    from_date: date
    to_date: date
    symbol: str = "NIFTY"
    force_refresh: bool = False

class DeleteDataRequest(BaseModel):
    from_date: date
    to_date: date
    symbol: str = "NIFTY"

@app.get("/")
def root():
    return {
        "message": "Market Data Collection API", 
        "version": "4.1.0",
        "endpoints": {
            "NIFTY Index": [
                "/api/v1/collect/nifty-direct - Synchronous collection",
                "/api/v1/collect/nifty-bulk - Background collection for large ranges"
            ],
            "Options": [
                "/api/v1/collect/options-direct - Synchronous options collection",
                "/api/v1/collect/options-bulk - Background options collection"
            ],
            "Data Management": [
                "/api/v1/data/check - Check NIFTY data availability",
                "/api/v1/data/check-options - Check options data availability",
                "/api/v1/delete/nifty-direct - Delete NIFTY data by date range",
                "/api/v1/delete/options-direct - Delete options data by date range",
                "/api/v1/delete/all - Delete ALL data (requires confirmation)"
            ],
            "Common": [
                "/api/v1/status/{job_id} - Check bulk job status"
            ]
        },
        "docs": "Visit /docs for interactive API documentation"
    }

def collect_nifty_data_sync(request: CollectNiftyRequest) -> dict:
    """Synchronous data collection logic"""
    # Initialize database
    db_manager = get_db_manager()
    hourly_service = HourlyAggregationService(db_manager)
    
    # Initialize Breeze once
    breeze = BreezeConnect(api_key=os.getenv('BREEZE_API_KEY'))
    try:
        breeze.generate_session(
            api_secret=os.getenv('BREEZE_API_SECRET'),
            session_token=os.getenv('BREEZE_API_SESSION')
        )
    except Exception as e:
        logger.info(f"Session notice: {e}")
    
    # Process date range
    current_date = request.from_date
    total_added_5min = 0
    total_added_hourly = 0
    total_skipped = 0
    total_skipped_days = 0
    total_weekend_days = 0
    total_processed = 0
    errors = []
    daily_results = []
    
    while current_date <= request.to_date:
        try:
            # Skip weekends
            if current_date.weekday() >= 5:  # 5=Saturday, 6=Sunday
                logger.info(f"{current_date}: Weekend, skipping")
                total_weekend_days += 1
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "skipped",
                    "reason": "weekend"
                })
                current_date += timedelta(days=1)
                continue
            
            # Check if data already exists for this date
            from_datetime = datetime.combine(current_date, datetime.min.time())
            to_datetime = datetime.combine(current_date, datetime.max.time())
            
            with db_manager.get_session() as session:
                existing_5min = session.query(NiftyIndexData).filter(
                    NiftyIndexData.symbol == request.symbol,
                    NiftyIndexData.interval == "5minute",
                    NiftyIndexData.timestamp >= from_datetime,
                    NiftyIndexData.timestamp <= to_datetime
                ).count()
                
                existing_hourly = session.query(NiftyIndexData).filter(
                    NiftyIndexData.symbol == request.symbol,
                    NiftyIndexData.interval == "hourly",
                    NiftyIndexData.timestamp >= from_datetime,
                    NiftyIndexData.timestamp <= to_datetime
                ).count()
            
            # Skip if data is complete and force_refresh is False
            # Note: Breeze API sometimes only provides data up to 15:25
            # Consider data complete if we have at least 73 records (minimum expected)
            if existing_5min >= 73 and existing_hourly == 7 and not request.force_refresh:
                logger.info(f"{current_date}: Data already complete, skipping")
                total_skipped_days += 1
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "skipped",
                    "reason": "data_already_complete"
                })
                current_date += timedelta(days=1)
                continue
            
            logger.info(f"Processing {current_date}...")
            
            # Fetch data
            result = breeze.get_historical_data_v2(
                interval="5minute",
                from_date=from_datetime.strftime("%Y-%m-%dT00:00:00.000Z"),
                to_date=to_datetime.strftime("%Y-%m-%dT23:59:59.000Z"),
                stock_code=request.symbol,
                exchange_code="NSE",
                product_type="cash"
            )
            
            if result and 'Success' in result:
                records = result['Success']
                day_added_5min = 0
                day_skipped = 0
                
                # Store 5-minute data
                with db_manager.get_session() as session:
                    for record in records:
                        try:
                            nifty_data = NiftyIndexData.from_breeze_data(record, request.symbol, request.extended_hours)
                            if nifty_data is None:
                                day_skipped += 1
                                continue
                            
                            exists = session.query(NiftyIndexData).filter(
                                NiftyIndexData.symbol == request.symbol,
                                NiftyIndexData.timestamp == nifty_data.timestamp,
                                NiftyIndexData.interval == "5minute"
                            ).first()
                            
                            if not exists:
                                session.add(nifty_data)
                                day_added_5min += 1
                            else:
                                day_skipped += 1
                        except Exception as e:
                            logger.error(f"Error processing record: {e}")
                    
                    session.commit()
                
                # Create hourly aggregations
                day_added_hourly = 0
                if day_added_5min > 0 or existing_5min > 0:
                    logger.info(f"Creating hourly aggregations for {current_date}...")
                    
                    with db_manager.get_session() as session:
                        five_min_data = session.query(NiftyIndexData).filter(
                            NiftyIndexData.symbol == request.symbol,
                            NiftyIndexData.interval == "5minute",
                            NiftyIndexData.timestamp >= from_datetime,
                            NiftyIndexData.timestamp <= to_datetime
                        ).order_by(NiftyIndexData.timestamp).all()
                        
                        if five_min_data:
                            hourly_candles = hourly_service.create_hourly_bars_from_5min(five_min_data)
                            
                            for candle in hourly_candles:
                                if hourly_service.store_hourly_candle(candle):
                                    day_added_hourly += 1
                
                # Update totals
                total_added_5min += day_added_5min
                total_added_hourly += day_added_hourly
                total_skipped += day_skipped
                total_processed += len(records)
                
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "processed",
                    "added_5min": day_added_5min,
                    "added_hourly": day_added_hourly
                })
                
                logger.info(f"{current_date}: Added {day_added_5min} 5-min, {day_added_hourly} hourly")
            else:
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "no_data"
                })
                
        except Exception as e:
            logger.error(f"{current_date}: {str(e)}")
            errors.append(f"{current_date}: {str(e)}")
            daily_results.append({
                "date": current_date.isoformat(),
                "status": "error",
                "error": str(e)
            })
        
        current_date += timedelta(days=1)
    
    # Calculate statistics
    total_days = (request.to_date - request.from_date).days + 1
    
    return {
        "status": "success",
        "summary": {
            "total_days": total_days,
            "days_processed": total_days - total_skipped_days - total_weekend_days,
            "days_skipped_complete": total_skipped_days,
            "days_skipped_weekend": total_weekend_days,
            "total_added_5min": total_added_5min,
            "total_added_hourly": total_added_hourly
        },
        "daily_results": daily_results,
        "errors": errors if errors else None
    }

@app.post("/api/v1/collect/nifty-direct", tags=["NIFTY Collection"])
def collect_nifty_direct(request: CollectNiftyRequest):
    """
    Direct NIFTY data collection (synchronous)
    
    - Best for small to medium date ranges (< 30 days)
    - Returns immediately with results
    - Shows detailed progress
    """
    return collect_nifty_data_sync(request)

def run_bulk_collection(request: CollectNiftyRequest, job_id: str):
    """Background task for bulk collection with optimization"""
    try:
        job_status[job_id]["status"] = "running"
        job_status[job_id]["start_time"] = datetime.now().isoformat()
        
        # Add progress tracking fields
        job_status[job_id]["progress"] = 0
        job_status[job_id]["current_batch"] = None
        
        # Choose optimization level
        if OPTIMIZATION_LEVEL >= 2 and ULTRA_OPTIMIZED_AVAILABLE:
            logger.info(f"Using ULTRA optimized NIFTY collection for job {job_id}")
            result = collect_nifty_data_ultra_optimized(request, job_id)
        else:
            logger.info(f"Using original NIFTY collection for job {job_id}")
            result = collect_nifty_data_sync(request)
        
        job_status[job_id]["status"] = "completed"
        job_status[job_id]["end_time"] = datetime.now().isoformat()
        job_status[job_id]["result"] = result
        
    except Exception as e:
        job_status[job_id]["status"] = "failed"
        job_status[job_id]["error"] = str(e)
        logger.error(f"Bulk collection failed: {e}")

@app.post("/api/v1/collect/nifty-bulk", tags=["NIFTY Collection"])
async def collect_nifty_bulk(request: CollectNiftyRequest, background_tasks: BackgroundTasks):
    """
    Bulk NIFTY data collection (asynchronous)
    
    - Best for large date ranges (> 30 days, 1 year+)
    - Runs in background
    - Returns job ID immediately
    """
    job_id = f"job_{int(time.time())}_{request.symbol}"
    
    # Initialize job status
    job_status[job_id] = {
        "status": "queued",
        "request": request.dict(),
        "created_at": datetime.now().isoformat()
    }
    
    # Start background task
    background_tasks.add_task(run_bulk_collection, request, job_id)
    
    total_days = (request.to_date - request.from_date).days + 1
    
    return {
        "job_id": job_id,
        "status": "started",
        "message": f"Processing {total_days} days in background",
        "check_status_at": f"/api/v1/status/{job_id}"
    }

@app.get("/api/v1/cache/stats", tags=["System"])
def get_cache_statistics():
    """Get cache statistics"""
    if ULTRA_OPTIMIZED_AVAILABLE:
        stats = get_cache_stats()
        return {
            "status": "active",
            "statistics": stats
        }
    else:
        return {
            "status": "disabled",
            "message": "Caching not available"
        }

@app.get("/api/v1/status/{job_id}", tags=["Job Management"])
def get_job_status(job_id: str):
    """Get status of a bulk collection job"""
    
    if job_id not in job_status:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return job_status[job_id]

@app.get("/api/v1/data/check", tags=["Data Check"])
def check_data_availability(
    from_date: date = Query(..., description="Start date"),
    to_date: date = Query(..., description="End date"),
    symbol: str = Query(default="NIFTY", description="Symbol"),
    extended_hours: bool = Query(default=False, description="Check for extended hours (9:20-15:35) instead of regular (9:15-15:30) - both have 76 records")
):
    """
    Check NIFTY data availability without collection
    
    - Shows complete vs incomplete days
    - Identifies missing dates
    - Useful before running large collections
    """
    db_manager = get_db_manager()
    
    current_date = from_date
    complete_days = 0
    incomplete_days = 0
    weekend_days = 0
    missing_dates = []
    
    with db_manager.get_session() as session:
        while current_date <= to_date:
            if current_date.weekday() >= 5:
                weekend_days += 1
            else:
                from_datetime = datetime.combine(current_date, datetime.min.time())
                to_datetime = datetime.combine(current_date, datetime.max.time())
                
                count = session.query(NiftyIndexData).filter(
                    NiftyIndexData.symbol == symbol,
                    NiftyIndexData.interval == "5minute",
                    NiftyIndexData.timestamp >= from_datetime,
                    NiftyIndexData.timestamp <= to_datetime
                ).count()
                
                # Consider day complete if we have at least 73 records
                if count >= 73:
                    complete_days += 1
                else:
                    incomplete_days += 1
                    missing_dates.append({
                        "date": current_date.isoformat(),
                        "records": count,
                        "missing": expected_records - count
                    })
            
            current_date += timedelta(days=1)
    
    total_days = (to_date - from_date).days + 1
    trading_days = total_days - weekend_days
    
    return {
        "summary": {
            "total_days": total_days,
            "trading_days": trading_days,
            "complete_days": complete_days,
            "incomplete_days": incomplete_days,
            "weekend_days": weekend_days,
            "completeness": f"{(complete_days / trading_days * 100):.1f}%" if trading_days > 0 else "0%"
        },
        "missing_dates": missing_dates[:20],  # First 20
        "total_missing": len(missing_dates)
    }

# Options collection functions
def get_first_trading_day_open_price(target_date: date, symbol: str, db_manager) -> Optional[float]:
    """Get first trading day's open price for the week containing target_date"""
    # Find the Monday of the week containing target_date
    days_since_monday = target_date.weekday()
    monday = target_date - timedelta(days=days_since_monday)
    
    # If target_date is a weekend, use next Monday
    if target_date.weekday() >= 5:  # Saturday or Sunday
        monday = target_date + timedelta(days=(7 - target_date.weekday()))
    
    # Search for first available trading day in the week (Monday to Friday)
    for day_offset in range(5):  # Check Mon, Tue, Wed, Thu, Fri
        check_date = monday + timedelta(days=day_offset)
        
        # Query for this day's first 5-minute candle (9:15)
        day_start = datetime.combine(check_date, datetime.min.time())
        day_915 = day_start.replace(hour=9, minute=15)
        
        with db_manager.get_session() as session:
            first_candle = session.query(NiftyIndexData).filter(
                NiftyIndexData.symbol == symbol,
                NiftyIndexData.interval == "5minute",
                NiftyIndexData.timestamp == day_915
            ).first()
            
            if first_candle:
                logger.info(f"First trading day of week: {check_date} ({check_date.strftime('%A')}), Open: {first_candle.open}")
                return float(first_candle.open)
    
    logger.warning(f"No trading day found in the week starting {monday}")
    return None

# Apply caching if available
if ULTRA_OPTIMIZED_AVAILABLE:
    get_first_trading_day_open_price = cache_strike_range(ttl=86400)(get_first_trading_day_open_price)

def get_weekly_expiry(target_date: date) -> date:
    """Get the weekly expiry date (Thursday) for the given date"""
    # Find the Thursday of the week
    days_to_thursday = (3 - target_date.weekday()) % 7
    if days_to_thursday == 0 and target_date.weekday() == 3:
        # If it's Thursday, use this Thursday
        expiry = target_date
    else:
        # Otherwise, find next Thursday
        expiry = target_date + timedelta(days=days_to_thursday)
    
    return expiry

def collect_options_data_sync(request: CollectOptionsRequest) -> dict:
    """Synchronous options data collection logic"""
    # Initialize database
    db_manager = get_db_manager()
    
    # Initialize Breeze once
    breeze = BreezeConnect(api_key=os.getenv('BREEZE_API_KEY'))
    try:
        breeze.generate_session(
            api_secret=os.getenv('BREEZE_API_SECRET'),
            session_token=os.getenv('BREEZE_API_SESSION')
        )
    except Exception as e:
        logger.info(f"Session notice: {e}")
    
    # Process date range
    current_date = request.from_date
    total_added = 0
    total_skipped = 0
    total_processed = 0
    errors = []
    daily_results = []
    
    while current_date <= request.to_date:
        try:
            # Skip weekends
            if current_date.weekday() >= 5:
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "skipped",
                    "reason": "weekend"
                })
                current_date += timedelta(days=1)
                continue
            
            # Get first trading day's open price for this week
            first_day_open = get_first_trading_day_open_price(current_date, request.symbol, db_manager)
            if not first_day_open:
                logger.warning(f"Skipping {current_date}: No trading day found this week")
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "skipped",
                    "reason": "no_trading_day_this_week"
                })
                current_date += timedelta(days=1)
                continue
            
            # Calculate strike range (±500 points from first trading day's open)
            base_strike = int(round(first_day_open / 50) * 50)  # Round to nearest 50
            min_strike = base_strike - 500
            max_strike = base_strike + 500
            
            # Get weekly expiry
            expiry_date = get_weekly_expiry(current_date)
            
            logger.info(f"Processing {current_date}: First day open={first_day_open:.2f}, Strikes={min_strike}-{max_strike}, Expiry={expiry_date}")
            
            # Check if data already exists for this date
            from_datetime = datetime.combine(current_date, datetime.min.time())
            to_datetime = datetime.combine(current_date, datetime.max.time())
            
            with db_manager.get_session() as session:
                existing_count = session.query(OptionsHistoricalData).filter(
                    OptionsHistoricalData.underlying == request.symbol,
                    OptionsHistoricalData.timestamp >= from_datetime,
                    OptionsHistoricalData.timestamp <= to_datetime,
                    OptionsHistoricalData.strike >= min_strike,
                    OptionsHistoricalData.strike <= max_strike
                ).count()
            
            # Enhanced check: verify if all expected strikes are present
            expected_strikes = list(range(min_strike, max_strike + 50, 50))
            expected_combinations = len(expected_strikes) * 2  # CE and PE
            
            # Check which strikes actually exist
            with db_manager.get_session() as session:
                existing_strikes = session.query(
                    OptionsHistoricalData.strike,
                    OptionsHistoricalData.option_type
                ).filter(
                    OptionsHistoricalData.underlying == request.symbol,
                    OptionsHistoricalData.timestamp >= from_datetime,
                    OptionsHistoricalData.timestamp <= to_datetime,
                    OptionsHistoricalData.strike >= min_strike,
                    OptionsHistoricalData.strike <= max_strike
                ).distinct().all()
                
                existing_combinations_count = len(existing_strikes)
            
            # Skip only if ALL expected strikes are present
            if existing_combinations_count >= expected_combinations and not request.force_refresh:
                logger.info(f"{current_date}: All {expected_combinations} strike combinations exist, skipping")
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "skipped",
                    "reason": "data_complete",
                    "existing_count": existing_count,
                    "strike_combinations": existing_combinations_count
                })
                current_date += timedelta(days=1)
                continue
            elif existing_count > 0 and not request.force_refresh:
                logger.warning(f"{current_date}: Partial data exists ({existing_combinations_count}/{expected_combinations} strikes), continuing to fill gaps")
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "partial_data_warning",
                    "existing_combinations": existing_combinations_count,
                    "expected_combinations": expected_combinations
                })
            
            # Collect data for each strike
            day_added = 0
            day_errors = []
            strikes_processed = []
            
            for strike in range(min_strike, max_strike + 50, 50):  # 50 point intervals
                for option_type in ['CE', 'PE']:
                    try:
                        # Construct option symbol
                        expiry_str = expiry_date.strftime("%y%b%d").upper()
                        option_symbol = f"{request.symbol}{expiry_str}{strike}{option_type}"
                        
                        # Fetch 5-minute data
                        # Convert CE/PE to call/put for Breeze API
                        right_type = "call" if option_type == "CE" else "put"
                        
                        result = breeze.get_historical_data_v2(
                            interval="5minute",
                            from_date=from_datetime.strftime("%Y-%m-%dT00:00:00.000Z"),
                            to_date=to_datetime.strftime("%Y-%m-%dT23:59:59.000Z"),
                            stock_code=request.symbol,  # Use NIFTY, not the full option symbol
                            exchange_code="NFO",
                            product_type="options",
                            expiry_date=expiry_date.strftime("%Y-%m-%dT00:00:00.000Z"),
                            right=right_type,
                            strike_price=str(strike)
                        )
                        
                        if result and 'Success' in result:
                            records = result['Success']
                            
                            # Store data
                            with db_manager.get_session() as session:
                                for record in records:
                                    try:
                                        # Add required fields for OptionsHistoricalData
                                        record['underlying'] = request.symbol
                                        record['strike_price'] = strike
                                        record['right'] = option_type
                                        record['expiry_date'] = expiry_date.strftime("%Y-%m-%dT00:00:00.000Z")
                                        record['trading_symbol'] = option_symbol  # Add the constructed trading symbol
                                        
                                        options_data = OptionsHistoricalData.from_breeze_data(record)
                                        if options_data is None:
                                            continue
                                        
                                        # Check if exists
                                        exists = session.query(OptionsHistoricalData).filter(
                                            OptionsHistoricalData.trading_symbol == option_symbol,
                                            OptionsHistoricalData.timestamp == options_data.timestamp
                                        ).first()
                                        
                                        if not exists:
                                            session.add(options_data)
                                            day_added += 1
                                    except Exception as e:
                                        logger.error(f"Error processing record: {e}")
                                
                                session.commit()
                            
                            if len(records) > 0:
                                strikes_processed.append(f"{strike}{option_type}")
                        
                        # Small delay to avoid rate limiting
                        time.sleep(0.1)
                        
                    except Exception as e:
                        error_msg = f"{strike}{option_type}: {str(e)}"
                        day_errors.append(error_msg)
                        logger.error(f"Error fetching {option_symbol}: {e}")
            
            # Update totals
            total_added += day_added
            total_processed += 1
            
            daily_results.append({
                "date": current_date.isoformat(),
                "status": "processed",
                "first_day_open": first_day_open,
                "strike_range": f"{min_strike}-{max_strike}",
                "strikes_processed": len(strikes_processed),
                "records_added": day_added,
                "errors": day_errors if day_errors else None
            })
            
            logger.info(f"{current_date}: Added {day_added} records for {len(strikes_processed)} strikes")
            
        except Exception as e:
            logger.error(f"{current_date}: {str(e)}")
            errors.append(f"{current_date}: {str(e)}")
            daily_results.append({
                "date": current_date.isoformat(),
                "status": "error",
                "error": str(e)
            })
        
        current_date += timedelta(days=1)
    
    # Calculate statistics
    total_days = (request.to_date - request.from_date).days + 1
    
    return {
        "status": "success",
        "summary": {
            "total_days": total_days,
            "days_processed": total_processed,
            "total_records_added": total_added
        },
        "daily_results": daily_results,
        "errors": errors if errors else None
    }

def collect_options_data_optimized(request: CollectOptionsRequest, job_id: str) -> dict:
    """Optimized options collection with parallel processing and progress tracking"""
    # Initialize database
    db_manager = get_db_manager()
    
    # Initialize Breeze once
    breeze = BreezeConnect(api_key=os.getenv('BREEZE_API_KEY'))
    try:
        breeze.generate_session(
            api_secret=os.getenv('BREEZE_API_SECRET'),
            session_token=os.getenv('BREEZE_API_SESSION')
        )
    except Exception as e:
        logger.info(f"Session notice: {e}")
    
    # Process date range
    current_date = request.from_date
    total_added = 0
    total_skipped = 0
    total_processed = 0
    errors = []
    daily_results = []
    
    # Calculate total days for progress tracking
    total_days = (request.to_date - request.from_date).days + 1
    processed_days = 0
    
    while current_date <= request.to_date:
        try:
            # Update progress
            job_status[job_id]["current_date"] = current_date.isoformat()
            job_status[job_id]["processed_days"] = processed_days
            job_status[job_id]["progress"] = round((processed_days / total_days) * 100, 1)
            
            # Skip weekends
            if current_date.weekday() >= 5:
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "skipped",
                    "reason": "weekend"
                })
                current_date += timedelta(days=1)
                processed_days += 1
                continue
            
            # Get first trading day's open price for this week
            first_day_open = get_first_trading_day_open_price(current_date, request.symbol, db_manager)
            if not first_day_open:
                logger.warning(f"Skipping {current_date}: No trading day found this week")
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "skipped",
                    "reason": "no_trading_day_this_week"
                })
                current_date += timedelta(days=1)
                processed_days += 1
                continue
            
            # Calculate strike range
            base_strike = int(round(first_day_open / 50) * 50)
            min_strike = base_strike - 500
            max_strike = base_strike + 500
            
            # Get weekly expiry
            expiry_date = get_weekly_expiry(current_date)
            
            logger.info(f"Processing {current_date}: First day open={first_day_open:.2f}, Strikes={min_strike}-{max_strike}, Expiry={expiry_date}")
            
            # Check existing data
            from_datetime = datetime.combine(current_date, datetime.min.time())
            to_datetime = datetime.combine(current_date, datetime.max.time())
            
            with db_manager.get_session() as session:
                existing_count = session.query(OptionsHistoricalData).filter(
                    OptionsHistoricalData.underlying == request.symbol,
                    OptionsHistoricalData.timestamp >= from_datetime,
                    OptionsHistoricalData.timestamp <= to_datetime,
                    OptionsHistoricalData.strike >= min_strike,
                    OptionsHistoricalData.strike <= max_strike
                ).count()
            
            # Enhanced check: verify if all expected strikes are present
            expected_strikes = list(range(min_strike, max_strike + 50, 50))
            expected_combinations = len(expected_strikes) * 2  # CE and PE
            
            # Check which strikes actually exist
            with db_manager.get_session() as session:
                existing_strikes = session.query(
                    OptionsHistoricalData.strike,
                    OptionsHistoricalData.option_type
                ).filter(
                    OptionsHistoricalData.underlying == request.symbol,
                    OptionsHistoricalData.timestamp >= from_datetime,
                    OptionsHistoricalData.timestamp <= to_datetime,
                    OptionsHistoricalData.strike >= min_strike,
                    OptionsHistoricalData.strike <= max_strike
                ).distinct().all()
                
                existing_combinations_count = len(existing_strikes)
            
            # Skip only if ALL expected strikes are present
            if existing_combinations_count >= expected_combinations and not request.force_refresh:
                logger.info(f"{current_date}: All {expected_combinations} strike combinations exist, skipping")
                daily_results.append({
                    "date": current_date.isoformat(),
                    "status": "skipped",
                    "reason": "data_complete",
                    "existing_count": existing_count,
                    "strike_combinations": existing_combinations_count
                })
                current_date += timedelta(days=1)
                processed_days += 1
                continue
            elif existing_count > 0 and not request.force_refresh:
                logger.warning(f"{current_date}: Partial data exists ({existing_combinations_count}/{expected_combinations} strikes), continuing to fill gaps")
            
            # Collect data with parallel processing
            day_result = collect_options_for_day_parallel(
                breeze, db_manager, current_date, request.symbol,
                min_strike, max_strike, expiry_date
            )
            
            # Update totals
            total_added += day_result["records_added"]
            total_processed += 1
            
            daily_results.append({
                "date": current_date.isoformat(),
                "status": "processed",
                "first_day_open": first_day_open,
                "strike_range": f"{min_strike}-{max_strike}",
                "strikes_processed": day_result["strikes_processed"],
                "records_added": day_result["records_added"],
                "errors": day_result.get("errors") if day_result.get("errors") else None
            })
            
            logger.info(f"{current_date}: Added {day_result['records_added']} records for {day_result['strikes_processed']} strikes")
            
        except Exception as e:
            logger.error(f"{current_date}: {str(e)}")
            errors.append(f"{current_date}: {str(e)}")
            daily_results.append({
                "date": current_date.isoformat(),
                "status": "error",
                "error": str(e)
            })
        
        current_date += timedelta(days=1)
        processed_days += 1
    
    # Final progress update
    job_status[job_id]["progress"] = 100
    job_status[job_id]["processed_days"] = processed_days
    
    return {
        "status": "success",
        "summary": {
            "total_days": total_days,
            "days_processed": total_processed,
            "total_records_added": total_added
        },
        "daily_results": daily_results,
        "errors": errors if errors else None
    }

def collect_options_for_day_parallel(breeze, db_manager, request_date: date, symbol: str,
                                    min_strike: int, max_strike: int, expiry_date: date) -> dict:
    """Collect options data for a single day using parallel processing"""
    from_datetime = datetime.combine(request_date, datetime.min.time())
    to_datetime = datetime.combine(request_date, datetime.max.time())
    
    # Prepare all tasks
    tasks = []
    for strike in range(min_strike, max_strike + 50, 50):
        for option_type in ['CE', 'PE']:
            tasks.append((strike, option_type))
    
    results = {
        "records_added": 0,
        "strikes_processed": 0,
        "errors": []
    }
    
    # Process in parallel with ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Submit all tasks
        future_to_strike = {
            executor.submit(
                fetch_and_store_single_option,
                breeze, db_manager, from_datetime, to_datetime,
                symbol, strike, option_type, expiry_date
            ): (strike, option_type)
            for strike, option_type in tasks
        }
        
        # Process completed tasks
        for future in as_completed(future_to_strike):
            strike, option_type = future_to_strike[future]
            try:
                records_added = future.result()
                if records_added > 0:
                    results["records_added"] += records_added
                    results["strikes_processed"] += 1
            except Exception as e:
                error_msg = f"{strike}{option_type}: {str(e)}"
                results["errors"].append(error_msg)
                logger.error(f"Failed to collect {strike}{option_type}: {e}")
    
    return results

def fetch_and_store_single_option(breeze, db_manager, from_datetime: datetime, to_datetime: datetime,
                                 symbol: str, strike: int, option_type: str, expiry_date: date) -> int:
    """Fetch and store data for a single option (called in parallel)"""
    try:
        # Construct option symbol
        expiry_str = expiry_date.strftime("%y%b%d").upper()
        option_symbol = f"{symbol}{expiry_str}{strike}{option_type}"
        
        # Convert CE/PE to call/put for Breeze API
        right_type = "call" if option_type == "CE" else "put"
        
        # Fetch data
        result = breeze.get_historical_data_v2(
            interval="5minute",
            from_date=from_datetime.strftime("%Y-%m-%dT00:00:00.000Z"),
            to_date=to_datetime.strftime("%Y-%m-%dT23:59:59.000Z"),
            stock_code=symbol,
            exchange_code="NFO",
            product_type="options",
            expiry_date=expiry_date.strftime("%Y-%m-%dT00:00:00.000Z"),
            right=right_type,
            strike_price=str(strike)
        )
        
        if result and 'Success' in result:
            records = result['Success']
            
            # Store data in batch
            records_added = 0
            with db_manager.get_session() as session:
                for record in records:
                    try:
                        # Add required fields
                        record['underlying'] = symbol
                        record['strike_price'] = strike
                        record['right'] = option_type
                        record['expiry_date'] = expiry_date.strftime("%Y-%m-%dT00:00:00.000Z")
                        record['trading_symbol'] = option_symbol
                        
                        options_data = OptionsHistoricalData.from_breeze_data(record)
                        if options_data is None:
                            continue
                        
                        # Check if exists
                        exists = session.query(OptionsHistoricalData).filter(
                            OptionsHistoricalData.trading_symbol == option_symbol,
                            OptionsHistoricalData.timestamp == options_data.timestamp
                        ).first()
                        
                        if not exists:
                            session.add(options_data)
                            records_added += 1
                    except Exception as e:
                        logger.error(f"Error processing record: {e}")
                
                session.commit()
            
            return records_added
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in fetch_and_store_single_option: {e}")
        raise

@app.post("/api/v1/collect/options-direct", tags=["Options Collection"])
def collect_options_direct(request: CollectOptionsRequest):
    """
    Direct options data collection (synchronous)
    
    - Fetches first trading day's NIFTY open price
    - Collects options data for strikes ±500 points from first trading day's open
    - 5-minute data only (no aggregation)
    """
    return collect_options_data_sync(request)

def run_options_bulk_collection(request: CollectOptionsRequest, job_id: str):
    """Background task for options bulk collection with progress tracking"""
    try:
        job_status[job_id]["status"] = "running"
        job_status[job_id]["start_time"] = datetime.now().isoformat()
        
        # Add progress tracking fields
        job_status[job_id]["progress"] = 0
        job_status[job_id]["current_date"] = None
        job_status[job_id]["total_days"] = (request.to_date - request.from_date).days + 1
        job_status[job_id]["processed_days"] = 0
        
        # Choose optimization level
        if OPTIMIZATION_LEVEL == 2 and ULTRA_OPTIMIZED_AVAILABLE:
            logger.info(f"Using ULTRA optimized collection for job {job_id}")
            result = collect_options_data_ultra_optimized(request, job_id)
        elif OPTIMIZATION_LEVEL == 1:
            logger.info(f"Using optimized collection for job {job_id}")
            result = collect_options_data_optimized(request, job_id)
        else:
            logger.info(f"Using original collection for job {job_id}")
            result = collect_options_data_sync(request)
        
        job_status[job_id]["status"] = "completed"
        job_status[job_id]["end_time"] = datetime.now().isoformat()
        job_status[job_id]["result"] = result
        
    except Exception as e:
        job_status[job_id]["status"] = "failed"
        job_status[job_id]["error"] = str(e)
        logger.error(f"Options bulk collection failed: {e}")

@app.post("/api/v1/collect/options-bulk", tags=["Options Collection"])
async def collect_options_bulk(request: CollectOptionsRequest, background_tasks: BackgroundTasks):
    """
    Bulk options data collection (asynchronous)
    
    - Runs in background
    - Returns job ID immediately
    - Best for large date ranges
    """
    job_id = f"job_{int(time.time())}_{request.symbol}_options"
    
    # Initialize job status
    job_status[job_id] = {
        "status": "queued",
        "request": request.dict(),
        "created_at": datetime.now().isoformat()
    }
    
    # Start background task
    background_tasks.add_task(run_options_bulk_collection, request, job_id)
    
    total_days = (request.to_date - request.from_date).days + 1
    
    return {
        "job_id": job_id,
        "status": "started",
        "message": f"Processing {total_days} days of options data in background",
        "check_status_at": f"/api/v1/status/{job_id}"
    }

@app.get("/api/v1/data/check-options", tags=["Data Check"])
def check_options_data_availability(
    from_date: date = Query(..., description="Start date"),
    to_date: date = Query(..., description="End date"),
    symbol: str = Query(default="NIFTY", description="Symbol")
):
    """
    Check options data availability without collection
    
    - Shows data coverage by date
    - Identifies missing dates
    """
    db_manager = get_db_manager()
    
    current_date = from_date
    data_summary = []
    
    with db_manager.get_session() as session:
        while current_date <= to_date:
            if current_date.weekday() < 5:  # Weekday
                from_datetime = datetime.combine(current_date, datetime.min.time())
                to_datetime = datetime.combine(current_date, datetime.max.time())
                
                # Count options records for this date
                count = session.query(OptionsHistoricalData).filter(
                    OptionsHistoricalData.underlying == symbol,
                    OptionsHistoricalData.timestamp >= from_datetime,
                    OptionsHistoricalData.timestamp <= to_datetime
                ).count()
                
                # Get unique strikes
                unique_strikes = session.query(OptionsHistoricalData.strike).filter(
                    OptionsHistoricalData.underlying == symbol,
                    OptionsHistoricalData.timestamp >= from_datetime,
                    OptionsHistoricalData.timestamp <= to_datetime
                ).distinct().count()
                
                data_summary.append({
                    "date": current_date.isoformat(),
                    "records": count,
                    "unique_strikes": unique_strikes
                })
            
            current_date += timedelta(days=1)
    
    total_days = (to_date - from_date).days + 1
    trading_days = sum(1 for d in data_summary)
    days_with_data = sum(1 for d in data_summary if d["records"] > 0)
    
    return {
        "summary": {
            "total_days": total_days,
            "trading_days": trading_days,
            "days_with_data": days_with_data,
            "coverage": f"{(days_with_data / trading_days * 100):.1f}%" if trading_days > 0 else "0%"
        },
        "daily_summary": data_summary[:20],  # First 20
        "total_records": sum(d["records"] for d in data_summary)
    }

# Delete endpoints
@app.delete("/api/v1/delete/nifty-direct", tags=["Data Deletion"])
def delete_nifty_data(request: DeleteDataRequest):
    """
    Delete NIFTY index data for a date range
    
    - Deletes both 5-minute and hourly data
    - Returns count of deleted records
    """
    db_manager = get_db_manager()
    
    from_datetime = datetime.combine(request.from_date, datetime.min.time())
    to_datetime = datetime.combine(request.to_date, datetime.max.time())
    
    with db_manager.get_session() as session:
        # Count records before deletion
        count_5min = session.query(NiftyIndexData).filter(
            NiftyIndexData.symbol == request.symbol,
            NiftyIndexData.interval == "5minute",
            NiftyIndexData.timestamp >= from_datetime,
            NiftyIndexData.timestamp <= to_datetime
        ).count()
        
        count_hourly = session.query(NiftyIndexData).filter(
            NiftyIndexData.symbol == request.symbol,
            NiftyIndexData.interval == "hourly",
            NiftyIndexData.timestamp >= from_datetime,
            NiftyIndexData.timestamp <= to_datetime
        ).count()
        
        # Delete 5-minute data
        session.query(NiftyIndexData).filter(
            NiftyIndexData.symbol == request.symbol,
            NiftyIndexData.interval == "5minute",
            NiftyIndexData.timestamp >= from_datetime,
            NiftyIndexData.timestamp <= to_datetime
        ).delete(synchronize_session=False)
        
        # Delete hourly data
        session.query(NiftyIndexData).filter(
            NiftyIndexData.symbol == request.symbol,
            NiftyIndexData.interval == "hourly",
            NiftyIndexData.timestamp >= from_datetime,
            NiftyIndexData.timestamp <= to_datetime
        ).delete(synchronize_session=False)
        
        session.commit()
    
    return {
        "status": "success",
        "message": f"Deleted NIFTY data from {request.from_date} to {request.to_date}",
        "deleted": {
            "5minute": count_5min,
            "hourly": count_hourly,
            "total": count_5min + count_hourly
        }
    }

@app.delete("/api/v1/delete/options-direct", tags=["Data Deletion"])
def delete_options_data(request: DeleteDataRequest):
    """
    Delete options data for a date range
    
    - Deletes all options data for the specified dates
    - Returns count of deleted records
    """
    db_manager = get_db_manager()
    
    from_datetime = datetime.combine(request.from_date, datetime.min.time())
    to_datetime = datetime.combine(request.to_date, datetime.max.time())
    
    with db_manager.get_session() as session:
        # Count records before deletion
        count = session.query(OptionsHistoricalData).filter(
            OptionsHistoricalData.underlying == request.symbol,
            OptionsHistoricalData.timestamp >= from_datetime,
            OptionsHistoricalData.timestamp <= to_datetime
        ).count()
        
        # Delete options data
        session.query(OptionsHistoricalData).filter(
            OptionsHistoricalData.underlying == request.symbol,
            OptionsHistoricalData.timestamp >= from_datetime,
            OptionsHistoricalData.timestamp <= to_datetime
        ).delete(synchronize_session=False)
        
        session.commit()
    
    return {
        "status": "success",
        "message": f"Deleted options data from {request.from_date} to {request.to_date}",
        "deleted": count
    }

@app.delete("/api/v1/delete/all", tags=["Data Deletion"])
def delete_all_data(confirm: bool = Query(False, description="Set to true to confirm deletion")):
    """
    Delete ALL NIFTY and options data from database
    
    - WARNING: This will delete all data!
    - Requires confirm=true parameter
    """
    if not confirm:
        return {
            "status": "error",
            "message": "Deletion not confirmed. Set confirm=true to delete all data.",
            "warning": "This will delete ALL NIFTY index and options data from the database!"
        }
    
    db_manager = get_db_manager()
    
    with db_manager.get_session() as session:
        # Count all records
        nifty_count = session.query(NiftyIndexData).count()
        options_count = session.query(OptionsHistoricalData).count()
        
        # Delete all NIFTY data
        session.query(NiftyIndexData).delete(synchronize_session=False)
        
        # Delete all options data
        session.query(OptionsHistoricalData).delete(synchronize_session=False)
        
        session.commit()
    
    return {
        "status": "success",
        "message": "Deleted all NIFTY and options data",
        "deleted": {
            "nifty": nifty_count,
            "options": options_count,
            "total": nifty_count + options_count
        }
    }

if __name__ == "__main__":
    import uvicorn
    print("Starting Market Data Collection API on port 8002...")
    print("Visit http://localhost:8002/docs for Swagger UI")
    print("\nNIFTY Index Endpoints:")
    print("- POST /api/v1/collect/nifty-direct - Sync NIFTY collection")
    print("- POST /api/v1/collect/nifty-bulk - Async NIFTY collection") 
    print("\nOptions Endpoints:")
    print("- POST /api/v1/collect/options-direct - Sync options collection")
    print("- POST /api/v1/collect/options-bulk - Async options collection")
    print("\nData Management Endpoints:")
    print("- GET /api/v1/data/check - Check NIFTY data availability")
    print("- GET /api/v1/data/check-options - Check options data availability")
    print("- DELETE /api/v1/delete/nifty-direct - Delete NIFTY data by date range")
    print("- DELETE /api/v1/delete/options-direct - Delete options data by date range")
    print("- DELETE /api/v1/delete/all - Delete ALL data (requires confirm=true)")
    print("\nCommon Endpoints:")
    print("- GET /api/v1/status/{job_id} - Check job status")
    uvicorn.run(app, host="0.0.0.0", port=8002)