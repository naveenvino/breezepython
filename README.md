# BreezeConnect Trading System - Clean Architecture

A comprehensive trading platform built with Clean Architecture principles, integrating Breeze API for automated trading, backtesting, and market data collection.

## 🚀 Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Environment
Create a `.env` file in the root directory:
```env
# Database
DB_SERVER=(localdb)\mssqllocaldb
DB_NAME=KiteConnectApi

# Breeze API
BREEZE_API_KEY=your_api_key
BREEZE_API_SECRET=your_api_secret
BREEZE_SESSION_TOKEN=your_session_token
```

### 3. Available APIs

#### Unified Trading API (Recommended) - Port 8000
```bash
# Start the unified API with all features
python -m api.unified_trading_api

# Access at http://localhost:8000/docs
```

This consolidated API includes:
- ✅ Backtest (both GET and POST methods)
- ✅ Data Collection (NIFTY and Options)
- ✅ Data Deletion
- ✅ Signal Analysis
- ✅ Health Checks

#### Alternative APIs (in api/ folder)
If you prefer to use individual APIs:
```bash
# Main Clean Architecture API
python -m src.api.main

# Data Collection API
python -m api.data_collection_api

# Backtest POST API
python -m api.backtest_api_post

# Backtest GET API  
python -m api.backtest_api_get
```

## 📁 Project Structure

```
breezepython/
├── src/                          # Clean architecture source code
│   ├── domain/                   # Core business logic
│   ├── application/              # Use cases and DTOs
│   ├── infrastructure/           # External services, database
│   └── api/                      # FastAPI routes
├── api/                          # All API modules
│   ├── unified_trading_api.py    # 🆕 Complete unified API (recommended)
│   ├── data_collection_api.py    # NIFTY & options data collection
│   ├── backtest_api_post.py      # POST endpoint backtest
│   ├── backtest_api_get.py       # GET endpoint backtest
│   └── optimizations/            # Performance optimization modules
├── api_backup_20250728/          # Backup of original APIs
├── scripts/                      # Utility scripts
├── docs/                         # Documentation
└── tests/                        # Test suite
```

## 🛠️ Key Features

### 1. Data Collection API
- **NIFTY Index Data**: 5-minute and hourly candles
- **Options Data**: Historical options prices with Greeks
- **Bulk Operations**: Optimized for large date ranges
- **Smart Caching**: Reduces API calls and improves performance

### 2. Backtest System
- **8 Trading Signals**: S1-S8 with customizable parameters
- **Position Management**: Entry, exit, stop loss logic
- **Risk Management**: Configurable lot sizes and hedging
- **Performance Metrics**: Detailed P&L analysis

### 3. Clean Architecture
- **Domain Layer**: Pure business logic
- **Application Layer**: Use cases orchestration
- **Infrastructure Layer**: External integrations
- **Dependency Injection**: Loose coupling

## 📚 API Documentation - Unified API

All endpoints are available at `http://localhost:8000` when running the unified API.

### Backtest Endpoints

#### Run Backtest (POST) - Recommended
```bash
POST http://localhost:8000/api/backtest
{
  "from_date": "2025-07-01",
  "to_date": "2025-07-31",
  "signals_to_test": ["S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8"],
  "lot_size": 75,
  "lots_to_trade": 10,
  "initial_capital": 500000,
  "use_hedging": true,
  "hedge_offset": 200,
  "commission_per_lot": 40
}
```

#### Run Backtest (GET) - Quick Testing
```bash
GET http://localhost:8000/api/backtest?from_date=2025-07-01&to_date=2025-07-31&signals_to_test=S1,S2,S3
```

### Data Collection Endpoints

#### Collect NIFTY Data
```bash
POST http://localhost:8000/api/collect/nifty
{
  "from_date": "2025-01-01",
  "to_date": "2025-01-31",
  "symbol": "NIFTY",
  "force_refresh": false
}
```

#### Collect Options Data
```bash
POST http://localhost:8000/api/collect/options
{
  "from_date": "2025-01-01", 
  "to_date": "2025-01-31",
  "symbol": "NIFTY",
  "strike_range": 500,
  "use_optimization": true
}
```

### Analysis Endpoints

#### Check Data Availability
```bash
GET http://localhost:8000/api/data/check?from_date=2025-07-01&to_date=2025-07-31&symbol=NIFTY
```

#### Get Available Signals
```bash
GET http://localhost:8000/api/signals/available
```

## 🧪 Testing

### Test Backtest System
```python
# Test all 8 signals for a specific period
python backtest_api_post.py
# Navigate to http://localhost:8002/docs
# Use the /backtest endpoint with desired parameters
```

### Test Data Collection
```python
# Start the data collection API
python -m api.data_collection_api
# Navigate to http://localhost:8002/docs
# Use /collect/nifty or /collect/options endpoints
```

## 📊 Trading Signals

- **S1** - Bear Trap (Bullish) - Sell PUT
- **S2** - Support Hold (Bullish) - Sell PUT
- **S3** - Resistance Hold (Bearish) - Sell CALL
- **S4** - Bias Failure Bull (Bullish) - Sell PUT
- **S5** - Bias Failure Bear (Bearish) - Sell CALL
- **S6** - Weakness Confirmed (Bearish) - Sell CALL
- **S7** - Breakout Confirmed (Bullish) - Sell PUT
- **S8** - Breakdown Confirmed (Bearish) - Sell CALL

## 🔧 Configuration

### Database Settings
Configure in `src/config/settings.py` or via environment variables:
```python
DB_SERVER = os.getenv('DB_SERVER', '(localdb)\\mssqllocaldb')
DB_NAME = os.getenv('DB_NAME', 'KiteConnectApi')
```

### API Settings
```python
# Port configuration
MAIN_API_PORT = 8000
DATA_COLLECTION_PORT = 8002
BACKTEST_GET_PORT = 8001
```

## 📈 Performance Optimizations

- **Parallel Processing**: Multi-threaded data collection
- **Smart Caching**: Reduces redundant API calls
- **Bulk Operations**: Efficient database inserts
- **Connection Pooling**: Optimized database connections

## 🐛 Troubleshooting

1. **API Key Issues**: Ensure Breeze API credentials are correct in `.env`
2. **Database Connection**: Check SQL Server is running and accessible
3. **Port Conflicts**: Ensure ports 8000, 8001, 8002 are available
4. **Missing Data**: Run data collection before backtesting

## 📞 Support

For detailed information:
- See `BACKTEST_CAPABILITIES.md` for backtest features
- Check `docs/` folder for additional documentation
- Review API documentation at `/docs` endpoints