# KiteApp Python - Clean Architecture Trading Platform

A comprehensive trading platform built with Clean Architecture principles, integrating Breeze API for automated trading, backtesting, and market analysis.

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

### 3. Run the Application
```bash
python run.py
```

The API will be available at `http://localhost:8100`

### 4. View API Documentation
- Swagger UI: `http://localhost:8100/docs`
- ReDoc: `http://localhost:8100/redoc`

### 5. Test the API
```bash
python test_api.py
```

## 📁 Project Structure

```
src/
├── domain/           # Core business logic (entities, value objects, interfaces)
├── application/      # Use cases and application services
├── infrastructure/   # External services, database, API implementations
└── api/             # FastAPI routes and HTTP layer
```

## 🧪 Testing Examples

### Run API Tests
```bash
# Test all endpoints
python test_api.py

# Direct usage examples (without API)
python example_direct_usage.py
```

### API Examples

#### Collect Weekly Options Data
```bash
curl -X POST "http://localhost:8100/api/v2/data/collect/weekly" \
  -H "Content-Type: application/json" \
  -d '{
    "from_date": "2024-01-01",
    "to_date": "2024-01-31",
    "symbol": "NIFTY",
    "strike_range": 500
  }'
```

#### Run Backtest
```bash
curl -X POST "http://localhost:8100/api/v2/backtest/run" \
  -H "Content-Type: application/json" \
  -d '{
    "strategy_name": "WeeklySignals",
    "from_date": "2024-01-01",
    "to_date": "2024-01-31",
    "initial_capital": 100000,
    "symbol": "NIFTY"
  }'
```

#### Calculate Position Size
```bash
curl -X POST "http://localhost:8100/api/v2/trading/risk/position-size" \
  -H "Content-Type: application/json" \
  -d '{
    "capital": 100000,
    "risk_percentage": 2,
    "entry_price": 23500,
    "stop_loss": 23300,
    "lot_size": 50
  }'
```

## 📚 Documentation

- **Quick Start Guide**: See `QUICK_START_GUIDE.md` for detailed setup and usage
- **Architecture**: See `README_ARCHITECTURE.md` for clean architecture details
- **API Documentation**: Available at `/docs` when the server is running

## 🛠️ Key Features

### Clean Architecture
- **Domain Layer**: Pure business logic with no external dependencies
- **Application Layer**: Use cases orchestrating business operations
- **Infrastructure Layer**: All external concerns (database, APIs, etc.)
- **Dependency Injection**: Loose coupling and testability

### Trading Features
- **Data Collection**: Automated NIFTY and options data collection
- **Backtesting**: Strategy backtesting with comprehensive metrics
- **Risk Management**: Position sizing, Kelly Criterion, portfolio analysis
- **Option Pricing**: Black-Scholes pricing and Greeks calculation
- **Signal Processing**: Weekly trading signals evaluation

### API Endpoints
- `/api/v2/data/` - Data collection operations
- `/api/v2/analysis/` - Market analysis and insights
- `/api/v2/trading/` - Trading operations and risk management
- `/api/v2/backtest/` - Strategy backtesting

## 🔧 Development

### Run Tests
```bash
pytest tests/
```

### Add New Features
1. Domain logic goes in `src/domain/`
2. Use cases go in `src/application/use_cases/`
3. External integrations go in `src/infrastructure/`
4. API routes go in `src/api/routers/`

### Debug Mode
Set in `.env`:
```env
APP_DEBUG=true
LOG_LEVEL=DEBUG
```

## 📞 Support

For issues or questions:
1. Check `QUICK_START_GUIDE.md` for common issues
2. Review logs in `logs/` directory
3. Check API documentation at `/docs`

## 🏗️ Architecture Benefits

- **Testable**: Each layer can be tested independently
- **Maintainable**: Clear separation of concerns
- **Scalable**: Easy to add new features
- **Flexible**: Swap implementations without changing business logic
- **Framework Independent**: Core logic doesn't depend on FastAPI