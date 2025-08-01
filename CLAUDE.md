# BreezeConnect Trading System - Project Guidelines

## Project Overview
This is a trading system using Breeze API for Indian markets (NIFTY options trading) with backtesting capabilities.

## Critical Rules - DO NOT VIOLATE
1. **NEVER change working code without explicit permission**
2. **NEVER modify default parameters in APIs**
3. **NEVER add "improvements" unless specifically requested**
4. **NEVER change response formats of working endpoints**
5. **ALWAYS test with the exact same parameters that were working before**

## Code Style Guidelines
- **NO unnecessary comments** - The code should be self-documenting
- **NO commented-out code** - Remove it completely
- **NO emoji in code** unless explicitly requested
- **MINIMAL output** - Be concise and direct in responses

## Working API Patterns
When asked to modify or consolidate APIs:
1. Copy the working code EXACTLY
2. Change ONLY what is explicitly requested (e.g., port number)
3. Keep all defaults, parameters, and behavior identical
4. Test with the same test case that was working

## Database Context
- SQL Server with existing tables: BacktestRuns, BacktestTrades, BacktestPositions
- NIFTY options data stored at 5-minute intervals
- Hourly data aggregated from 5-minute data
- Weekly options expire on Thursday

## Common Commands
```bash
# Start unified API
python unified_api_correct.py

# Run backtest for July 2025
curl -X POST http://localhost:8000/backtest \
  -H "Content-Type: application/json" \
  -d '{"from_date": "2025-07-14", "to_date": "2025-07-18", "signals_to_test": ["S1"]}'
```

## Testing Context
- **Known working test**: July 14-18, 2025 with signal S1 produces 1 trade
- **Entry time**: Second candle after signal (11:15 AM)
- **Stop loss**: Main strike price (e.g., 25000)
- **Default lot size**: 75
- **Default lots to trade**: 10

## Architecture
```
src/
   domain/           # Business logic (signals, trades)
   application/      # Use cases (RunBacktestUseCase)
   infrastructure/   # External services (Breeze, Database)
   api/             # FastAPI endpoints

api/
   unified_api_correct.py  # Combined API (port 8000)
   data_collection_api.py  # NIFTY/options collection
   optimizations/          # Performance modules
```

## DO NOT Touch List
1. Stop loss calculation logic (uses main strike as stop loss)
2. Entry time calculation (second candle after signal)
3. Working database queries
4. Signal evaluation logic
5. Option pricing service

## When Creating/Modifying APIs
1. Start with the simplest working version
2. Do not add features unless requested
3. Keep exact same defaults as working versions
4. Use the same time handling (9:00 to 16:00 for backtests)
5. Preserve exact response formats
6. **ALWAYS scan entire source file for ALL endpoints** using: `@app.post|@app.get|@app.delete|@app.put`
7. **List all found endpoints before consolidating** to ensure nothing is missed
8. **Compare endpoint count** - if original has 10 endpoints, consolidated must have 10

## Debugging Approach
1. If something was working before, check what changed
2. Compare with working backup versions
3. Use exact same test parameters
4. Don't assume - verify with actual code
5. **ALWAYS show code when explaining behavior** - Never make claims without showing the exact code
6. **Trace through calculations with actual values** - Show step-by-step execution
7. **No assumptions** - If unsure, read the code first before responding

## Project-Specific Context
- Trading 8 signals (S1-S8) for NIFTY options
- Option selling strategy with hedging
- Stop loss = main strike price
- Commission = Rs. 40 per lot
- Initial capital = Rs. 500,000
- All signals selected by default in APIs

## Remember
"Just consolidate working APIs" means:
- Copy them exactly
- Put them in one file
- Change only the port
- Nothing else

## Subagent Usage Guidelines
Use subagents for:
1. **Complex searches**: "Find all signal evaluation logic" → Use general-purpose agent
2. **Testing tasks**: "Create tests for new features" → Use testing-automation agent
3. **Architecture decisions**: "Design new trading features" → Use trading-architect agent
4. **Performance optimization**: "Optimize data processing" → Use python-finance-expert agent
5. **Parallel exploration**: Use multiple agents to explore different parts simultaneously

Example prompts that trigger subagent use:
- "Use subagents to find all places where options are priced"
- "Delegate testing of the backtest API to appropriate agent"
- "Use parallel tasks to check data in all tables"