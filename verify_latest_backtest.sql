-- Check latest backtest results
SELECT TOP 10
    bt.signal,
    bt.entry_time,
    bt.exit_time,
    bt.entry_price,
    bt.exit_price,
    bt.pnl,
    bt.exit_reason,
    bt.main_strike,
    bt.hedge_strike
FROM BacktestTrades bt
ORDER BY bt.entry_time DESC;

-- Summary by signal
SELECT 
    signal,
    COUNT(*) as total_trades,
    SUM(pnl) as total_pnl,
    AVG(pnl) as avg_pnl,
    MIN(entry_time) as first_trade,
    MAX(exit_time) as last_trade
FROM BacktestTrades
WHERE entry_time >= '2025-07-01'
GROUP BY signal
ORDER BY signal;