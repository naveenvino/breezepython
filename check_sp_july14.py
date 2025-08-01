"""Check SP execution for July 14"""
from sqlalchemy import text
from src.infrastructure.database.database_manager import get_db_manager

db = get_db_manager()

with db.get_session() as session:
    # Check SP results for July 14
    query = text("""
        SELECT 
            SignalCandle,
            SignalBar,
            SignalType,
            EntryPrice,
            StopLoss,
            EntryTime,
            Direction
        FROM BacktestTrades
        WHERE CAST(SignalCandle as date) = '2025-07-14'
        ORDER BY SignalCandle
    """)
    
    results = session.execute(query).fetchall()
    
    print("SP Trades on July 14:")
    for row in results:
        print(f"\nSignal: {row.SignalType}")
        print(f"Signal Candle: {row.SignalCandle}")
        print(f"Signal Bar: {row.SignalBar}")
        print(f"Entry Time: {row.EntryTime}")
        print(f"Entry Price: {row.EntryPrice}")
        print(f"Stop Loss: {row.StopLoss}")
        print(f"Direction: {row.Direction}")
    
    # Check NIFTY data that SP is using
    print("\n\nChecking NIFTY data for July 14:")
    query2 = text("""
        SELECT TOP 3
            Timestamp,
            [Open],
            High,
            Low,
            [Close]
        FROM NiftyIndexDataHourly
        WHERE CAST(Timestamp as date) = '2025-07-14'
        ORDER BY Timestamp
    """)
    
    nifty_data = session.execute(query2).fetchall()
    
    for row in nifty_data:
        print(f"\n{row.Timestamp}: O={row.Open}, H={row.High}, L={row.Low}, C={row.Close}")
    
    # Check what the SP calculated for zones
    print("\n\nChecking SP zone calculations:")
    query3 = text("""
        EXEC GetBacktestSignals 
            @FromDate = '2025-07-14', 
            @ToDate = '2025-07-14',
            @SignalsToTest = 'S1'
    """)
    
    try:
        sp_results = session.execute(query3).fetchall()
        if sp_results:
            print(f"SP found {len(sp_results)} signals on July 14")
            for row in sp_results[:5]:  # Show first 5 columns
                print(row)
    except Exception as e:
        print(f"Could not execute SP: {e}")