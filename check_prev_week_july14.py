"""Check previous week data for July 14 signal"""
from datetime import datetime, timedelta
from sqlalchemy import text
from src.infrastructure.database.database_manager import get_db_manager
from src.domain.services.weekly_context_manager import WeeklyContextManager

db = get_db_manager()
context_manager = WeeklyContextManager()

# July 14 is Monday, so previous week is July 7-11 (Mon-Fri)
# But with Sunday week start, previous week would be June 30 - July 6

with db.get_session() as session:
    # Get week start for July 14
    july14 = datetime(2025, 7, 14, 10, 15)
    week_start = context_manager.get_week_start(july14)
    print(f"July 14 week starts: {week_start}")
    
    # Previous week would be 7 days before
    prev_week_start = week_start - timedelta(days=7)
    prev_week_end = week_start - timedelta(seconds=1)
    
    print(f"Previous week: {prev_week_start} to {prev_week_end}")
    
    # Get previous week data
    query = text("""
        SELECT 
            MIN([Low]) as WeekLow,
            MAX([High]) as WeekHigh,
            MIN(Timestamp) as FirstTime,
            MAX(Timestamp) as LastTime
        FROM NiftyIndexDataHourly
        WHERE Timestamp >= :start
        AND Timestamp <= :end
    """)
    
    result = session.execute(query, {
        'start': prev_week_start,
        'end': prev_week_end
    }).first()
    
    if result and result.WeekLow:
        print(f"\nPrevious week data found:")
        print(f"Week High: {result.WeekHigh:,.2f}")
        print(f"Week Low: {result.WeekLow:,.2f}")
        print(f"Data from: {result.FirstTime} to {result.LastTime}")
        
        # Calculate zones
        range_val = float(result.WeekHigh - result.WeekLow)
        zone_size = range_val * 0.025
        
        support_bottom = float(result.WeekLow)
        support_top = float(result.WeekLow) + zone_size
        
        print(f"\nZone Calculation:")
        print(f"Range: {range_val:.2f}")
        print(f"Zone size (2.5%): {zone_size:.2f}")
        print(f"Support Zone: {support_bottom:,.2f} - {support_top:,.2f}")
        
        # Now check S1 conditions with correct support
        print(f"\nS1 Conditions check:")
        print(f"1. First bar open (25151.10) >= Support bottom ({support_bottom:,.2f}): {25151.10 >= support_bottom}")
        print(f"2. First bar close (25119.80) < Support bottom ({support_bottom:,.2f}): {25119.80 < support_bottom}")
        print(f"3. Second bar close (25063.95) > First bar low (25041.90): {25063.95 > 25041.90}")
        
        all_conditions = (25151.10 >= support_bottom) and (25119.80 < support_bottom) and (25063.95 > 25041.90)
        print(f"\nAll S1 conditions met: {all_conditions}")
    else:
        print("No previous week data found!")
        
    # Also check what data exists around that time
    print("\n\nChecking available data around July 7-11:")
    query2 = text("""
        SELECT 
            CAST(Timestamp as date) as Date,
            COUNT(*) as Records,
            MIN([Low]) as DayLow,
            MAX([High]) as DayHigh
        FROM NiftyIndexDataHourly
        WHERE Timestamp >= '2025-07-07'
        AND Timestamp <= '2025-07-11'
        GROUP BY CAST(Timestamp as date)
        ORDER BY CAST(Timestamp as date)
    """)
    
    daily_data = session.execute(query2).fetchall()
    for row in daily_data:
        print(f"{row.Date}: {row.Records} records, Low={row.DayLow:,.2f}, High={row.DayHigh:,.2f}")