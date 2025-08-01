"""Debug why S1 is not matching on July 14"""
from datetime import datetime
from sqlalchemy import text
from src.infrastructure.database.database_manager import get_db_manager

# From the user's SP results:
# 2025-07-14 11:15:00  |  1  |  S1  |  25048.5  |  -71.95  |  25000
# This shows S1 triggered with stop loss at 25000

# Let's check what values SP is using vs Python

db = get_db_manager()
with db.get_session() as session:
    # Check NIFTY data around July 14
    query = text("""
        SELECT 
            Timestamp,
            [Open],
            High,
            Low,
            [Close]
        FROM NiftyIndexDataHourly
        WHERE Timestamp >= '2025-07-13 09:00' 
        AND Timestamp <= '2025-07-14 12:00'
        ORDER BY Timestamp
    """)
    
    results = session.execute(query).fetchall()
    
    print("NIFTY Data July 13-14:")
    print("="*80)
    
    prev_week_high = None
    prev_week_low = None
    
    for row in results:
        print(f"{row.Timestamp}: O={row.Open:,.2f}, H={row.High:,.2f}, L={row.Low:,.2f}, C={row.Close:,.2f}")
        
        # Track July 13 (Sunday) data for previous week high/low
        if row.Timestamp.date() == datetime(2025, 7, 13).date():
            if prev_week_high is None or row.High > prev_week_high:
                prev_week_high = row.High
            if prev_week_low is None or row.Low < prev_week_low:
                prev_week_low = row.Low
    
    print(f"\nPrevious week (July 13) High: {prev_week_high:,.2f}")
    print(f"Previous week (July 13) Low: {prev_week_low:,.2f}")
    
    # Calculate zones like SP might be doing
    if prev_week_high and prev_week_low:
        # SP might be using different zone calculation
        zone_range = (prev_week_high - prev_week_low) * 0.025  # 2.5% of range
        
        support_bottom = prev_week_low
        support_top = prev_week_low + zone_range
        resistance_bottom = prev_week_high - zone_range  
        resistance_top = prev_week_high
        
        print(f"\nCalculated Zones (2.5% method):")
        print(f"Support Zone: {support_bottom:,.2f} - {support_top:,.2f}")
        print(f"Resistance Zone: {resistance_bottom:,.2f} - {resistance_top:,.2f}")
        
        # Check S1 conditions with this calculation
        print("\n\nChecking S1 conditions on July 14 10:15:")
        print("First bar (09:15): From data above")
        print("Second bar (10:15): From data above")
        
        # From the debug output, we know:
        # First bar: O=25151.10, C=25119.80, L=25041.90
        # Second bar: C=25063.95
        
        print(f"\nS1 Conditions with Python zones (support bottom = 25340.45):")
        print(f"1. First bar open (25151.10) >= Support bottom (25340.45): False ❌")
        print(f"2. First bar close (25119.80) < Support bottom (25340.45): True ✓")
        print(f"3. Current close (25063.95) > First bar low (25041.90): True ✓")
        
        print(f"\nBUT if SP is using previous week LOW as support (simpler approach):")
        print(f"Support = Previous week low = {prev_week_low:,.2f}")
        print(f"\nS1 Conditions with this approach:")
        print(f"1. First bar open (25151.10) >= Support ({prev_week_low:,.2f}): {25151.10 >= prev_week_low}")
        print(f"2. First bar close (25119.80) < Support ({prev_week_low:,.2f}): {25119.80 < prev_week_low}")
        print(f"3. Current close (25063.95) > First bar low (25041.90): True")
    
    # SP shows stop loss at 25000, which is a round number
    # This suggests SP might be using round(first_bar.low - abs(first_bar.open - first_bar.close))
    print("\n\nStop Loss Calculation:")
    print("First bar: Open=25151.10, Close=25119.80, Low=25041.90")
    print(f"Range = |Open - Close| = |25151.10 - 25119.80| = 31.30")
    print(f"Stop = Low - Range = 25041.90 - 31.30 = 25010.60")
    print(f"SP Stop Loss = 25000 (rounded down to nearest 100?)")