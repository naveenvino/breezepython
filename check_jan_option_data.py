"""Check option data availability for January 2025"""
import asyncio
from datetime import datetime
from sqlalchemy import text
from src.infrastructure.database.database_manager import get_db_manager
from src.infrastructure.database.models import OptionsHistoricalData

async def check_january_data():
    """Check what option data we have for January"""
    db = get_db_manager()
    
    with db.get_session() as session:
        # Check raw SQL for January 16 expiry (from SP data)
        print("Checking January 16 expiry data...")
        
        # Direct SQL query
        query = text("""
            SELECT COUNT(*) as count, 
                   MIN(Timestamp) as min_time, 
                   MAX(Timestamp) as max_time,
                   COUNT(DISTINCT Strike) as unique_strikes
            FROM OptionsHistoricalData
            WHERE ExpiryDate >= '2025-01-16' AND ExpiryDate < '2025-01-17'
        """)
        
        result = session.execute(query).first()
        print(f"Raw SQL - Records: {result.count}, Strikes: {result.unique_strikes}")
        print(f"Date range: {result.min_time} to {result.max_time}")
        
        # Check with different time formats
        print("\nChecking different expiry time formats:")
        
        # Check 00:00:00
        count1 = session.query(OptionsHistoricalData).filter(
            OptionsHistoricalData.expiry_date == datetime(2025, 1, 16, 0, 0, 0)
        ).count()
        print(f"Expiry at 00:00:00: {count1} records")
        
        # Check 05:30:00
        count2 = session.query(OptionsHistoricalData).filter(
            OptionsHistoricalData.expiry_date == datetime(2025, 1, 16, 5, 30, 0)
        ).count()
        print(f"Expiry at 05:30:00: {count2} records")
        
        # Check 15:30:00
        count3 = session.query(OptionsHistoricalData).filter(
            OptionsHistoricalData.expiry_date == datetime(2025, 1, 16, 15, 30, 0)
        ).count()
        print(f"Expiry at 15:30:00: {count3} records")
        
        # Get actual expiry times in DB
        query2 = text("""
            SELECT DISTINCT ExpiryDate, COUNT(*) as count
            FROM OptionsHistoricalData
            WHERE ExpiryDate >= '2025-01-01' AND ExpiryDate < '2025-02-01'
            GROUP BY ExpiryDate
            ORDER BY ExpiryDate
        """)
        
        print("\nActual expiry dates in January:")
        results = session.execute(query2).fetchall()
        for row in results:
            print(f"  {row.ExpiryDate}: {row.count} records")
        
        # Check specific strike from SP data (23300)
        print("\nChecking strike 23300 data:")
        query3 = text("""
            SELECT COUNT(*) as count, 
                   MIN(Timestamp) as min_time,
                   MAX(Timestamp) as max_time
            FROM OptionsHistoricalData
            WHERE Strike = 23300 
            AND Timestamp >= '2025-01-13' 
            AND Timestamp < '2025-01-17'
        """)
        
        result3 = session.execute(query3).first()
        print(f"Strike 23300 in Jan 13-16: {result3.count} records")
        print(f"Time range: {result3.min_time} to {result3.max_time}")

if __name__ == "__main__":
    asyncio.run(check_january_data())