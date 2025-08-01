"""Check week start dates"""
from datetime import datetime, timedelta
from src.domain.services.weekly_context_manager import WeeklyContextManager

context_manager = WeeklyContextManager()

# Check week starts for key dates
dates = [
    datetime(2025, 1, 13),  # Monday Jan 13
    datetime(2025, 1, 27),  # Monday Jan 27
    datetime(2025, 7, 14),  # Monday Jul 14
]

for date in dates:
    week_start = context_manager.get_week_start(date)
    print(f"{date.strftime('%A %Y-%m-%d')} -> Week starts: {week_start.strftime('%A %Y-%m-%d %H:%M')}")
    
    # Calculate days between
    days_diff = (date - week_start).days
    print(f"  Days from week start: {days_diff}")
    
    # Check what day of week the week_start is
    print(f"  Week start day: {week_start.weekday()} (0=Mon, 6=Sun)")
    print()