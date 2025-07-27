"""
Directory cleanup script for breezepython
"""
import os
import shutil
from datetime import datetime
from pathlib import Path

# Setup paths
ROOT_DIR = Path("C:/Users/E1791/Kitepy/breezepython")
BACKUP_DIR = ROOT_DIR / "cleanup_backup_20250127"
LOG_FILE = BACKUP_DIR / "cleanup_log.txt"

# Categories of files to move
PATTERNS_TO_MOVE = {
    "test_files": [
        "test_*.py",
        # Exclude our main API
        "!test_direct_endpoint_simple.py"
    ],
    "debug_files": [
        "debug_*.py",
        "check_*.py",
        "verify_*.py",
        "analyze_*.py"
    ],
    "collection_scripts": [
        "collect_*.py",
        "fetch_*.py",
        "store_*.py",
        "refetch_*.py"
    ],
    "old_api_files": [
        "app.py",
        "app_simple.py",
        "main_api.py",
        "main.py",
        "run.py"
    ],
    "temp_files": [
        "*.json",
        "nul",
        "2025-07-27-this-session-is-being-continued-from-a-previous-co.txt"
    ],
    "duplicate_services": [
        "breeze_service_*.py",
        "test_direct_endpoint_backup.py",
        "test_direct_endpoint_simple_backup.py"
    ],
    "documentation_extras": [
        "ACTUAL_SWAGGER_ENDPOINTS.md",
        "API_RESTART_REQUIRED.md",
        "DIRECT_API_USAGE.md",
        "IMPLEMENTATION_SUMMARY.md",
        "QUICK_START_GUIDE.md",
        "README_ARCHITECTURE.md",
        "RUNNING_GUIDE.md",
        "SIGNAL_TESTING_GUIDE.md",
        "SWAGGER_ENDPOINTS_GUIDE.md",
        "TECHNICAL_SPEC.md",
        "TEST_UPDATED_ENDPOINT.md",
        "WORKING_SOLUTION.md",
        "weekly_signals_api_guide.md"
    ],
    "batch_files": [
        "*.bat",
        "setup.ps1"
    ],
    "misc_files": [
        "compare_*.py",
        "diagnose_*.py",
        "delete_*.py",
        "update_*.py",
        "quick_*.py",
        "final_*.py",
        "comprehensive_*.py"
    ]
}

# Files to definitely keep
KEEP_FILES = {
    "test_direct_endpoint_simple.py",  # Main API
    "enhanced_optimizations.py",
    "nifty_optimizations.py",
    "enhanced_collection_logic.py",
    "apply_db_indexes_sqlserver.py",
    "ultimate_speed_config.py",
    "ultra_fast_async.py",
    "requirements.txt",
    ".env",
    "README.md",
    "OPTIMIZATION_SUMMARY.md",
    "kite_trading.db"
}

def setup_logging():
    """Setup logging for the cleanup process"""
    with open(LOG_FILE, 'w') as f:
        f.write(f"Cleanup started at {datetime.now()}\n")
        f.write("=" * 60 + "\n\n")

def log_action(action, file_path):
    """Log each file movement"""
    with open(LOG_FILE, 'a') as f:
        f.write(f"{action}: {file_path}\n")

def should_move_file(file_path: Path, patterns: list) -> bool:
    """Check if file matches any pattern and should be moved"""
    file_name = file_path.name
    
    # Check if it's in keep list
    if file_name in KEEP_FILES:
        return False
    
    for pattern in patterns:
        if pattern.startswith("!"):
            # Exclusion pattern
            continue
        
        # Convert glob pattern to check
        if pattern.endswith("*.py"):
            prefix = pattern[:-4]
            if file_name.startswith(prefix) and file_name.endswith(".py"):
                return True
        elif "*" in pattern:
            # Handle other glob patterns
            if file_path.match(pattern):
                return True
        elif file_name == pattern:
            return True
    
    return False

def move_files():
    """Move files to backup directory"""
    moved_count = 0
    
    for category, patterns in PATTERNS_TO_MOVE.items():
        category_dir = BACKUP_DIR / category
        category_dir.mkdir(exist_ok=True)
        
        print(f"\nProcessing {category}...")
        
        for file_path in ROOT_DIR.iterdir():
            if file_path.is_file() and should_move_file(file_path, patterns):
                try:
                    dest_path = category_dir / file_path.name
                    shutil.move(str(file_path), str(dest_path))
                    log_action(f"MOVED to {category}", file_path.name)
                    moved_count += 1
                    print(f"  Moved: {file_path.name}")
                except Exception as e:
                    log_action(f"ERROR moving", f"{file_path.name}: {e}")
                    print(f"  Error moving {file_path.name}: {e}")
    
    return moved_count

def create_new_structure():
    """Create the new organized directory structure"""
    # Create new directories
    directories = [
        "api",
        "api/optimizations",
        "docs",
        "scripts",
        "config"
    ]
    
    for dir_name in directories:
        dir_path = ROOT_DIR / dir_name
        dir_path.mkdir(exist_ok=True)
        print(f"Created directory: {dir_name}")

def reorganize_files():
    """Move remaining files to appropriate directories"""
    moves = {
        "test_direct_endpoint_simple.py": "api/test_direct_endpoint_simple.py",
        "enhanced_optimizations.py": "api/optimizations/enhanced_optimizations.py",
        "nifty_optimizations.py": "api/optimizations/nifty_optimizations.py",
        "enhanced_collection_logic.py": "api/optimizations/enhanced_collection_logic.py",
        "ultimate_speed_config.py": "api/optimizations/ultimate_speed_config.py",
        "ultra_fast_async.py": "api/optimizations/ultra_fast_async.py",
        "apply_db_indexes_sqlserver.py": "scripts/apply_db_indexes_sqlserver.py",
        "OPTIMIZATION_SUMMARY.md": "docs/OPTIMIZATION_SUMMARY.md",
        ".env": "config/.env",
        # Additional optimization files
        "db_pool_optimization.py": "api/optimizations/db_pool_optimization.py",
        "multiprocessing_optimization.py": "api/optimizations/multiprocessing_optimization.py",
        "advanced_caching.py": "api/optimizations/advanced_caching.py",
        "optimize_db_indexes.py": "scripts/optimize_db_indexes.py",
        "optimized_options_bulk.py": "api/optimizations/optimized_options_bulk.py",
        # Other remaining files
        "apply_db_indexes.py": "scripts/apply_db_indexes.py",
        "fix_missing_thursday_data.py": "scripts/fix_missing_thursday_data.py",
        "generate_breeze_session.py": "scripts/generate_breeze_session.py",
        "session_status.py": "scripts/session_status.py",
        "start_api_fresh.py": "scripts/start_api_fresh.py",
        # Documentation
        "july_2025_analysis_summary.md": "docs/july_2025_analysis_summary.md",
        "july_2025_corrected_analysis.md": "docs/july_2025_corrected_analysis.md",
        "COMPLETE_API_GUIDE.md": "docs/COMPLETE_API_GUIDE.md"
    }
    
    print("\nReorganizing files...")
    for src, dest in moves.items():
        src_path = ROOT_DIR / src
        dest_path = ROOT_DIR / dest
        
        if src_path.exists():
            try:
                # Create parent directory if needed
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src_path), str(dest_path))
                print(f"  Moved {src} -> {dest}")
                log_action("REORGANIZED", f"{src} -> {dest}")
            except Exception as e:
                print(f"  Error moving {src}: {e}")

def main():
    """Main cleanup process"""
    print("Starting breezepython cleanup...")
    print("=" * 60)
    
    # Setup
    setup_logging()
    
    # Count initial files
    initial_files = len([f for f in ROOT_DIR.iterdir() if f.is_file()])
    print(f"Initial file count: {initial_files}")
    
    # Move files to backup
    moved_count = move_files()
    print(f"\nMoved {moved_count} files to backup")
    
    # Create new structure
    print("\nCreating new directory structure...")
    create_new_structure()
    
    # Reorganize remaining files
    reorganize_files()
    
    # Count final files
    final_files = len([f for f in ROOT_DIR.iterdir() if f.is_file()])
    print(f"\nFinal file count in root: {final_files}")
    
    # Summary
    with open(LOG_FILE, 'a') as f:
        f.write(f"\n\nCleanup completed at {datetime.now()}\n")
        f.write(f"Files moved: {moved_count}\n")
        f.write(f"Initial files: {initial_files}\n")
        f.write(f"Final files in root: {final_files}\n")
    
    print("\n" + "=" * 60)
    print(f"Cleanup complete! Check {LOG_FILE} for details.")
    print(f"Backup created at: {BACKUP_DIR}")

if __name__ == "__main__":
    main()