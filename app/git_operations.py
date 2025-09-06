# app/git_operations.py

import os
import subprocess
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Tuple

from app.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# Path to project root (where CSV files are located)
PROJECT_ROOT = Path(__file__).parent.parent


def run_git_command(command: list, cwd: Optional[Path] = None) -> Tuple[bool, str]:
    """
    Run a git command and return (success, output/error)
    """
    try:
        cwd = cwd or PROJECT_ROOT
        result = subprocess.run(
            command,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            logger.error("Git command failed: %s", result.stderr)
            return False, result.stderr.strip()
            
    except subprocess.TimeoutExpired:
        logger.error("Git command timed out: %s", ' '.join(command))
        return False, "Command timed out"
    except Exception as e:
        logger.error("Git command error: %s", e)
        return False, str(e)


def setup_git_auth():
    """
    Configure git to use the GitHub token for authentication
    """
    token = settings.GITHUB_TOKEN
    username = settings.GITHUB_USERNAME
    
    if not token:
        logger.error("GITHUB_TOKEN not found in environment")
        return False
        
    if not username:
        logger.error("GITHUB_USERNAME not found in environment")
        return False
    
    # Set up git credential helper to use the token
    repo_url = f"https://{username}:{token}@github.com/{username}/my-horse-scraper.git"
    
    # Update the remote URL to include the token
    success, output = run_git_command(["git", "remote", "set-url", "origin", repo_url])
    if success:
        logger.info("Git authentication configured successfully")
        return True
    else:
        logger.error("Failed to configure git authentication: %s", output)
        return False


def check_csv_changes() -> Tuple[bool, int]:
    """
    Check if CSV files have changes since last commit
    Returns (has_changes, num_changed_files)
    """
    # Check git status for CSV files specifically
    success, output = run_git_command(["git", "status", "--porcelain", "*.csv"])
    
    if not success:
        logger.error("Failed to check git status: %s", output)
        return False, 0
    
    if not output:
        logger.info("No CSV file changes detected")
        return False, 0
    
    # Count changed files
    changed_files = [line for line in output.split('\n') if line.strip()]
    logger.info("Found %d changed CSV files", len(changed_files))
    for file_line in changed_files:
        logger.info("  %s", file_line)
    
    return True, len(changed_files)


def get_daily_stats(target_date: date) -> Tuple[int, int]:
    """
    Get statistics for races and runners from CSV files for a specific date
    Returns (num_races, num_runners)
    """
    runners_csv = PROJECT_ROOT / "race_runners_log.csv"
    
    if not runners_csv.exists():
        return 0, 0
    
    try:
        import csv
        races_seen = set()
        runner_count = 0
        
        with open(runners_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Try to extract date from race_id or timestamp
                race_id = row.get('Race_ID', '')
                if race_id:
                    races_seen.add(race_id)
                    runner_count += 1
        
        return len(races_seen), runner_count
        
    except Exception as e:
        logger.error("Failed to get daily stats: %s", e)
        return 0, 0


def commit_daily_data(target_date: Optional[date] = None) -> bool:
    """
    Commit CSV files with a meaningful message for the given date
    """
    if target_date is None:
        # Default to yesterday (since we run at 00:00 UTC)
        target_date = date.today() - timedelta(days=1)
    
    # Check if there are changes to commit
    has_changes, num_files = check_csv_changes()
    if not has_changes:
        logger.info("No CSV changes to commit for %s", target_date)
        return True  # Not an error, just nothing to do
    
    # Get statistics for the commit message
    num_races, num_runners = get_daily_stats(target_date)
    
    # Stage CSV files
    success, output = run_git_command(["git", "add", "*.csv"])
    if not success:
        logger.error("Failed to stage CSV files: %s", output)
        return False
    
    # Create commit message
    date_str = target_date.strftime("%Y-%m-%d")
    if num_races > 0:
        commit_msg = f"Race data: {date_str} ({num_races} races, {num_runners} runners)"
    else:
        commit_msg = f"Race data: {date_str} ({num_files} files updated)"
    
    # Commit the changes
    success, output = run_git_command(["git", "commit", "-m", commit_msg])
    if not success:
        logger.error("Failed to commit changes: %s", output)
        return False
    
    logger.info("Successfully committed: %s", commit_msg)
    return True


def push_to_github() -> bool:
    """
    Push committed changes to GitHub
    """
    # Ensure git auth is set up
    if not setup_git_auth():
        return False
    
    # Push to main branch
    success, output = run_git_command(["git", "push", "origin", "main"])
    if not success:
        logger.error("Failed to push to GitHub: %s", output)
        return False
    
    logger.info("Successfully pushed to GitHub")
    return True


def daily_git_commit() -> dict:
    """
    Main function for daily git operations
    Returns status dict for logging
    """
    result = {
        "status": "error",
        "message": "",
        "files_committed": 0,
        "races": 0,
        "runners": 0
    }
    
    try:
        # Check if git auto-commit is enabled
        if not settings.GIT_AUTO_COMMIT:
            result["status"] = "disabled"
            result["message"] = "Git auto-commit is disabled"
            return result
        
        target_date = date.today() - timedelta(days=1)
        
        # Check for changes
        has_changes, num_files = check_csv_changes()
        if not has_changes:
            result["status"] = "ok"
            result["message"] = f"No changes to commit for {target_date}"
            return result
        
        # Get stats
        num_races, num_runners = get_daily_stats(target_date)
        result["files_committed"] = num_files
        result["races"] = num_races
        result["runners"] = num_runners
        
        # Commit changes
        if not commit_daily_data(target_date):
            result["message"] = "Failed to commit changes"
            return result
        
        # Push to GitHub
        if not push_to_github():
            result["message"] = "Committed locally but failed to push to GitHub"
            result["status"] = "partial"
            return result
        
        # Success
        result["status"] = "ok"
        result["message"] = f"Successfully committed and pushed data for {target_date}"
        logger.info("Daily git commit completed successfully")
        
    except Exception as e:
        logger.exception("Unexpected error in daily git commit")
        result["message"] = f"Unexpected error: {str(e)}"
    
    return result


if __name__ == "__main__":
    # Test the git operations
    logging.basicConfig(level=logging.INFO)
    result = daily_git_commit()
    print(f"Result: {result}")