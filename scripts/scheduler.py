import schedule
import time
import os
import sys
import traceback
from export_data import export_joined_table

# Add current directory to path to ensure import works
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

def job():
    try:
        print(f"Running scheduled export at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        export_joined_table()
        print("Export completed successfully!")
    except Exception as e:
        print(f"Error during export: {e}")
        traceback.print_exc()

# Schedule to run every minute
schedule.every(1).minutes.do(job)

print("Scheduler started. Press Ctrl+C to stop.")
print(f"Current working directory: {os.getcwd()}")
print(f"Script directory: {current_dir}")

# Run the job once immediately to test
print("Running initial test...")
job()

# Main scheduler loop
try:
    while True:
        schedule.run_pending()
        time.sleep(1)
except KeyboardInterrupt:
    print("\nScheduler stopped by user.")
except Exception as e:
    print(f"Scheduler error: {e}")
    traceback.print_exc()