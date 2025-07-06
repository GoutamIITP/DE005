import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from export_data import export_joined_table

class Handler(FileSystemEventHandler):
    def on_created(self, event):
        if event.src_path.endswith(".trigger"):
            print(f"Trigger file {event.src_path} detected. Running export...")
            export_joined_table()
            os.remove(event.src_path)

if __name__ == "__main__":
    path = "watch_folder/"
    os.makedirs(path, exist_ok=True)

    observer = Observer()
    observer.schedule(Handler(), path, recursive=False)
    observer.start()
    print("Watching for trigger file in 'watch_folder/'...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
