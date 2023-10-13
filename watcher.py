import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileWatcher(FileSystemEventHandler):
    def on_modified(self, event):
        if not event.is_directory:
            # Call your existing script with the input and output folder paths
            input_folder = './input'
            output_folder = './output'
            subprocess.run(['python3', 'verify.py', input_folder, output_folder])

if __name__ == "__main__":
    input_folder = './input'
    event_handler = FileWatcher()

    observer = Observer()
    observer.schedule(event_handler, input_folder, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()