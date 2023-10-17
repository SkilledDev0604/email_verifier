import time
import subprocess
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileWatcher(FileSystemEventHandler):
    def __init__(self, script_path):
        self.script_path = script_path

    def on_modified(self, event):
        if not event.is_directory:
            # Stop the B script if it's running
            subprocess.run(['pkill', '-f', 'verify.py'])
            time.sleep(1)  # Wait for the process to be terminated
            # Call your existing script with the input and output folder    paths
            input_folder = './input'
            output_folder = './output'
            subprocess.run(['python3', 'verify.py', input_folder,   output_folder])
                

def main():
    input_folder = './input'
    script_path = './verify.py'
    event_handler = FileWatcher(script_path)
    observer = Observer()
    observer.schedule(event_handler, input_folder, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()