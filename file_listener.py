from watchdog.events import FileSystemEventHandler

class FileHandler(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback

    def on_modified(self, event):
        if not event.is_directory:
            self.callback(event.src_path, event.event_type)

    def on_created(self, event):
        if not event.is_directory:
            self.callback(event.src_path, event.event_type)

    def on_deleted(self, event):
        if not event.is_directory:
            self.callback(event.src_path, event.event_type)

