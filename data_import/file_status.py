import os
import stat
from concurrent.futures import Future
from threading import Event, Thread


FILE_UPDATED = "Update"
FILE_STOPPED = "Stopped"


def file_key(path):
    if isinstance(path, str) and path:
        return os.path.normcase(os.path.abspath(path))
    return None


def scan_file_metadata(paths, cancelled=None):
    """Read file size and modification time once per distinct linked path."""
    metadata = {}
    for path in paths:
        if cancelled is not None and cancelled.is_set():
            break
        key = file_key(path)
        if key is None or key in metadata:
            continue
        try:
            info = os.stat(path)
        except (OSError, ValueError):
            metadata[key] = None
        else:
            metadata[key] = (info.st_mtime_ns, info.st_size) if stat.S_ISREG(info.st_mode) else None
    return metadata


class FileStatusScan:
    def __init__(self, references):
        self.references = references
        self.cancelled = Event()
        self.future = Future()
        # A network path can block in stat() even when the window has closed.
        Thread(target=self._run, name="battery-file-status", daemon=True).start()

    def _run(self):
        try:
            metadata = scan_file_metadata(self.references.values(), self.cancelled)
        except Exception as error:
            self.future.set_exception(error)
        else:
            self.future.set_result(metadata)
