import threading
import time
import logging
from queue import Queue
from pathlib import Path
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)
SENTINEL = object()


class DirectoryWatcher:
    def __init__(
        self,
        watch_dir: Path,
        poll_interval: int,
        stability_seconds: int,
    ) -> None:
        self.watch_dir = watch_dir
        self.poll_interval = poll_interval
        self.stability_seconds = stability_seconds

        self.queue: Queue = Queue()
        self.stop_event = threading.Event()

        self._producer_thread: threading.Thread | None = None
        self._consumer_thread: threading.Thread | None = None

        self._seen_files: set[Path] = set()
        self._candidates: dict[Path, datetime] = {}

    # -------------------------
    # Public API
    # -------------------------

    def start(self) -> None:
        logger.info("Avvio DirectoryWatcher")

        self.watch_dir.mkdir(exist_ok=True)

        self._producer_thread = threading.Thread(
            target=self._producer, name="producer"
        )
        self._consumer_thread = threading.Thread(
            target=self._consumer, name="consumer"
        )

        self._producer_thread.start()
        self._consumer_thread.start()

    def stop(self) -> None:
        logger.info("Arresto DirectoryWatcher")

        self.stop_event.set()
        self.queue.put(SENTINEL)

        if self._producer_thread:
            self._producer_thread.join()

        self.queue.join()

        if self._consumer_thread:
            self._consumer_thread.join()

        logger.info("DirectoryWatcher arrestato correttamente")

    # -------------------------
    # Internal methods
    # -------------------------

    def _producer(self) -> None:
        logger.info("Producer avviato")

        while not self.stop_event.is_set():
            now = datetime.now(timezone.utc)

            for path in self.watch_dir.iterdir():
                if not path.is_file():
                    continue

                mtime = datetime.fromtimestamp(
                    path.stat().st_mtime,
                    tz=timezone.utc,
                )

                if path not in self._candidates:
                    self._candidates[path] = mtime
                    continue

                if self._candidates[path] != mtime:
                    self._candidates[path] = mtime
                    continue

                if (
                    path not in self._seen_files
                    and now - mtime >= timedelta(seconds=self.stability_seconds)
                ):
                    logger.info("File stabile rilevato: %s", path.name)
                    self._seen_files.add(path)
                    self.queue.put(path)

            time.sleep(self.poll_interval)

        logger.info("Producer terminato")

    def _consumer(self) -> None:
        logger.info("Consumer avviato")

        while True:
            item = self.queue.get()

            try:
                if item is SENTINEL:
                    logger.info("Sentinella ricevuta dal consumer")
                    break

                self._process_file(item)

            finally:
                self.queue.task_done()

        logger.info("Consumer terminato")

    def _process_file(self, path: Path) -> None:
        """
        Qui va la logica reale di processing.
        Per ora simuliamo.
        """
        logger.info("Processing file: %s", path.name)
        time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format=('%(asctime)s | %(name)s | %(levelname)s | %(message)s'))
    watcher = DirectoryWatcher(Path('watch_dir'),3,3)
    watcher.start()