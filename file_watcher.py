import threading
import time
import logging
from queue import Queue
from pathlib import Path
from datetime import datetime, timezone, timedelta
from config.logging_config import setup_logging

logger = logging.getLogger('File Watcher')
SENTINEL = object()

PATH_TO_WATCH = Path('watch_dir')
POLL_INTERVAL = 2
STABILITY_SECONDS = 3

def producer(q: Queue, watch_dir: Path, stop_event: threading.Event):
    """
    Controlla periodicamente la directory e mette in coda solo i file stabili.
    """
    seen_files: set[Path] = set()
    candidates: dict[Path, datetime] = {}

    logger.info("Producer avviato")

    while not stop_event.is_set():
        now = datetime.now(timezone.utc)

        for path in watch_dir.iterdir():
            if not path.is_file():
                continue

            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)

            if path not in candidates:
                candidates[path] = mtime
                continue

            if candidates[path] != mtime:
                candidates[path] = mtime
                continue

            if path not in seen_files and now - mtime >= timedelta(seconds=STABILITY_SECONDS):
                logger.info("Il file %s è stabile", path.name)
                seen_files.add(path)
                q.put(path)

        time.sleep(POLL_INTERVAL)

    logger.info("Producer terminato")

def consumer(q: Queue):
    """
    Consuma file dalla queue e li processa.
    """
    while True:
        elemento = q.get()
        if elemento is SENTINEL:
            logger.info("Sentinella ricevuta, termino il thread")
            q.task_done()
            break
        q.task_done()



def main():
    setup_logging()
    PATH_TO_WATCH.mkdir(exist_ok=True)

    q = Queue()
    stop_event = threading.Event()

    prod = threading.Thread(target=producer,args=(q, PATH_TO_WATCH, stop_event))
    cons = threading.Thread(target=consumer,args=(q,))
    prod.start()
    cons.start()

    try:
        prod.join()
    except KeyboardInterrupt:
        logger.warning("KeyboardInterrupt ricevuto, avvio shutdown")
        stop_event.set()
        q.put(SENTINEL)
    finally:
        prod.join()
        q.join()
        cons.join()
        logger.info("Shutdown completato")

if __name__ == '__main__':
    main()