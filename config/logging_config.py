import logging
from logging.handlers import RotatingFileHandler


def setup_logging(level=logging.INFO) -> None:
    """Set up application-wide logging."""

    root_logger = logging.getLogger()

    if root_logger.handlers:
        return

    root_logger.setLevel(level)

    formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )

    # Stream handler
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    # Rotating file handler
    rotating_handler = RotatingFileHandler(
        filename="watcher.log",
        maxBytes=5_000,
        backupCount=5,
        encoding="utf-8",
    )
    rotating_handler.setFormatter(formatter)

    root_logger.addHandler(stream_handler)
    root_logger.addHandler(rotating_handler)
