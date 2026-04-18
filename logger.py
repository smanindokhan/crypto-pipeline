import logging
import os
from datetime import datetime

def get_logger(name: str) -> logging.Logger:
    """
    Returns a logger that writes to BOTH:
      - the terminal (so you can see what's happening)
      - a log file in logs/ (so you have a permanent record)

    'name' should be the module name, e.g. 'coin_prices'.
    Each module gets its own named logger — they all feed
    into the same handlers but are labelled separately.
    """
    logger = logging.getLogger(name)

    # Only add handlers once — if get_logger is called
    # multiple times, don't stack duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # ── FORMAT ───────────────────────────────────────────
    # Every log line will look like:
    # 2025-04-18 10:30:01 | INFO     | coin_prices | Fetched 50 coins
    # timestamp            level       module name   your message
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # ── HANDLER 1: Terminal ───────────────────────────────
    # StreamHandler prints to the terminal.
    # Set to INFO so DEBUG messages don't clutter your screen.
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)

    # ── HANDLER 2: File ──────────────────────────────────
    # FileHandler writes to logs/pipeline_YYYY-MM-DD.log
    # A new file per day means logs stay manageable.
    # mode="a" means append — don't overwrite existing logs.
    os.makedirs("logs", exist_ok=True)
    today = datetime.now().strftime("%Y-%m-%d")
    file_handler = logging.FileHandler(
        f"logs/pipeline_{today}.log", mode="a"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    logger.addHandler(console)
    logger.addHandler(file_handler)
    return logger