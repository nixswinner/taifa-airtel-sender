import logging
import os
import sys
from pythonjsonlogger import jsonlogger
from config import Config

_LOG_FILE = os.path.join(os.getenv("LOG_DIR", "logs"), "consumer.log")
_formatter = jsonlogger.JsonFormatter(
    "%(timestamp)s %(level)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def _file_handler() -> logging.Handler:
    os.makedirs(os.path.dirname(_LOG_FILE), exist_ok=True)
    h = logging.FileHandler(_LOG_FILE)
    h.setFormatter(_formatter)
    return h


def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO))
    logger.handlers = []

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(_formatter)
    logger.addHandler(stdout_handler)
    logger.addHandler(_file_handler())

    return logger
