import logging
from typing import Callable, Any

logger = logging.getLogger(__name__)

def log_title(title: str):
    logger.info(f"{'=' * 20}")
    logger.info(f"{title}")
    logger.info(f"{'=' * 20}")

def set_config(f: Callable[[Any], None]):
    f(logging)
