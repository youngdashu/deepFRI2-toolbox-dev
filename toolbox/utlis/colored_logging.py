import logging
from typing import Any, Optional

# ANSI color codes
COLORS = {
    'DEBUG': '\033[36m',     # Cyan
    'INFO': '\033[32m',      # Green
    'WARNING': '\033[33m',   # Yellow
    'ERROR': '\033[31m',     # Red
    'CRITICAL': '\033[35m',  # Magenta
    'RESET': '\033[0m',      # Reset
}

class ColoredFormatter(logging.Formatter):
    """Custom formatter that adds colors to logging output."""
    
    def __init__(self, fmt: Optional[str] = None, datefmt: Optional[str] = None):
        if fmt is None:
            fmt = '%(asctime)s %(levelname)s %(message)s'
        if datefmt is None:
            datefmt = '%Y-%m-%d %H:%M:%S'
        super().__init__(fmt, datefmt)
    
    def format(self, record: logging.LogRecord) -> str:
        # Get the level name and its corresponding color
        levelname = record.levelname
        color = COLORS.get(levelname, COLORS['RESET'])
        
        # Add color to the level name
        record.levelname = f"{color}{levelname}{COLORS['RESET']}"
        
        # Format the message
        return super().format(record)

def setup_colored_logging(level: int = logging.INFO, fmt: Optional[str] = None, datefmt: Optional[str] = None) -> None:
    """Set up colored logging for the application.
    
    Args:
        level: Logging level (default: logging.INFO)
        fmt: Log format string (default: '%(asctime)s %(levelname)s %(message)s')
        datefmt: Date format string (default: '%Y-%m-%d %H:%M:%S')
    """
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # Remove any existing handlers to avoid duplicate output
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    
    # Create formatter and add it to the handler
    formatter = ColoredFormatter(fmt, datefmt)
    console_handler.setFormatter(formatter)
    
    # Add the handler to the logger
    logger.addHandler(console_handler)

# Example usage:
if __name__ == "__main__":
    setup_colored_logging()
    logger = logging.getLogger(__name__)
    
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message") 