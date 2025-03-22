import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional

# Define log levels
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

# Default log level
DEFAULT_LOG_LEVEL = "INFO"

# Default log format
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Default log directory
DEFAULT_LOG_DIR = "logs"

class KafkaLogger:
    """
    Logger class for Kafka server application.
    Provides logging functionality with configurable log levels and output destinations.
    """
    _loggers = {}

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get or create a logger with the specified name.
        
        Args:
            name: The name of the logger, typically the module name
            
        Returns:
            A configured logger instance
        """
        if name in cls._loggers:
            return cls._loggers[name]
        
        # Get log level from environment or use default
        log_level_name = os.environ.get("KAFKA_LOG_LEVEL", DEFAULT_LOG_LEVEL)
        log_level = LOG_LEVELS.get(log_level_name, logging.INFO)
        
        # Create logger
        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        
        # Avoid adding handlers if they already exist
        if not logger.handlers:
            # Create console handler
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            
            # Create formatter
            formatter = logging.Formatter(DEFAULT_LOG_FORMAT)
            console_handler.setFormatter(formatter)
            
            # Add console handler to logger
            logger.addHandler(console_handler)
            
            # Create file handler if log directory is specified
            log_to_file = os.environ.get("KAFKA_LOG_TO_FILE", "false").lower() == "true"
            if log_to_file:
                log_dir = os.environ.get("KAFKA_LOG_DIR", DEFAULT_LOG_DIR)
                
                # Create log directory if it doesn't exist
                os.makedirs(log_dir, exist_ok=True)
                
                # Create rotating file handler (10 MB max size, 5 backup files)
                file_handler = RotatingFileHandler(
                    os.path.join(log_dir, f"{name}.log"),
                    maxBytes=10*1024*1024,  # 10 MB
                    backupCount=5
                )
                file_handler.setLevel(log_level)
                file_handler.setFormatter(formatter)
                
                # Add file handler to logger
                logger.addHandler(file_handler)
        
        # Store logger in cache
        cls._loggers[name] = logger
        
        return logger

# Convenience function to get a logger
def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: The name of the logger, typically the module name
        
    Returns:
        A configured logger instance
    """
    return KafkaLogger.get_logger(name)
