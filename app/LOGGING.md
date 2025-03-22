# Kafka Server Logging System

This document describes the logging system implemented for the Kafka server application.

## Overview

The logging system provides comprehensive logging capabilities for the Kafka server application, allowing for:

- Different log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Console and file-based logging
- Configurable log formats and destinations
- Component-specific logging

## Configuration

The logging system can be configured using environment variables:

| Environment Variable | Description | Default Value |
|----------------------|-------------|---------------|
| `KAFKA_LOG_LEVEL` | Sets the log level for all loggers | `INFO` |
| `KAFKA_LOG_TO_FILE` | Enable file logging (true/false) | `false` |
| `KAFKA_LOG_DIR` | Directory for log files | `logs` |

### Log Levels

Available log levels (in increasing order of severity):

- `DEBUG`: Detailed information, typically useful only for diagnosing problems
- `INFO`: Confirmation that things are working as expected
- `WARNING`: Indication that something unexpected happened, but the application is still working
- `ERROR`: Due to a more serious problem, the application has not been able to perform some function
- `CRITICAL`: A serious error, indicating that the application itself may be unable to continue running

## Usage

### Running with Default Logging

By default, the application will log at the INFO level to the console:

```bash
python -m app.main
```

### Running with Custom Log Level

To run with a different log level:

```bash
KAFKA_LOG_LEVEL=DEBUG python -m app.main
```

### Enabling File Logging

To enable file logging:

```bash
KAFKA_LOG_LEVEL=DEBUG KAFKA_LOG_TO_FILE=true python -m app.main
```

This will create log files in the `logs` directory (which will be created if it doesn't exist).

### Custom Log Directory

To specify a custom log directory:

```bash
KAFKA_LOG_LEVEL=DEBUG KAFKA_LOG_TO_FILE=true KAFKA_LOG_DIR=/path/to/logs python -m app.main
```

## For Developers

### Using the Logger in Code

To use the logger in your code:

```python
from app.utils.logger import get_logger

# Create a logger with the module name
logger = get_logger(__name__)

# Log messages at different levels
logger.debug("Detailed information for debugging")
logger.info("General information about program execution")
logger.warning("Warning about potential issues")
logger.error("Error information when something fails")
logger.critical("Critical error that might cause program termination")
```

### Best Practices

1. **Use the appropriate log level**:
   - DEBUG: Detailed information for troubleshooting
   - INFO: General operational information
   - WARNING: Unexpected situations that don't cause failures
   - ERROR: Errors that prevent specific operations
   - CRITICAL: Errors that might cause application failure

2. **Include relevant context**:
   - For requests/responses, include correlation IDs
   - For client operations, include client identifiers
   - For errors, include exception details

3. **Avoid sensitive information**:
   - Don't log passwords, tokens, or sensitive user data
   - Be cautious with connection strings and credentials

4. **Performance considerations**:
   - Avoid expensive operations in log messages at DEBUG level
   - Use string formatting only when the log will actually be emitted:
     ```python
     # Good (lazy evaluation)
     logger.debug(f"Processing request: {request_id}")
     
     # Bad (always evaluated)
     logger.debug("Processing request: %s" % compute_expensive_value())
     ```

## Log File Management

When file logging is enabled, the system uses a `RotatingFileHandler` that:

- Creates separate log files for each component
- Limits each log file to 10 MB
- Keeps up to 5 backup files per component

Log files are named after the component, e.g., `main.log`, `app.server.kafka_server.log`, etc.
