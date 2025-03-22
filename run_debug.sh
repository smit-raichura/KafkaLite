#!/bin/sh
#
# Use this script to run your program with DEBUG level logging.
#
# This script enables detailed logging for development and troubleshooting.

set -e # Exit early if any commands fail

# Configure logging with DEBUG level
export KAFKA_LOG_LEVEL="DEBUG"

# Enable file logging
export KAFKA_LOG_TO_FILE="true"

# Directory for log files
export KAFKA_LOG_DIR="logs"

echo "Starting Kafka server with DEBUG logging..."
echo "Log files will be saved to the 'logs' directory"

# Run the application
python3 -m app.main "$@"
