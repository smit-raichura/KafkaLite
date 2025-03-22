#!/bin/sh
#
# Use this script to run your program LOCALLY.
#
# Note: Changing this script WILL NOT affect how CodeCrafters runs your program.
#
# Learn more: https://codecrafters.io/program-interface

set -e # Exit early if any commands fail

# Configure logging
# Uncomment and modify these environment variables to change logging behavior
# Available log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
export KAFKA_LOG_LEVEL="INFO"

# Set to "true" to enable file logging
# export KAFKA_LOG_TO_FILE="true"

# Directory for log files (only used if KAFKA_LOG_TO_FILE is "true")
# export KAFKA_LOG_DIR="logs"

# Copied from .codecrafters/run.sh
#
# - Edit this to change how your program runs locally
# - Edit .codecrafters/run.sh to change how your program runs remotely
exec python3 -m app.main "$@"
