import os
from .server.kafka_server import KafkaServer
from .utils.logger import get_logger

if __name__ == "__main__":
    # Configure logging
    logger = get_logger("main")
    
    # Log application startup
    logger.info("Kafka server application starting")
    
    # Get host and port from environment variables or use defaults
    host = os.environ.get("KAFKA_HOST", "localhost")
    port = int(os.environ.get("KAFKA_PORT", "9092"))
    
    logger.info(f"Initializing Kafka server with host={host}, port={port}")
    
    # Create and start server
    server = KafkaServer(host, port)
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
    except Exception as e:
        logger.error(f"Error starting server: {str(e)}")
        raise
    finally:
        logger.info("Kafka server application stopped")
