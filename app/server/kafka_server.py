import socket
import threading
from io import BytesIO
from ..requests.request_factory import RequestFactory
from ..requests.response_factory import ResponseFactory
from ..utils.logger import get_logger
class KafkaServer:
    """Server to handle Kafka client connections."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.logger = get_logger(__name__)

    def start(self):
        self.logger.info(f"Starting Kafka server on {self.host}:{self.port}")
        server = socket.create_server((self.host, self.port), reuse_port=True)
        self.logger.info("Server started, waiting for connections")
        while True:
            client, addr = server.accept()
            self.logger.info(f"New client connection from {addr}")
            thread = threading.Thread(target=self.handle_client, args=(client, addr))
            thread.start()

    def handle_client(self, client: socket.socket, addr):
        client_id = f"{addr[0]}:{addr[1]}"
        self.logger.debug(f"Handling client {client_id}")
        try:
            while True:
                data = client.recv(2048)
                if not data:
                    self.logger.info(f"Client {client_id} disconnected")
                    break
                
                self.logger.debug(f"Received {len(data)} bytes from client {client_id}")
                request_buffer = BytesIO(data)
                
                try:
                    request_obj = RequestFactory.read_request(request_buffer)
                    self.logger.info(f"Processed {request_obj.header.api_key} request from client {client_id}")
                    
                    response = ResponseFactory.create_response(request_obj)
                    self.logger.debug(f"Created response for client {client_id}")
                    
                    encoded_response = response.encode()
                    client.sendall(encoded_response)
                    self.logger.debug(f"Sent {len(encoded_response)} bytes to client {client_id}")
                except Exception as e:
                    self.logger.error(f"Error processing request from client {client_id}: {str(e)}")
                    # Continue serving other requests even if one fails
        except Exception as e:
            self.logger.error(f"Error handling client {client_id}: {str(e)}")
        finally:
            client.close()
            self.logger.info(f"Closed connection with client {client_id}")
