import socket
import threading
from io import BytesIO
from ..requests.request_factory import RequestFactory
from ..requests.response_factory import ResponseFactory 
class KafkaServer:
    """Server to handle Kafka client connections."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def start(self):
        server = socket.create_server((self.host, self.port), reuse_port=True)
        while True:
            client, addr = server.accept()
            thread = threading.Thread(target=self.handle_client, args=(client,))
            thread.start()

    def handle_client(self, client: socket.socket):
        while True:
            request_buffer = BytesIO(client.recv(2048))
            if not request_buffer:
                break
            request_obj = RequestFactory.read_request(request_buffer)
            response = ResponseFactory.create_response(request_obj)
            client.sendall(response.encode())
        client.close()

    

        
