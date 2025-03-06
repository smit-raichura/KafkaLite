import socket
import threading
from ..handlers.request_factory import RequestFactory
from ..handlers.response_factory import ResponseFactory 
class KafkaServer:
    """Server to handle Kafka client connections."""
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def handle_client(self, client: socket.socket):
        while True:
            request = client.recv(2048)
            if not request:
                break
            request_obj = RequestFactory.create_request(request)
            response = ResponseFactory.create_response(request_obj).make_response()
            client.sendall(response)
        client.close()

    def start(self):
        server = socket.create_server((self.host, self.port), reuse_port=True)
        while True:
            client, addr = server.accept()
            thread = threading.Thread(target=self.handle_client, args=(client,))
            thread.start()

        
