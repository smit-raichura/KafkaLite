import socket
import asyncio
class KafkaServer:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server_socket = socket.create_server(self.host, self.port, True)
        self.clients = []
        
