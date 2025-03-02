import socket

import threading
from ..handlers.request_header import Requestheader 

class KafkaServer:

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
    
    def start(self):
        self.server_socket = socket.create_server((self.host, self.port), reuse_port= True)
        while True:
            client_socket, addr = self.server_socket.accept()
            thread = threading.Thread(target = self.handle_client, args = (client_socket, ))
            thread.start()
        

    def get_server_socket(self):
        return self.server_socket

    def handle_client(self, client_socket):
        while True:
            request = client_socket.recv(2048)
            #parse the request header
            request_header_instance = Requestheader(request)



        
