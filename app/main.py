import socket  # noqa: F401
import threading



def parseRequest(request):
    
    request_obj = {
        "message_size_bytes" : request[0:4],
        "request_api_key_bytes" : request[4:6],
        "request_api_version_bytes" : request[6:8],
        "correlation_id_bytes" : request[8:12]

    }
    
    return request_obj

def isValidApiVersion(request_obj):
    request_api_version_bytes = request_obj["request_api_version_bytes"]
    request_api_version = int.from_bytes(request_api_version_bytes, "big")
    if request_api_version <= 0 or request_api_version >= 4:
        return False
    return True

def createMessage(request_obj):

    header = request_obj["correlation_id_bytes"]

    # Body:
    # - Error code (2 bytes, big-endian, 0 = No Error)
    
    error_code = (
        0
        if int.from_bytes(request_obj["request_api_version_bytes"]) in range(5)
        else 35
    )
    error_code_bytes = error_code.to_bytes(2, byteorder="big")
    tagged_fields_bytes = b"\x00"
    # - API versions list:
    #   Each entry contains:
    #   - API key (2 bytes, big-endian)
    #   - MinVersion (2 bytes, big-endian)
    #   - MaxVersion (2 bytes, big-endian)
    api_versions = [
        (18, 0, 4, tagged_fields_bytes),  # APIVersions (API key 18)
        (75, 0, 0, tagged_fields_bytes),  # DescribeTopicPartitions (API key 75)
    ]
    api_versions_length_bytes = (len(api_versions) + 1).to_bytes(1, byteorder='big')
    # Encode the API versions list
    api_versions_bytes = b""
    for api_key, min_version, max_version, tag_buffter in api_versions:
        api_versions_bytes += (
            api_key.to_bytes(2, byteorder="big")
            + min_version.to_bytes(2, byteorder="big")
            + max_version.to_bytes(2, byteorder="big")
            + tag_buffter
        )

    # - Tagged fields (TAG_BUFFER):
    #   - TAG_BUFFER length (1 byte, unsigned varint)
    #   - TAGGED_FIELD_ARRAY (empty in this case)
    throttle_time = 0
    throttle_time_bytes = throttle_time.to_bytes(4)

    # Combine the body
    body = error_code_bytes + api_versions_length_bytes + api_versions_bytes + throttle_time_bytes + tagged_fields_bytes

    # Calculate the message length (4 bytes, big-endian)
    message_length = len(header) + len(body)
    message_length_bytes = message_length.to_bytes(4, byteorder="big")

    # Combine everything into the final message
    message = message_length_bytes + header + body
    return message
    
    
    

def handleClient(client):
    while True:
        request = client.recv(2048)
        request_obj = parseRequest(request = request)
        # is_valid_request_api_version = isValidApiVersion(request_obj)
        client.sendall(createMessage(request_obj))
        # client.close()
    



def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True:
        client, addr = server.accept()
        thread = threading.Thread(target= handleClient, args=(client, ))
        thread.start()
    
         


if __name__ == "__main__":
    main()

# import socket
# from abc import ABC, abstractmethod

# class KafkaRequestParser:
#     """Handles parsing of Kafka requests."""
#     @staticmethod
#     def parse(request: bytes) -> dict:
#         return {
#             "message_size_bytes": request[0:4],
#             "request_api_key_bytes": request[4:6],
#             "request_api_version_bytes": request[6:8],
#             "correlation_id_bytes": request[8:12]
#         }

# class KafkaResponseBuilder:
#     """Constructs responses for Kafka requests."""
#     @staticmethod
#     def create_response(request_obj: dict) -> bytes:
#         correlation_id_bytes = request_obj["correlation_id_bytes"]
#         response_header = correlation_id_bytes
        
#         error_code = 0 if int.from_bytes(request_obj["request_api_version_bytes"], "big") in range(5) else 35
#         min_version, max_version = 0, 4
#         throttle_time_ms = 0
#         tag_buffer = b"\x00"
#         api_key_bytes = request_obj["request_api_key_bytes"]
#         api_keys = 2
        
#         response_body = (
#             error_code.to_bytes(2, "big") +
#             api_keys.to_bytes(1, "big") +
#             api_key_bytes +
#             min_version.to_bytes(2, "big") +
#             max_version.to_bytes(2, "big") +
#             tag_buffer +
#             throttle_time_ms.to_bytes(4, "big") +
#             tag_buffer
#         )

#         response_length = len(response_header + response_body)
#         return int(response_length).to_bytes(4, "big") + response_header + response_body

# class KafkaRequestHandler(ABC):
#     """Abstract base class for handling Kafka requests."""
#     @abstractmethod
#     def handle(self, request_obj: dict) -> bytes:
#         pass

# class ApiVersionsHandler(KafkaRequestHandler):
#     """Handles API Version requests."""
#     def handle(self, request_obj: dict) -> bytes:
#         return KafkaResponseBuilder.create_response(request_obj)

# class KafkaServer:
#     """Manages the Kafka server."""
#     def __init__(self, host: str = "localhost", port: int = 9092):
#         self.host = host
#         self.port = port
#         self.server = socket.create_server((self.host, self.port), reuse_port=True)
#         self.handlers = {0: ApiVersionsHandler()}
    
#     def handle_client(self, client_socket: socket.socket):
#         request = client_socket.recv(2048)
#         request_obj = KafkaRequestParser.parse(request)
#         api_key = int.from_bytes(request_obj["request_api_key_bytes"], "big")
        
#         handler = self.handlers.get(api_key, ApiVersionsHandler())
#         response = handler.handle(request_obj)
#         client_socket.sendall(response)
#         client_socket.close()
    
#     def start(self):
#         print(f"Kafka server running on {self.host}:{self.port}")
#         while True:
#             client, _ = self.server.accept()
#             self.handle_client(client)

# if __name__ == "__main__":
    server = KafkaServer()
    server.start()
