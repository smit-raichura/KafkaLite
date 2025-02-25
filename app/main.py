import socket  # noqa: F401

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

def createMessage(correlation_id_bytes, isValid):
    message = correlation_id_bytes
    if isValid == False:
        error_code = 35
        message += error_code.to_bytes(2, byteorder="big")

    return bytes(4) + message

def handleClient(client):
    request = client.recv(2048)
    request_obj = parseRequest(request = request)
    is_valid_request_api_version = isValidApiVersion(request_obj)
    client.sendall(createMessage(request_obj["correlation_id_bytes"], is_valid_request_api_version))
    client.close()
    



def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept()
        handleClient(client)
        
         


if __name__ == "__main__":
    main()
