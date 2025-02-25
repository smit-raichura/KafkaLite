import socket  # noqa: F401


def createMessage(response):
    corelationId_Bytes = response[8:12]
    
    return bytes(4) + corelationId_Bytes

def handleClient(client):
    response = client.recv(2048)
    client.sendall(createMessage(response))
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
