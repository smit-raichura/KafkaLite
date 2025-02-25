import socket  # noqa: F401


def createMessage(data):    

    corelationId_Bytes = int.from_bytes(data[8:12], byteorder = "big")
    
    return len(corelationId_Bytes).to_bytes(4, byteorder="big") + corelationId_Bytes

def handleClient(client):
    data = client.recv(1024)
    client.sendall(createMessage(data))
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
