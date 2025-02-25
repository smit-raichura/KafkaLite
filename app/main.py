import socket  # noqa: F401


def createMessage(corelationId, messageBody):
    corelationId_Bytes = corelationId.to_bytes(4, byteorder = "big")
    messageBody_Bytes = messageBody.to_bytes(4, byteorder = "big")
    return messageBody_Bytes + corelationId_Bytes

def handleClient(client):
    client.sendall(createMessage(7, 1))
    client.close()
    pass



def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        client, addr = server.accept()
        
         


if __name__ == "__main__":
    main()
