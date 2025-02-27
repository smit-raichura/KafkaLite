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

def createMessage(request_obj, isValid):
    '''
    header -> correlation_id

    body -> error_code      : 0 if valid 35 if invalid
            api_keys        : number of api keys to expect 
                              -> each of 6 bytes, can be from 0 to 81; as there are only 81 unique differentapi_keys; can be rpresesnted in 2 bytes
                              -> basically works as index to set 6byte off set for each api_key in the request
                              -> eg: 
                                    - only one api_key say produce api key = 0
                                        error_code = 0 (0x0000)  
                                        api_keys count = 1 (0x0001)  
                                        api_key = 0 (0x0000)  # Produce API  
                                        min_version = 3 (0x0003)  
                                        max_version = 9 (0x0009)  
                                        throttle_time_ms = 0 (0x00000000)

                                        byte representation: 

                                        0000  # error_code (2 bytes)
                                        0001  # number of API keys (2 bytes)
                                        0000  # api_key (2 bytes)
                                        0003  # min_version (2 bytes)
                                        0009  # max_version (2 bytes)
                                        00000000  # throttle_time_ms (4 bytes) ...

                                    - two api_key in request produce, fetch key = 0, 1 respectively
                                        error_code = 0 (0x0000)  
                                        api_keys count = 2 (0x0002)  
                                        API Key 1: Produce (API Key = 0)
                                            api_key = 0 (0x0000)  
                                            min_version = 3 (0x0003)  
                                            max_version = 9 (0x0009)  
                                        API Key 2: Fetch (API Key = 1)
                                            api_key = 1 (0x0001)  
                                            min_version = 5 (0x0005)  
                                            max_version = 12 (0x000C)  
                                        throttle_time_ms = 0 (0x00000000)


                                        byte representation:

                                        0000  # error_code (2 bytes)
                                        0002  # number of API keys (2 bytes)
                                        0000  # api_key 1 (2 bytes)
                                        0003  # min_version 1 (2 bytes)
                                        0009  # max_version 1 (2 bytes)
                                        0001  # api_key 2 (2 bytes)
                                        0005  # min_version 2 (2 bytes)
                                        000C  # max_version 2 (2 bytes)
                                        00000000  # throttle_time_ms (4 bytes)
                                // refrence of all possible api_keys: https://kafka.apache.org/protocol.html#protocol_api_keys

            api_key         : which type of api is being called; values can be between 0 to 81
            max_version     : max_version of that particular api allowed
            min_version     : min_version of that particular api allowed
            _tagged_fields  : \x00 for this task

            throttle_time_ms : 
            _tagged_fields   : \x00 for this task

    '''
    correlation_id_bytes = request_obj["correlation_id_bytes"]
    response_header = correlation_id_bytes
    
    
    error_code = 0 if isValid else 35  # if isvalid then gvie no errror: 0 if invalid return error: 35
    min_version, max_version = 0, 4
    throttle_time_ms = 0
    tag_buffer = b"\x00"
    api_key_bytes = request_obj["request_api_key_bytes"]
    api_keys = 2

    response_body = (
        error_code.to_bytes(2) #error_code
        + api_keys.to_bytes(1)
        + api_key_bytes
        + min_version.to_bytes(2)
        + max_version.to_bytes(2)
        + tag_buffer
        + throttle_time_ms.to_bytes(4)
        + tag_buffer
    )

    response_length = len(response_header + response_body)

    return int(response_length).to_bytes(4) + response_header + response_body
    

def handleClient(client):
    request = client.recv(2048)
    request_obj = parseRequest(request = request)
    is_valid_request_api_version = isValidApiVersion(request_obj)
    client.sendall(createMessage(request_obj, is_valid_request_api_version))
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
