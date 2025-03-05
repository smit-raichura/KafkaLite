import socket  # noqa: F401
import threading





def isValidApiVersion(request_obj):
    request_api_version_bytes = request_obj["request_api_version_bytes"]
    request_api_version = int.from_bytes(request_api_version_bytes, "big")
    if request_api_version <= 0 or request_api_version >= 4:
        return False
    return True


def parseRequest(request):
    print('----------------------- Parsing Request ----------------------')
    print(f'RAW_REQUEST : {request}')
    
    msg_len_bytes = request [:4]
    msg_len = int.from_bytes(msg_len_bytes)
# --------------------- Request Header ----------------------------------------    
    api_key_bytes = request[4:6]
    api_key = int.from_bytes(api_key_bytes)

    api_version_bytes = request[4:8]
    api_version = int.from_bytes(api_version_bytes)

    correlation_id_bytes = request[8:12]
    correlation_id = int.from_bytes(correlation_id_bytes)

    client_id_string_len_bytes = request[12:14]
    client_id_string_len = int.from_bytes(client_id_string_len_bytes)

    clinet_id_bytes : bytes = request[14: 14 + client_id_string_len]
    client_id = clinet_id_bytes.decode('utf-8')
    index = 14 + client_id_string_len
    
    tag_buffer = b'\x00'
    tag_buffer = request[index:index + 1]
    index += 1
# --------------------- Request Header ----------------------------------------

    header = {
        "msg_len" : msg_len,
        "api_key" : api_key,
        "api_version" : api_version,
        "correlation_id" : correlation_id,
        "client_id" : client_id
    }

    print(f'REQUEST_HEADER : {header}')

# --------------------- Request Body ----------------------------------------
    topics_array_len_bytes = request[index : index + 1]
    topics_array_len = int.from_bytes(topics_array_len_bytes)
    
    index += 1

    print(f'index : {index} topics_arr_len : {topics_array_len}')

    topics_arr = []
    for i in range(topics_array_len-1):
        topic_name_str_len_bytes =  request[index: index + 1]
        topic_name_str_len = int.from_bytes(topic_name_str_len_bytes)
        index += 1

        topic_name_bytes : bytes = request[index : index + topic_name_str_len - 1]
        topic_name = topic_name_bytes.decode('utf-8')
        index += topic_name_str_len 

        tag_buffer
        index += 1

        print(f'topics_arr : {i} : {(topic_name_str_len, topic_name, tag_buffer)}')
        topics_arr.append((topic_name_str_len, topic_name, tag_buffer))

    print(f'topics_arr : {topics_arr}')

    response_partition_limit_bytes = request[index: index + 4]
    response_partition_limit = int.from_bytes(response_partition_limit_bytes)
    index += 4

    cursor_bytes = b'\xff'
    index += 1

    tag_buffer
    index += 1

# --------------------- Request Body ----------------------------------------
    body = {
        "topics_arr" : topics_arr,
        "response_partition_limit" : response_partition_limit,
        "cursor_bytes" : cursor_bytes
    }
    print(f'REQUEST_BODY : {body}')

    request_obj = {
        "header" : header,
        "body" : body
    }
    print('----------------------- Parsing Request ----------------------')

    return request_obj
'''
 DescribeTopicPartitions (v0) request:

 message_length_bytes : 4 endian int
----------- Header -----------  
 api_key : 2 int
 api_version : 2 int
 correlation_id : 4 int
 client_id_len : 4 int
 client_id_content : client_id_len
 tag_buffer : 1 
----------- Header ----------- 
----------- Body ----------- 
 topic_array_len : 2 int
    topic_name_len : 2 int
    topic_name : topic_name_len str(utf-8)
    tag_buffer : 1
 response_partition_limit : 4 int
 cursor : 1 nullable; default: b'\xff'
 tag_buffer : 1

DescribeTopicPartitions Request (Version: 0) => [topics] response_partition_limit cursor TAG_BUFFER 
  topics => name TAG_BUFFER 
    name => COMPACT_STRING
  response_partition_limit => INT32
  cursor => topic_name partition_index TAG_BUFFER 
    topic_name => COMPACT_STRING
    partition_index => INT32


The tester will validate that:

The first 4 bytes of your response (the "message length") are valid.
The correlation ID in the response header matches the correlation ID in the request header.
The error code in the response body is 3 (UNKNOWN_TOPIC_OR_PARTITION).
The response body should be valid DescribeTopicPartitions (v0) Response.
The topic_name field in the response should be equal to the topic name sent in the request.
The topic_id field in the response should be equal to 00000000-0000-0000-0000-000000000000.
The partitions field in the response should be empty. (As there are no partitions assigned to this non-existent topic.)
'''
def make_response_describeTopicPartitions(request_obj):
    '''
    messg_len : 4 int 
    ------ header -------
    
    correlation_id : 4 int
    tag_buffer : 1
    ------ header -------
    '''
    print('----------------------- Making Response ----------------------')

    request_header = request_obj['header']
    correlation_id :int = request_header['correlation_id']
    correlation_id_bytes = correlation_id.to_bytes(4)
    tag_buffer = b'\x00'

    response_header = correlation_id_bytes + tag_buffer

    request_body = request_obj["body"]
    req_topics_arr = request_body["topics_arr"]

    throttle_time = 0
    array_length = len(req_topics_arr)
    error_code = 3
    topic_name_length = req_topics_arr[0][0]
    topic_name : str = req_topics_arr[0][1]
    topic_id = '00000000000000000000000000000000'
    is_internal = 0
    partitions_array_len = 0
    topic_authorized_ops =  0x00000df8
    tag_buffer = b'\x00'
    next_cursor = b'\xff'

    response_body = (
        throttle_time.to_bytes(4),
        array_length.to_bytes(1),
        error_code.to_bytes(2),
        topic_name.encode('utf-8'),
        bytes.fromhex(topic_id),
        is_internal.to_bytes(1),
        partitions_array_len.to_bytes(1),
        topic_authorized_ops.to_bytes(4),
        tag_buffer,
        next_cursor,
        tag_buffer
    )

    # throttle_time_ms = 0 # 4 bytes
    # throttle_time_bytes = throttle_time_ms.to_bytes(4)
    
    # response_body = throttle_time_bytes

    # topics_arr =[]
    # req_topics_arr_len = len(request_body["topics_arr"]) 
    # req_topics_arr = request_body['topics_arr']

    # error_code = 3 #

    # topic_id = '00000000-0000-0000-0000-000000000000' # 16 byte #
    # topic_id_bytes = bytes.fromhex(topic_id.replace("-", "")) 
    # is_internal = 0 #
    # partitions_array = 0 #
    # topic_authorized_operations = int("00000df8", 16) # 4 bytes

    # for len_, name, buffer in req_topics_arr:
    #     tup = []
    #     tup.append(error_code)
    #     tup.append(len_)
    #     print(f'topic_name_len : {len_}')
    #     tup.append(name)
    #     tup.append(topic_id)
    #     tup.append(is_internal)
    #     tup.append(partitions_array)
    #     tup.append(topic_authorized_operations)
    #     topics_arr.append(tup)
    # response_body += len(topics_arr).to_bytes(1)

    # print(f'topics_arr_response : {topics_arr} \n topics.length : {len(topics_arr)}')

    # for error, len_, name_, uuid, is_internal_, part_arr, ops in topics_arr:
    #     response_body += error.to_bytes(2)
    #     response_body += len_.to_bytes(1)
    #     response_body += name_.encode('utf-8')
    #     response_body += bytes.fromhex(uuid.replace("-", ""))
    #     response_body += is_internal_.to_bytes(1)
    #     response_body += part_arr.to_bytes(1) 
    #     response_body += ops.to_bytes(4)
    #     response_body += tag_buffer
    
    # next_cursor_bytes =  b'\xff'
    # response_body += next_cursor_bytes
    # response_body += tag_buffer

    
    message = response_header + response_body
    response = len(message).to_bytes(4) + message
    print('----------------------- Making Response ----------------------')
    # print(response.hex())
    return response
    




def make_response_apiVersions(request_obj):

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
        client.sendall(make_response_describeTopicPartitions(request_obj))
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
        # print(f"Started thread: {thread.name}, Alive: {thread.is_alive()}, Daemon: {thread.daemon}")
         


if __name__ == "__main__":
    main()

