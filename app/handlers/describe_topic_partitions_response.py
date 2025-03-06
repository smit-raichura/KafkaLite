from typing import Dict, Any
from .abstract_response import Response
from ..utils.constants import ErrorCode, ApiKey

class DescribeTopicPartitionsResponse(Response):
    """Response class for Describe Topic Partitions."""
    def __init__(self, request_obj: Dict[str, Any]):
        self.request_obj = request_obj

    def make_response(self) -> bytes:
        print("----------------------- Making Response ----------------------")
        
        # Extract request header
        request_header = self.request_obj['header']
        correlation_id = request_header['correlation_id']
        print(f"Correlation ID: {correlation_id}")

        # Response header
        correlation_id_bytes = correlation_id.to_bytes(4, byteorder='big')
        tag_buffer = b'\x00'
        response_header = correlation_id_bytes + tag_buffer
        print(f"Response Header: {response_header.hex()}")

        # Extract request body
        request_body = self.request_obj["body"]
        req_topics_arr = request_body["topics_arr"]
        print(f"Request Topics Array: {req_topics_arr}")

        # Response body fields
        throttle_time_ms = 0  # INT32
        array_length = 2  # Only one topic in the response (INT8)
        error_code = 3  # INT16 (UNKNOWN_TOPIC_OR_PARTITION)
        topic_name = req_topics_arr[0][1]  # COMPACT_NULLABLE_STRING
        topic_name_length = len(topic_name)+1  # Length of the topic name
        topic_id = bytes.fromhex('00000000000000000000000000000000')  # UUID (16 bytes)
        is_internal = 0  # BOOLEAN (1 byte)
        partitions_array_len = 0  # No partitions (INT8)
        topic_authorized_operations = 0x00000df8  # INT32
        next_cursor = b'\xff'  # Next cursor (1 byte)
        tagged_fields = b'\x00'  # TAG_BUFFER (1 byte)

        # Construct the response body
        response_body = (
            throttle_time_ms.to_bytes(4, byteorder='big'),  # throttle_time_ms (INT32)
            array_length.to_bytes(1, byteorder='big'),  # array_length (INT8)
            error_code.to_bytes(2, byteorder='big'),  # error_code (INT16)
            topic_name_length.to_bytes(1, byteorder='big'),  # topic_name_length (INT8)
            topic_name.encode('utf-8'),  # topic_name (COMPACT_NULLABLE_STRING)
            topic_id,  # topic_id (UUID, 16 bytes)
            is_internal.to_bytes(1, byteorder='big'),  # is_internal (BOOLEAN, 1 byte)
            partitions_array_len.to_bytes(1, byteorder='big'),  # partitions_array_len (INT8)
            topic_authorized_operations.to_bytes(4, byteorder='big'),  # topic_authorized_operations (INT32)
            tagged_fields,  # TAG_BUFFER (1 byte)
            next_cursor,  # next_cursor (1 byte)
            tagged_fields  # TAG_BUFFER (1 byte)
        )
        print(f"Response Body: {response_body}")
        # Combine the response body into bytes
        response_body_bytes = b''.join(response_body)
        print(f"Response Body: {response_body_bytes.hex()}")

        # Combine the response
        message = response_header + response_body_bytes
        message_length = len(message)
        message_length_bytes = message_length.to_bytes(4, byteorder='big')
        response = message_length_bytes + message

        print(f"Full Response: {response.hex()}")
        print("----------------------- Making Response ----------------------")
        return response