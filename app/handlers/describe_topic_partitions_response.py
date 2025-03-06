from typing import Dict, Any
from .abstract_response import Response
from ..utils.constants import ErrorCode, ApiKey

class DescribeTopicPartitionsResponse(Response):
    """Response class for Describe Topic Partitions."""
    def __init__(self, request_obj: Dict[str, Any]):
        self.request_obj = request_obj

    def make_response(self) -> bytes:
        request_header = self.request_obj['header']
        correlation_id_bytes = request_header['correlation_id'].to_bytes(4)
        tag_buffer = b'\x00'
        response_header = correlation_id_bytes + tag_buffer
        request_body = self.request_obj["body"]
        error_code = ErrorCode.UNKNOWN_TOPIC_OR_PARTITION.value
        req_topics_arr = request_body["topics_arr"]
        topic_name_length = req_topics_arr[0][0]
        req_topics_arr_len = len(req_topics_arr) + 1
        is_internal = 0
        partitions_array_len = 0
        topic_authorized_ops = 0x00000df8
        throttle_time = 0
        response_body = (
            throttle_time.to_bytes(4),  # throttle_time
            req_topics_arr_len.to_bytes(1),  # array_length
            error_code.to_bytes(2),  # error_code
            topic_name_length.to_bytes(1),  # topic_name_length
            req_topics_arr[0][1].encode('utf-8'),  # topic_name
            bytes.fromhex('00000000000000000000000000000000'),  # topic_id
            is_internal.to_bytes(1),  # is_internal 1
            partitions_array_len.to_bytes(1),  # partitions_array_len 1
            topic_authorized_ops.to_bytes(4),  # topic_authorized_ops 4
            tag_buffer,  # tag_buffer
            b'\xff',  # next_cursor
            tag_buffer  # tag_buffer
        )
        message = response_header + b''.join(response_body)
        return len(message).to_bytes(4) + message
