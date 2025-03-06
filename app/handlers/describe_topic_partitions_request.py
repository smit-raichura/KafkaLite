from typing import Dict, Any
from .abstract_request import Request

class DescribeTopicPartitionsRequest(Request):
    """Request class for Describe Topic Partitions."""
    @classmethod
    def decode(cls, request: bytes) -> Dict[str, Any]:
        index = 0
        msg_len = int.from_bytes(request[index:index + 4])
        index += 4
        api_key = int.from_bytes(request[index:index + 2])
        index += 2
        api_version = int.from_bytes(request[index:index + 2])
        index += 2
        correlation_id = int.from_bytes(request[index:index + 4])
        index += 4
        client_id_len = int.from_bytes(request[index:index + 2])
        index += 2
        client_id = request[index:index + client_id_len].decode('utf-8')
        index += client_id_len
        tag_buffer = request[index:index + 1]
        index += 1
        topics_array_len = int.from_bytes(request[index:index + 1])
        index += 1
        topics_arr = []
        for _ in range(topics_array_len - 1):
            topic_name_len = int.from_bytes(request[index:index + 1])
            index += 1
            topic_name = request[index:index + topic_name_len].decode('utf-8')
            index += topic_name_len
            tag_buffer = request[index:index + 1]
            index += 1
            topics_arr.append((topic_name_len, topic_name, tag_buffer))
        response_partition_limit = int.from_bytes(request[index:index + 4])
        index += 4
        cursor_bytes = request[index:index + 1]
        index += 1
        tag_buffer = request[index:index + 1]
        index += 1
        return {
            "header": {
                "msg_len": msg_len,
                "api_key": api_key,
                "api_version": api_version,
                "correlation_id": correlation_id,
                "client_id": client_id
            },
            "body": {
                "topics_arr": topics_arr,
                "response_partition_limit": response_partition_limit,
                "cursor_bytes": cursor_bytes
            }
        }

    def encode(self) -> bytes:
        raise NotImplementedError("Encoding not required for this request type.")