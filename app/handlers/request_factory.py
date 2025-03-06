from dataclasses import dataclass
from ..utils.constants import ApiKey
from .abstract_request import Request
from .api_versions_request import ApiVersionsRequest
from .describe_topic_partitions_request import DescribeTopicPartitionsRequest

class RequestFactory:
    """Factory to create request objects based on API key."""
    @staticmethod
    def create_request(request: bytes) -> Request:
        api_key_bytes = request[4:6]
        api_key = int.from_bytes(api_key_bytes)
        if api_key == ApiKey.API_VERSIONS.value:
            return ApiVersionsRequest.decode(request)
        elif api_key == ApiKey.DESCRIBE_TOPIC_PARTITIONS.value:
            return DescribeTopicPartitionsRequest.decode(request)
        else:
            raise ValueError(f"Unknown API key: {api_key}")

