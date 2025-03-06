from dataclasses import dataclass
from ..utils.constants import ApiKey
from .abstract_response import Response
from typing import Dict, Any
from .api_versions_response import ApiVersionsResponse
from .describe_topic_partitions_response import DescribeTopicPartitionsResponse

class ResponseFactory:
    """Factory to create response objects based on request type."""
    @staticmethod
    def create_response(request_obj: Dict[str, Any]) -> Response:
        if "request_api_version_bytes" in request_obj:
            return ApiVersionsResponse(request_obj)
        elif "topics_arr" in request_obj.get("body", {}):
            return DescribeTopicPartitionsResponse(request_obj)
        else:
            raise ValueError("Unknown request type")