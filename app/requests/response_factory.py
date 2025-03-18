from dataclasses import dataclass
from ..utils.constants import ApiKey
from .abstract_response import AbstractResponse
from .Headers.response_header import ResponseHeader
from typing import Dict, Any
from .ApiVersions.api_versions_response import ApiVersionsResponse
from .DescribeTopicPartitions.describe_topic_partitions_response import DescribeTopicPartitionsResponse

class ResponseFactory:
    """Factory to create response objects based on request type."""
    @staticmethod
    def create_response(request):
        response_header = ResponseHeader.from_request_header(request.header)
        response_class : type[AbstractResponse]

        match response_header.api_key:
            case ApiKey.API_VERSIONS:
                response_class = ApiVersionsResponse
            case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                response_class = DescribeTopicPartitionsResponse
        
        return response_class(response_header, **response_class.make_body_kwargs(request))