from dataclasses import dataclass
from ..utils.constants import ApiKey
from ..utils.logger import get_logger
from .abstract_response import AbstractResponse
from .Headers.response_header import ResponseHeader
from typing import Dict, Any
from .ApiVersions.api_versions_response import ApiVersionsResponse
from .DescribeTopicPartitions.describe_topic_partitions_response import DescribeTopicPartitionsResponse
from .Fetch.fetch_response import FetchResponse
class ResponseFactory:
    """Factory to create response objects based on request type."""
    _logger = get_logger(__name__)
    
    @classmethod
    def create_response(cls, request):
        cls._logger.debug(f"Creating response for request with API key: {request.header.api_key}")
        
        response_header = ResponseHeader.from_request_header(request.header)
        cls._logger.debug(f"Created response header with correlation ID: {response_header.correlation_id}")
        
        response_class : type[AbstractResponse]

        match response_header.api_key:
            case ApiKey.API_VERSIONS:
                response_class = ApiVersionsResponse
                cls._logger.debug("Creating API_VERSIONS response")
            case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                response_class = DescribeTopicPartitionsResponse
                cls._logger.debug("Creating DESCRIBE_TOPIC_PARTITIONS response")
            case ApiKey.FETCH:
                response_class = FetchResponse
                cls._logger.debug("Creating FETCH response")
            case _:
                cls._logger.warning(f"Unknown API key for response: {response_header.api_key}")
                raise ValueError(f"Unknown API key for response: {response_header.api_key}")
        
        cls._logger.debug(f"Building body kwargs for {response_class.__name__}")
        body_kwargs = response_class.make_body_kwargs(request)
        cls._logger.debug(f"Response body kwargs created successfully")
        
        response = response_class(response_header, **body_kwargs)
        cls._logger.debug(f"Response object created successfully")
        
        return response
