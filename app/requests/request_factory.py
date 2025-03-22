from dataclasses import dataclass
from typing import BinaryIO
from ..utils.constants import ApiKey
from ..utils.logger import get_logger

from .Headers.request_header import RequestHeader
from .abstract_request import AbstractRequest
from .ApiVersions.api_versions_request import ApiVersionsRequest
from .DescribeTopicPartitions.describe_topic_partitions_request import DescribeTopicPartitionsRequest
from .Fetch.fetch_request import FetchRequest

from ..utils.converter import (
    decode_int32
)


class RequestFactory:
    """Factory to create request objects based on API key."""
    _logger = get_logger(__name__)
    
    @classmethod
    def read_request(cls, request_buffer: BinaryIO):
        cls._logger.debug("Reading request from buffer")
        mssg_size = decode_int32(request_buffer)
        cls._logger.debug(f"Message size: {mssg_size} bytes")

        request_header = RequestHeader.decode(request_buffer)
        cls._logger.debug(f"Decoded request header with API key: {request_header.api_key}")
        
        request_class : type[AbstractRequest]
        match request_header.api_key:
            case ApiKey.API_VERSIONS:
                request_class = ApiVersionsRequest
                cls._logger.debug("Processing API_VERSIONS request")
            case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                request_class = DescribeTopicPartitionsRequest
                cls._logger.debug("Processing DESCRIBE_TOPIC_PARTITIONS request")
            case ApiKey.FETCH:
                request_class = FetchRequest
                cls._logger.debug("Processing FETCH request")
            case _:
                cls._logger.warning(f"Unknown API key: {request_header.api_key}")
                raise ValueError(f"Unknown API key: {request_header.api_key}")

        cls._logger.debug(f"Decoding request body for {request_class.__name__}")
        body_kwargs = request_class.decode_body(request_buffer)
        cls._logger.debug(f"Request body decoded successfully")
        
        return request_class(request_header, **body_kwargs)
