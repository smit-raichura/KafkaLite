from dataclasses import dataclass
from typing import BinaryIO
from ..utils.constants import ApiKey
from .request_header import RequestHeader
from .abstract_request import AbstractRequest
from .api_versions_request import ApiVersionsRequest
from .describe_topic_partitions_request import DescribeTopicPartitionsRequest
from ..utils.converter import (
    decode_int32
)

class RequestFactory:
    """Factory to create request objects based on API key."""
    @staticmethod
    def read_request(request_buffer: BinaryIO):
        mssg_size = decode_int32(request_buffer)

        request_header = RequestHeader.decode(request_buffer)
        request_class : type[AbstractRequest]
        match request_header.api_key:
            case ApiKey.API_VERSIONS:
                request_class = ApiVersionsRequest
            case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
                request_class = DescribeTopicPartitionsRequest
        request_body = request_class.decode_body(request_buffer)
        print(f'request_body : {request_body}' )
        # return request_class(**request_body)
        return request_class(request_header, **request_class.decode_body(request_buffer))
    
 

