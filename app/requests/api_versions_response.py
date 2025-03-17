
from typing import  Dict, Any
from dataclasses import dataclass, field
from io import BytesIO

from .abstract_response import AbstractResponse
from ..utils.constants import ErrorCode, ApiKey
from .api_versions_request import ApiVersionsRequest
from ..utils.converter import(
    encode_compact_array,
    encode_int16,
    encode_int32,
    encode_tagged_fields,
)

@dataclass
class ApiVersionsResponseApiKey:
    api_key: ApiKey
    min_version: int
    max_version: int

    def encode(self):
        buffer = BytesIO()
        buffer.write(self.api_key.encode())
        buffer.write(encode_int16(self.min_version))
        buffer.write(encode_int16(self.max_version))
        buffer.write(encode_tagged_fields())

        return buffer.getvalue()

@dataclass
class ApiVersionsResponse(AbstractResponse):
    error_code: ErrorCode = ErrorCode.NO_ERROR
    api_keys: list[ApiVersionsResponseApiKey] = field(default_factory=list)
    throttle_time: int = 0
    
    @classmethod
    def make_body_kwargs(cls, request: ApiVersionsRequest):
        if request.header.api_version in range(5):
            error_code = ErrorCode.NO_ERROR
        else:
            error_code = ErrorCode.UNSUPPORTED_VERSION

        return {
            "error_code": error_code,
            "api_keys": [
                ApiVersionsResponseApiKey(api_key= ApiKey.API_VERSIONS, min_version=0, max_version=4),
                ApiVersionsResponseApiKey(api_key= ApiKey.DESCRIBE_TOPIC_PARTITIONS, min_version=0, max_version=0),
            ],
            "throttle_time_ms": 0,
        }
 

    def _encode_body(self):
        body_buffer = BytesIO()
        body_buffer.write(self.error_code.encode())
        body_buffer.write(encode_compact_array(self.api_keys))
        body_buffer.write(encode_int32(self.throttle_time))
        body_buffer.write(encode_tagged_fields())

        return body_buffer.getvalue()
    