
from .abstract_response import Response
from ..utils.constants import ErrorCode, ApiKey
from typing import  Dict, Any


class ApiVersionsResponse(Response):
    
    def __init__(self, request_obj : Dict [str, Any]):
        self.request_obj = request_obj

    def make_response(self) -> bytes:
        header = self.request_obj['correlation_id_bytes']
        error_code = (
            ErrorCode.NO_ERROR
            if int.from_bytes(self.request_obj["request_api_version_bytes"]) in range(5)
            else ErrorCode.UNSUPPORTED_VERSION
        )
        error_code_bytes = error_code.value.to_bytes(2)
        tagged_fields_bytes = b'\x00'
        api_versions = [
            (ApiKey.API_VERSIONS.value, 0, 4, tagged_fields_bytes),
            (ApiKey.DESCRIBE_TOPIC_PARTITIONS.value, 0, 0, tagged_fields_bytes)
        ]

        api_versions_bytes = b''
        api_versions_length = len(api_versions) + 1
        api_versions_length_bytes = api_versions_length.to_bytes(1)
        for api_key, min_version, max_version, tag_buffer in api_versions:
            api_versions_bytes  += (
                api_key.to_bytes(2) +
                min_version.to_bytes(2) +
                max_version.to_bytes(2) +
                tag_buffer
            )

        throttle_time = 0
        throttle_time_bytes = throttle_time.to_bytes(4)

        body = error_code_bytes + api_versions_length_bytes + api_versions_bytes + throttle_time_bytes + tagged_fields_bytes
        message_len = len(header) + len(body)
        message = message_len.to_bytes(4) + header + body
        
        return message