from dataclasses import dataclass
from typing import BinaryIO

from ...utils.constants import ApiKey
from ...utils.converter import (
    decode_int16,
    decode_int32,
    decode_nullable_string,
    decode_tagged_fields,
)

@dataclass
class RequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int
    client_id: str

    @classmethod
    def decode(cls, buffer: BinaryIO):
        request_header = RequestHeader(
            api_key=ApiKey.decode(buffer),
            api_version=decode_int16(buffer),
            correlation_id=decode_int32(buffer),
            client_id=decode_nullable_string(buffer),
        )
        decode_tagged_fields(buffer)
        return request_header