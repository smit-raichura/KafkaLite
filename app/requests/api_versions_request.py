from typing import BinaryIO, List, Tuple, Dict, Any
from dataclasses import dataclass

from .abstract_request import AbstractRequest
from ..utils.constants import ErrorCode, ApiKey
from ..utils.converter import(
    decode_compact_string,
    decode_tagged_fields,
    encode_compact_string
)


@dataclass
class ApiVersionsRequest(AbstractRequest):
    """Request class for API Versions."""
    client_software_name: str
    client_software_version: str

    @classmethod
    def decode_body(cls, request_buffer: BinaryIO):
        body_kwargs = {
            "client_software_name": decode_compact_string(request_buffer),
            "client_software_version": decode_compact_string(request_buffer)
        }
        decode_tagged_fields(request_buffer)
        return body_kwargs
        

    def encode(self) -> bytes:
        raise NotImplementedError("Encoding not required for this request type.")