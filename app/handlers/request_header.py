from dataclasses import dataclass
from ..utils.constants import ApiKey


@dataclass
class RequestHeader:
    api_key: ApiKey
    api_version: int
    correlation_id: int

    @classmethod
    def decode(cls, header_bytes):
        api_key= ApiKey.decode(header_bytes[4:6])
        api_version = int.from_bytes(header_bytes[6:8])
        correlation_id = int.from_bytes(header_bytes[8:12])
        
        return RequestHeader(api_key,api_version,correlation_id)

