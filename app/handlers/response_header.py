from dataclasses import dataclass
from ..utils.constants import ApiKey
from .request_header import RequestHeader

@dataclass
class Responseheader:
    correlation_id: int

    @classmethod
    def from_request_header(cls, request_header: RequestHeader):
        return RequestHeader(request_header.correlation_id)
    
    def encode(self):
        return self.correlation_id.to_bytes(4)