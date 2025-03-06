from .abstract_request import Request
from ..utils.constants import ErrorCode, ApiKey
from typing import List, Tuple, Dict, Any

class ApiVersionsRequest(Request):
    """Request class for API Versions."""
    @classmethod
    def decode(cls, request: bytes) -> Dict[str, Any]:
        client_id_string_len_bytes = request[12:14]
        client_id_string_len = int.from_bytes(client_id_string_len_bytes)
        return {
            "request_api_version_bytes": request[6:8],
            "api_key_bytes" : request[4:6],
            "correlation_id_bytes" : request[8:12],
            "client_id_string_len_bytes" : request[12:14],
            "clinet_id_bytes" : request[14: 14 + client_id_string_len]
        }

    def encode(self) -> bytes:
        raise NotImplementedError("Encoding not required for this request type.")