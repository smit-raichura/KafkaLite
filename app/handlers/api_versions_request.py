from .abstract_request import Request
from ..utils.constants import ErrorCode, ApiKey
from typing import List, Tuple, Dict, Any

class ApiVersionsRequest(Request):
    """Request class for API Versions."""
    @classmethod
    def decode(cls, request: bytes) -> Dict[str, Any]:
        return {
            "request_api_version_bytes": request[4:8]
        }

    def encode(self) -> bytes:
        raise NotImplementedError("Encoding not required for this request type.")