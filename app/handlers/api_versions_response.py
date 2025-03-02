from dataclasses import dataclass
from .abstract_response_handler import AbstractResponse
from ..utils.constants import ErrorCode, ApiKey
from typing import List, Tuple

@dataclass
class ApiVersionsResponse(AbstractResponse):
    error_code: ErrorCode
    api_Keys: List[Tuple[int, int, int]] = None
    throttle_time_ms: int = 0

    def make_response(self, request):
        