from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, BinaryIO

from .Headers.request_header import RequestHeader

@dataclass
class AbstractRequest(ABC):
    '''
        Abstract base class for all request types.
    '''
    header: RequestHeader

    @classmethod
    @abstractmethod
    def decode_body(cls, request_buffer : BinaryIO) -> Dict[str, Any]:
        return NotImplementedError
    
    
    