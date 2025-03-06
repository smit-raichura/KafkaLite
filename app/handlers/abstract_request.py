from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict


class Request(ABC):
    '''
        Abstract base class for all request types.
    '''
    @classmethod
    @abstractmethod
    def decode(cls, request : bytes) -> Dict[str, Any]:
        return NotImplementedError
    
    @abstractmethod
    def encode(self) -> bytes:
        return NotImplementedError
    
    