from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, BinaryIO

from .Headers.request_header import RequestHeader
from ..utils.logger import get_logger

@dataclass
class AbstractRequest(ABC):
    '''
        Abstract base class for all request types.
    '''
    header: RequestHeader
    
    def __post_init__(self):
        # Initialize logger with class name
        self._logger = get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        self._logger.debug(f"Created {self.__class__.__name__} with header correlation ID: {self.header.correlation_id}")

    @classmethod
    @abstractmethod
    def decode_body(cls, request_buffer : BinaryIO):
        # Note: Subclasses should log their specific decoding operations
        logger = get_logger(f"{cls.__module__}.{cls.__name__}")
        logger.debug(f"Decoding request body for {cls.__name__}")
        return NotImplementedError
