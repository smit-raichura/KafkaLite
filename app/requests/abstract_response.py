from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..utils.converter import encode_int32
from .Headers.response_header import ResponseHeader
from ..utils.logger import get_logger

@dataclass
class AbstractResponse(ABC):
    '''
        Abstract base class for all response types
    '''
    header: ResponseHeader
    
    def __post_init__(self):
        # Initialize logger with class name
        self._logger = get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")
        self._logger.debug(f"Created {self.__class__.__name__} with header correlation ID: {self.header.correlation_id}")

    @classmethod
    @abstractmethod
    def make_body_kwargs(cls, request):
        # Note: Subclasses should log their specific body creation operations
        logger = get_logger(f"{cls.__module__}.{cls.__name__}")
        logger.debug(f"Making body kwargs for {cls.__name__} from request with correlation ID: {request.header.correlation_id}")
        raise NotImplementedError

    def encode(self):
        self._logger.debug(f"Encoding {self.__class__.__name__} response")
        encoded_header = self.header.encode()
        self._logger.debug(f"Encoded response header: {len(encoded_header)} bytes")
        
        encoded_body = self._encode_body()
        self._logger.debug(f"Encoded response body: {len(encoded_body)} bytes")
        
        encoded_response = encoded_header + encoded_body
        total_size = len(encoded_response)
        
        self._logger.debug(f"Total response size (without length prefix): {total_size} bytes")
        
        final_response = encode_int32(total_size) + encoded_response
        self._logger.debug(f"Final encoded response size: {len(final_response)} bytes")
        
        return final_response

    @abstractmethod
    def _encode_body(self):
        # Note: Subclasses should log their specific encoding operations
        self._logger.debug(f"Encoding body for {self.__class__.__name__}")
        raise NotImplementedError
