from abc import ABC, abstractmethod
from dataclasses import dataclass

from ..utils.converter import encode_int32
from .Headers.response_header import ResponseHeader

@dataclass
class AbstractResponse(ABC):
    '''
        Abstract base class for all response types
    '''
    header: ResponseHeader

    @classmethod
    @abstractmethod
    def make_body_kwargs(cls, request):
        raise NotImplementedError

    def encode(self):
        encoded_response = self.header.encode() + self._encode_body()
        print(f'Encoded Response  ::  { encode_int32(len(encoded_response)) + encoded_response}')
        return encode_int32(len(encoded_response)) + encoded_response

    @abstractmethod
    def _encode_body(self):
        raise NotImplementedError