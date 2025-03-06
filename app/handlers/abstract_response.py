from abc import ABC, abstractmethod
from dataclasses import dataclass


class Response(ABC):
    '''
        Abstract base class for all response types
    '''

    @abstractmethod
    def make_response(self) -> bytes:
        '''
            Generate response in bytes
        '''
        raise NotImplementedError