from abc import ABC, abstractmethod
from dataclasses import dataclass

@dataclass
class AbstractResponse(ABC):

    @abstractmethod
    def make_response(self, request):
        raise NotImplementedError