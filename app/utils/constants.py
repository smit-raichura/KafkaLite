from enum import unique, IntEnum
from .converter import read_int, write_int
from typing import BinaryIO
@unique #this decorator ensures each enum has a unique value
class ErrorCode(IntEnum):
    NO_ERROR = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35

    @classmethod
    def decode(cls, buffer: BinaryIO):
        return ErrorCode(read_int(buffer= buffer, size= 2))

    def encode(self):
        return write_int(buffer= self, size= 2)

@unique
class ApiKey(IntEnum):
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75
     
    def decode(cls, buffer: BinaryIO):
        return ApiKey(read_int(buffer= buffer, size= 2))

    def encode(self):
        return write_int(buffer= self, size= 2)