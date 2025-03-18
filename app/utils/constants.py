from enum import unique, IntEnum
from .converter import decode_int16, encode_int16
from typing import BinaryIO
@unique #this decorator ensures each enum has a unique value
class ErrorCode(IntEnum):
    NO_ERROR = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35

    @classmethod
    def decode(cls, buffer: BinaryIO):
        return ErrorCode(decode_int16(buffer))

    def encode(self):
        return encode_int16(self)
    

@unique
class ApiKey(IntEnum):
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75
    FETCH = 1
     
    @classmethod
    def decode(cls, buffer: BinaryIO):
        return ApiKey(decode_int16(buffer))

    def encode(self):
        return encode_int16(self)
