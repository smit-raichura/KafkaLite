from enum import unique, IntEnum

@unique #this decorator ensures each enum has a unique value
class ErrorCode(IntEnum):
    NONE = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35

    @classmethod
    def decode(cls, error_code_bytes):
        return ErrorCode(int.from_bytes(error_code_bytes, byteorder= 'big'))
    
    def encode(self):
        return self.to_bytes(2)

@unique
class ApiKey(IntEnum):
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75
     
    @classmethod
    def decode(cls, api_key_bytes):
        return ApiKey(int.from_bytes(api_key_bytes, byteorder= 'big'))

    def encode(self):
        return self.to_bytes(2)