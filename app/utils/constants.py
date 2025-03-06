from enum import unique, IntEnum

@unique #this decorator ensures each enum has a unique value
class ErrorCode(IntEnum):
    NO_ERROR = 0
    UNKNOWN_TOPIC_OR_PARTITION = 3
    UNSUPPORTED_VERSION = 35

    

@unique
class ApiKey(IntEnum):
    API_VERSIONS = 18
    DESCRIBE_TOPIC_PARTITIONS = 75
     
    