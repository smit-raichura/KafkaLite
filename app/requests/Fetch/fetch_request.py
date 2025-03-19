from dataclasses import dataclass
from typing import Any, Dict, BinaryIO
from uuid import UUID

from ..abstract_request import AbstractRequest
from ...utils.converter import(
    decode_compact_array,
    decode_compact_string,
    decode_int64, 
    decode_int8,
    decode_int32, 
    decode_tagged_fields, 
    decode_uuid,
)


@dataclass
class FetchRequestPartition:
    partition: int # 32
    current_leader_epoch: int # 32
    fetch_offset: int # 64
    last_fetched_epoch: int # 32
    log_start_offset: int # 64
    partition_max_bytes: int #32

    @classmethod
    def decode(cls, buffer: BinaryIO):
        request_part_dict = {
            "partition": decode_int32(buffer),
            "current_leader_epoch": decode_int32(buffer),
            "fetch_offset": decode_int64(buffer),
            "last_fetched_epoch": decode_int32(buffer),
            "log_start_offset": decode_int64(buffer),
            "partition_max_bytes": decode_int32(buffer),
        }
        decode_tagged_fields(buffer)
        return FetchRequestPartition(**request_part_dict)


@dataclass
class FetchRequestTopic:
    topic_id: UUID
    partitions: list[FetchRequestPartition]

    @classmethod
    def decode(cls, buffer: BinaryIO):
        topic_dict = {
            "topic_id": decode_uuid(buffer),
            "partitions": decode_compact_array(buffer, FetchRequestPartition.decode)
        }
        decode_tagged_fields(buffer)
        return FetchRequestTopic(**topic_dict)


@dataclass
class FetchRequestForgottenTopicData:
    topic_id: UUID
    partitions: list[int] # 32

    @classmethod
    def decode(cls, buffer: BinaryIO):
        forgotten_topic_data = FetchRequestForgottenTopicData(
            topic_id= decode_uuid(buffer),
            partitions= decode_compact_array(buffer, decode_int32)
        )
        decode_tagged_fields(buffer)

        return forgotten_topic_data


@dataclass
class FetchRequest(AbstractRequest):
    max_wait: int # 32
    min_bytes: int # 32
    max_bytes: int #32
    isolation_level: int # 8
    session_id: int # 32
    session_epoch: int # 32
    topics: list[FetchRequestTopic]
    forgotten_topics_data: list[FetchRequestForgottenTopicData]
    rack_id: str # compact str


    @classmethod
    def decode_body(cls, request_buffer: BinaryIO):
        request_body = {
            "max_wait": decode_int32(request_buffer),
            "min_bytes": decode_int32(request_buffer),
            "max_bytes": decode_int32(request_buffer),
            "isolation_level": decode_int8(request_buffer),
            "session_id": decode_int32(request_buffer),
            "session_epoch": decode_int32(request_buffer),
            "topics": decode_compact_array(request_buffer, FetchRequestTopic.decode),
            "forgotten_topics_data": decode_compact_array(request_buffer, FetchRequestForgottenTopicData.decode),
            "rack_id": decode_compact_string(request_buffer)
        }
        decode_tagged_fields(request_buffer)
        print(f' FEtch Request Body  ::  {request_body}')
        return request_body