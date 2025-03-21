from dataclasses import dataclass, field
from uuid import UUID
from io import BytesIO

from ..abstract_response import AbstractResponse
from ...utils.constants import ErrorCode
from ...metadata.record_batch import RecordBatch
from .fetch_request import FetchRequest, FetchRequestTopic
from ...metadata.cluster_metadata import ClusterMetadata, read_record_batches

from ...utils.converter import (
    encode_int32,
    encode_int64,
    encode_uuid,
    encode_tagged_fields,
    encode_compact_array,
)
@dataclass
class FetchResponseAbortedTransaction:
    producer_id: int # 64
    first_offset: int # 64

    def encode(self):
        # print(f'FetchResponseAbortedTransaction :: {self}')
        transaction_buffer = BytesIO()

        transaction_buffer.write(encode_int64(self.producer_id))
        transaction_buffer.write(encode_int64(self.first_offset))
        transaction_buffer.write(encode_tagged_fields())

        return transaction_buffer.getvalue()


@dataclass
class FetchResponsePartition:
    partition_index: int # 32
    error_code: ErrorCode
    high_watermark: int = 0# 64 
    last_stable_offset: int = 0# 64
    log_start_offset: int = 0# 64
    aborted_transactions: list[FetchResponseAbortedTransaction] = field(default_factory=list)
    preferred_read_replica: int = 0
    records: list[RecordBatch] = field(default_factory=list)

    def encode(self):
        # print(f'FetchResponsePartition :: {self}')
        partition_buffer = BytesIO()

        partition_buffer.write(encode_int32(self.partition_index))
        partition_buffer.write(self.error_code.encode())
        partition_buffer.write(encode_int64(self.high_watermark))
        partition_buffer.write(encode_int64(self.last_stable_offset))
        partition_buffer.write(encode_int64(self.log_start_offset))
        partition_buffer.write(encode_compact_array(self.aborted_transactions))
        partition_buffer.write(encode_int32(self.preferred_read_replica))
        partition_buffer.write(encode_compact_array(self.records))
        partition_buffer.write(encode_tagged_fields())

        return partition_buffer.getvalue()



@dataclass
class FetchResponseTopic:
    topic_id: UUID
    partitions: list[FetchResponsePartition]

    @classmethod
    def from_topic(cls, request_topic: FetchRequestTopic):
        cluster_metadata = ClusterMetadata()
        
        topic_name = cluster_metadata.get_topic_name(request_topic.topic_id)
        if topic_name is None:
            # print('No topic found')
            return FetchResponseTopic(
                topic_id= request_topic.topic_id,
                partitions=[
                    FetchResponsePartition(
                        partition_index= 0,
                        error_code=  ErrorCode.UNKNOWN_TOPIC_ID,
                    )
                ]
            )

        return FetchResponseTopic(
                topic_id= request_topic.topic_id,
                partitions=[
                    FetchResponsePartition(
                        partition_index= partition.partition,
                        error_code=  ErrorCode.NO_ERROR,
                        records= list(read_record_batches(topic_name= topic_name, partition_index= partition.partition))
                    )
                   for partition in request_topic.partitions
                ]
            )
    
    def encode(self):
        # print(f'FetchResponseTopic :: {self}')
        topic_buffer = BytesIO()

        topic_buffer.write(encode_uuid(self.topic_id))
        topic_buffer.write(encode_compact_array(self.partitions))
        topic_buffer.write(encode_tagged_fields())

        return topic_buffer.getvalue()


@dataclass
class FetchResponse(AbstractResponse):

    throttle_time: int # 32 
    error_code: ErrorCode
    session_id: int # 32
    responses: list[FetchResponseTopic]

    @classmethod
    def make_body_kwargs(cls, request: FetchRequest):
        body_dict = {
            "throttle_time": 0,
            "error_code": ErrorCode.NO_ERROR,
            "session_id": 0,
            "responses": [FetchResponseTopic.from_topic(topic) for topic in request.topics]
        }     
        return body_dict 
    
    def _encode_body(self):
        body_buffer = BytesIO()
        print(f'Cluster Metadata : {ClusterMetadata()}')
        print(f'Fetch Response Body : {self}')

        body_buffer.write(encode_int32(self.throttle_time))
        body_buffer.write(self.error_code.encode())
        body_buffer.write(encode_int32(self.session_id))
        body_buffer.write(encode_compact_array(self.responses))
        body_buffer.write(encode_tagged_fields())

        return body_buffer.getvalue() 
    