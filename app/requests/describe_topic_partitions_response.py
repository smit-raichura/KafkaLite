from typing import Dict, Any
from dataclasses import dataclass, field 
from uuid import UUID
from io import BytesIO

from .abstract_response import AbstractResponse
from ..utils.constants import ErrorCode, ApiKey
from .describe_topic_partitions_request import DescribeTopicPartitionsCursor
from ..metadata.cluster_metadata import ClusterMetadata
from .describe_topic_partitions_request import DescribeTopicPartitionsRequest

from ..utils.converter import (
    encode_boolean,
    encode_compact_array,
    encode_compact_nullable_string,
    encode_int32,
    encode_tagged_fields,
    encode_uuid,
)



@dataclass
class DescribeTopicPartitionsResponsePartition:
    error_code : ErrorCode
    partition_index : int = 0
    leader_id : int = 0
    leader_epoch: int
    replica_nodes: list[int] = field(default_factory=list) 
    isr_nodes : list[int] = field(default_factory=list)
    eligible_leader_replicas : list[int] = field(default_factory=list)
    last_known_elr : list[int] = field(default_factory=list)
    offline_replicas : list[int] = field(default_factory=list)

    def encode(self):
        partition_buffer = BytesIO()

        partition_buffer.write(self.error_code.encode()),
        partition_buffer.write(encode_int32(self.partition_index)),
        partition_buffer.write(encode_int32(self.leader_id)),
        partition_buffer.write(encode_int32(self.leader_epoch)),
        partition_buffer.write(encode_compact_array(self.replica_nodes, encode_int32)),
        partition_buffer.write(encode_compact_array(self.isr_nodes, encode_int32)),
        partition_buffer.write(encode_compact_array(self.eligible_leader_replicas, encode_int32)),
        partition_buffer.write(encode_compact_array(self.last_known_elr, encode_int32)),
        partition_buffer.write(encode_compact_array(self.offline_replicas, encode_int32)),
        partition_buffer.write(encode_tagged_fields)

        return partition_buffer.getvalue()


@dataclass
class DescribeTopicPartitionsResponseTopic:
    error_code : ErrorCode
    topic_name : str
    topic_id : UUID
    is_internal : bool
    # Use `field(default_factory=list)` to ensure each instance gets its own independent list,
    # avoiding shared mutable defaults that can lead to unintended side effects.
    partitions : list[DescribeTopicPartitionsResponsePartition] = field(default_factory=list) 
    topic_authorized_operations : int

    @classmethod
    def form_topic(cls, topic_name: str):
        cluster_metadata = ClusterMetadata()
        topic_id = cluster_metadata.get_topic_id(topic_name)

        if topic_id is None:
            topic_dict = {
                "error_code": ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                "topic_name": topic_name,
                "topic_id": UUID(int = 0),
                "is_internal": False,
                "partitions": [],
                "topic_authorized_operations": 0
            }
        
            return DescribeTopicPartitionsResponseTopic(**topic_dict)
        
        topic_dict = {
            "error_code": ErrorCode.NO_ERROR,
            "topic_name": topic_name,
            "topic_id": topic_id,
            "is_internal": False,
            "partitions": [DescribeTopicPartitionsResponsePartition( error_code= ErrorCode.NO_ERROR, partition_index= partition_index) for partition_index in cluster_metadata.get_partition_indices(topic_id)],
            "topic_authorized_operations": 0
        }

        return DescribeTopicPartitionsResponseTopic(**topic_dict)

    def encode(self):
        topic_buffer = BytesIO()
        topic_buffer.write(self.error_code.encode())
        topic_buffer.write(encode_compact_nullable_string(self.name))
        topic_buffer.write(encode_uuid(self.topic_id))
        topic_buffer.write(encode_boolean(self.is_internal))
        topic_buffer.write(encode_compact_array(self.partitions))
        topic_buffer.write(encode_int32(self.topic_authorized_operations))
        topic_buffer.write(encode_tagged_fields())

        return topic_buffer.getvalue()

@dataclass
class DescribeTopicPartitionsResponse(AbstractResponse):
    """Response class for Describe Topic Partitions."""
    '''
        throttle_time
        [topics]
        next_cursor
    '''
    throttle_time : int
    topics : list[DescribeTopicPartitionsResponseTopic]
    next_cursor : DescribeTopicPartitionsCursor | None


    @classmethod
    def make_body_kwargs(cls, request: DescribeTopicPartitionsRequest):
        return {
            "throttle_time": 0,
            "topics": [DescribeTopicPartitionsResponseTopic.form_topic(topic.topic_name) for topic in request.topics],
            "next_cursor": request.cursor,
        }

    def _encode_body(self):
        body_buffer= BytesIO()

        body_buffer.write(encode_int32(self.throttle_time)),
        body_buffer.write(encode_compact_array(self.topics)),
        body_buffer.write(b'\xff' if self.next_cursor is None else self.next_cursor.encode()),
        body_buffer.write(encode_tagged_fields())

        return body_buffer.getvalue()

    