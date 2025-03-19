from threading import Lock
import uuid
from collections import defaultdict
from io import open, BytesIO
from uuid import UUID

from ..utils.converter import (
    decode_compact_string,
    decode_int8,
    decode_int32,
    decode_uuid
)

from .record_batch import RecordBatch
from .record import Record
from .record_type import RecordType

class SingletonMetaData:
    '''
        A singleton class for central use of the whole application
    '''
    _instances = {}
    _lock = Lock()

    def __new__(cls, *args, **kwargs):
    
        with cls._lock:
            if cls not in cls._instances:
                cls._instances[cls] = super(SingletonMetaData, cls).__new__(cls)
        return cls._instances[cls]

class ClusterMetadata(SingletonMetaData):
     
    # metadata sample ref: https://binspec.org/kafka-cluster-metadata

    def __init__(self):
        self._topic_id_lookup : dict[str, uuid.UUID] = {}
        self._topic_name_lookup: dict[uuid.UUID, str] = {}
        self._partition_indices_lookup: defaultdict[uuid.UUID, list[int]] = defaultdict(list)

        for record_batch in read_record_batches(topic_name= "__cluster_metadata", partition_index= 0):
            for record in record_batch.records:
                self._add_record(record)

    def __repr__(self):
        return f"id_lookup : {self._topic_id_lookup} \n name_lookup : {self._topic_name_lookup} \n partition_lookup : {self._partition_indices_lookup}"

    def get_topic_id(self, topic_name: str):
        return self._topic_id_lookup[topic_name] if topic_name in self._topic_id_lookup else None

    def get_topic_name(self, topic_id: UUID):
        return self._topic_name_lookup[topic_id] if topic_id in self._topic_name_lookup else None
    
    def get_partition_indices(self, topic_id: UUID):
        return self._partition_indices_lookup[topic_id] if topic_id in self._partition_indices_lookup else None
    
    def _add_record(self, record: Record):
        
        value_buffer = BytesIO(record.value)
        decode_int8(value_buffer) #parsing frame_version ref: https://binspec.org/kafka-cluster-metadata?highlight=67-67

        # match type of record
        record_type = decode_int8(value_buffer)

        match record_type:
            case RecordType.TOPIC:
                decode_int8(value_buffer) # parse version ref: https://binspec.org/kafka-cluster-metadata?highlight=69-69
                topic_name = decode_compact_string(value_buffer)
                topic_id = decode_uuid(value_buffer)

                self._topic_id_lookup[topic_name] = topic_id
                self._topic_id_lookup[topic_id] = topic_name

            case RecordType.PARTITION:
                decode_int8(value_buffer) # parse version ref: https://binspec.org/kafka-cluster-metadata?highlight=69-69
                partition_index = decode_int32(value_buffer)
                topic_id = decode_uuid(value_buffer)

                self._partition_indices_lookup[topic_id].append(partition_index)

def read_record_batches(topic_name: str, partition_index: int):
    print(f'topic_name : {topic_name}  ----  partition_index : {partition_index}')
    filepath = f'/tmp/kraft-combined-logs/{topic_name}-{partition_index}/00000000000000000000.log'
    with open(filepath, mode = 'rb') as file_buffer:
        while file_buffer.peek():
            yield RecordBatch.decode(file_buffer)