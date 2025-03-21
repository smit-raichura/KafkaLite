from dataclasses import dataclass
from typing import BinaryIO
from io import BytesIO
from zlib import crc32

from ..utils.converter import (
    decode_array,
    decode_int8,
    decode_int16,
    decode_int32,
    decode_int64,
    decode_uint32,
    encode_array,
    encode_int8,
    encode_int16,
    encode_int32,
    encode_int64,
    encode_uint32,
    calculate_crc,
    encode_uint32_at
)

from .record import Record

@dataclass
class RecordBatch:
    base_offset: int # 8byte 
    batch_length: int # 4byte
    partition_leader_epoch: int # 4byte
    magic_byte: int # 1byte
    crc: int #4byte
    attributes: int # 2byte
    last_offset_data: int # 4byte
    base_timestamp: int #8byte
    max_timestamp: int #8byte
    producer_id: int #8byte
    producer_epoch: int #2byte
    base_sequence: int #4byte
    records: list[Record] 

    @classmethod
    def decode(cls, buffer: BinaryIO):
        batch_dict = {
            'base_offset': decode_int64(buffer),
            'batch_length': decode_int32(buffer),
            'partition_leader_epoch': decode_int32(buffer),
            'magic_byte': decode_int8(buffer),
            'crc': decode_uint32(buffer),
            'attributes': decode_int16(buffer),
            'last_offset_data': decode_int32(buffer),
            'base_timestamp': decode_int64(buffer),
            'max_timestamp': decode_int64(buffer),
            'producer_id': decode_int64(buffer),
            'producer_epoch': decode_int16(buffer),
            'base_sequence': decode_int32(buffer),
            'records': decode_array(buffer, Record.decode)
        }
    
        return RecordBatch(**batch_dict)
    
    def encode(self):
        batch_buffer = BytesIO()

        batch_buffer.write(encode_int64(self.base_offset))
        batch_buffer.write(encode_int32(0))
        batch_buffer.write(encode_int32(self.partition_leader_epoch))
        batch_buffer.write(encode_int8(self.magic_byte))

        crc_start_offset = batch_buffer.tell()
        batch_buffer.write(encode_int32(0))  # CRC placeholder

        batch_buffer.write(encode_int16(self.attributes))
        batch_buffer.write(encode_int32(self.last_offset_data))
        batch_buffer.write(encode_int64(self.base_timestamp))
        batch_buffer.write(encode_int64(self.max_timestamp))
        batch_buffer.write(encode_int64(self.producer_id))
        batch_buffer.write(encode_int16(self.producer_epoch))
        batch_buffer.write(encode_int32(self.base_sequence))
        batch_buffer.write(encode_array(self.records))

        # Get the complete buffer
        buffer = batch_buffer.getvalue()

        # Calculate and write batch length
        batch_length = len(buffer) - 12  # Exclude baseOffset (8 bytes) and batchLength (4 bytes)
        encode_uint32_at(buffer, 8, batch_length)

        
        
        # Calculate CRC
        crc_data = buffer[crc_start_offset + 4:]  # Skip the CRC placeholder
        crc_value = calculate_crc(crc_data)
        

        # Write CRC back to buffer
        encode_uint32_at(buffer, crc_start_offset, crc_value)
        
        
        return batch_buffer.getvalue()