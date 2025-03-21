from dataclasses import dataclass
from typing import BinaryIO
from io import BytesIO

from ..utils.converter import (
    decode_compact_array,
    decode_compact_bytes,
    decode_int8,
    decode_varint,
    decode_varlong,
    encode_compact_array,
    encode_compact_bytes,
    encode_int8,
    encode_varint,
    encode_varlong,
)

from .record_header import RecordHeader

@dataclass
class Record:
    length: int
    attributes: int
    timestamp_delta: int
    offset_delta: int
    key: bytes
    value_length: int
    value: bytes
    headers: list[RecordHeader]

    @classmethod
    def decode(cls, buffer: BinaryIO):
        record_dict = {
            "length": decode_varint(buffer),
            "attributes": decode_int8(buffer),
            "timestamp_delta": decode_varlong(buffer),
            "offset_delta": decode_varint(buffer),
            "key": decode_compact_bytes(buffer),
            "value_length": decode_varint(buffer)
        }
        record_dict["value"] = buffer.read(record_dict["value_length"])
        record_dict["headers"] = decode_compact_array(buffer, RecordHeader.decode)
        
        return Record(**record_dict)

    def encode(self):
        record_buffer = BytesIO()
        print(f"RECORD - metadata : {self}")

        record_buffer.write(encode_varint(self.length))
        record_buffer.write(encode_int8(self.attributes))
        record_buffer.write(encode_varlong(self.timestamp_delta))
        record_buffer.write(encode_compact_bytes(self.key))
        record_buffer.write(encode_varint(self.value_length))
        record_buffer.write(self.value)
        record_buffer.write(encode_compact_array(self.headers))

        return record_buffer.getvalue()
