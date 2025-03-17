from dataclasses import dataclass
from typing import BinaryIO
from ..utils.converter import decode_varint, encode_varint
from io import BytesIO

@dataclass
class RecordHeader:
    key: str
    value: bytes

    @classmethod
    def decode(cls, buffer: BinaryIO):
        key_length = decode_varint(buffer)
        key = buffer.read(key_length).decode('utf-8')
        value_length = decode_varint(buffer)
        value = buffer.read(value_length)
        return RecordHeader(key, value)
    
    def encode(self):
        record_header_buffer = BytesIO()
        record_header_buffer.write(encode_varint(len(self.key)))
        record_header_buffer.write(self.key.encode('utf-8'))
        record_header_buffer.write(encode_varint(len(self.value)))
        record_header_buffer.write(self.value)

        return record_header_buffer.getvalue()