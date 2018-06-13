import struct

class MetadataColumn:
    """One column with type and additional information"""
    def __init__(self, name, type_, meaning = None, compression = None):
        self.name = name
        self.type_ = type_
        self.meaning = meaning
        self.compression = compression
    def encode_uint(self, v):
        if self.type_ == 'u32le':
            return struct.pack('<I', v)
        elif self.type_ == 'u64le':
            return struct.pack('<Q', v)
        elif self.type_ == 'string':
            # u32 length
            return struct.pack('<I', v)
        else:
            raise Exception('unknown type passed to encode_uint')
