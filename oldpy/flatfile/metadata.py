import struct
import lz4.frame
import brotli
import zlib

VALID_TYPES = ['u32le', 'u64le', 'u128le', 'string']
VALID_COMPRESSION = ['lz4', 'zlib', 'brotli', 'none']
VALID_CHECKSUM = ['crc32', 'adler32', 'none']

class Metadata:
    def __init__(self):
        self.checksum = None
        self.columns = []
        self.header_bytes = None

    def encode_header(self, hlist):
        h = 0
        for i, v in enumerate(hlist):
            if v is not None:
                h = h | (2 ** i)
        if self.header_bytes == 1:
            assert h >= 0 and h <= ((2 ** 8) - 1)
            return struct.pack('<B', h) # unsigned char
        elif self.header_bytes == 2:
            assert h >= 0 and h <= ((2 ** 16) - 1)
            return struct.pack('<H', h) # unsigned short
        elif self.header_bytes == 3 or self.header_bytes == 4:
            assert h >= 0 and h <= ((2 ** 32) - 1)
            return struct.pack('<I', h) # unsigned int
        elif self.header_bytes > 4 and self.header_bytes <= 8:
            assert h >= 0 and h <= ((2 ** 64) - 1)
            return struct.pack('<Q', h) # unsigned long long
        else:
            assert self.header_bytes > 0
            assert self.header_bytes <= 8

    def decode_header(self, f):
        if self.header_bytes == 1:
            b = f.read(1)
            if len(b) != 1:
                return None
            h, = struct.unpack('<B', b)
        elif self.header_bytes == 2:
            b = f.read(2)
            if len(b) != 2:
                return None
            h, = struct.unpack('<H', b)
        elif self.header_bytes == 3 or self.header_bytes == 4:
            b = f.read(4)
            if len(b) != 4:
                return None
            h, = struct.unpack('<I', b)
        elif self.header_bytes > 4 and self.header_bytes <= 8:
            b = f.read(8)
            if len(b) != 8:
                return None
            h, = struct.unpack('<Q', b)
        else:
            assert self.header_bytes > 0
            assert self.header_bytes <= 8
        hlist = []
        for n in range(0, len(self.columns)):
            if h & (2 ** n):
                hlist.append(True)
            else:
                hlist.append(False)
        return hlist, b

    def add_column(self, c):
        for x in self.columns:
            if c.name == x.name:
                raise Exception('Duplicate name {}'.format(c.name))
        self.columns.append(c)

    def set_checksum(self, checksum_type):
        if checksum_type in VALID_CHECKSUM:
            self.checksum = checksum_type
        else:
            print ('invalid checksum type', checksum_type, VALID_CHECKSUM)
        assert checksum_type in VALID_CHECKSUM

    def finalize(self, reorder):
        # if 'reorder' is True, put all nonstring columns
        # before the string columns, DEPRECATED
        if reorder is True:
            nonstrings = []
            strings = []
            for c in self.columns:
                if c.type_ == 'string':
                    strings.append(c)
                else:
                    nonstrings.append(c)
            strings.sort(key=lambda x: x.name)
            nonstrings.sort(key=lambda x: x.name)
            self.columns = nonstrings + strings

        self.header_bytes = int((len(self.columns) + 7) / 8)

        offset = 0
        for c in self.columns:
            c.offset = offset
            if c.type_ == 'u32le':
                offset += 4
            elif c.type_ == 'u64le':
                offset += 8
            elif c.type_ == 'u128le':
                offset += 16
            elif c.type_ == 'string':
                offset += 4
            else:
                assert not "unknown type"

    def write(self, kv, fobj):
        append_order = []
        append_values = {}
        header = []
        checksum = 0
        for c in self.columns:
            if c.name in kv:
                if c.type_ == 'string':
                    s = kv[c.name]
                    b = s.encode('utf-8')
                    if c.compression == 'lz4':
                        b = lz4.frame.compress(b)
                    elif c.compression == 'brotli':
                        b = brotli.compress(b)
                    elif c.compression == 'zlib':
                        b = zlib.compress(b, 9)
                    else:
                        print (c.compression)
                        assert c.compression is None
                    append_order.append(c.name)
                    append_values[c.name] = b
                    v = len(b)
                else:
                    v = kv[c.name]
                header.append(c.encode_uint(v))
            else:
                header.append(None)
        bits = self.encode_header(header)
        fobj.write(bits)
        checksum = zlib.adler32(bits, checksum)
        for h in header:
            if h is not None:
                fobj.write(h)
                checksum = zlib.adler32(h, checksum)
        for k in append_order:
            b = append_values[k]
            fobj.write(b)
            checksum = zlib.adler32(b, checksum)
        if self.checksum is not None:
            fobj.write(struct.pack('<I', checksum))
        for k in kv.keys():
            found = False
            for c in self.columns:
                if c.name == k:
                    found = True
                    break
            if not found:
                print('key not in metadata: "{}"'.format(k))
                print('known keys:')
                for c in self.columns:
                    print('"{}" -> {}'.format(c.name, c.name == k))
                print('-----------')
                raise Exception('key not in metadata: {}'.format(k))

    def read(self, f):
        checksum = 0
        h = self.decode_header(f)
        if h is None:
            return None
        hlist, b = h
        checksum = zlib.adler32(b, checksum)
        fixed_size = 0
        fixed_fmt = '<'
        for i, c in enumerate(self.columns):
            if hlist[i] is False:
                # skip this column
                pass
            elif c.type_ == 'u32le' or c.type_ == 'string':
                fixed_size += 4
                fixed_fmt += 'I'
            elif c.type_ == 'u64le':
                fixed_size += 8
                fixed_fmt += 'Q'
            elif c.type_ == 'u128le':
                fixed_size += 16
                # TODO
            else:
                raise Exception('unknown column type')
        buf = f.read(fixed_size)
        checksum = zlib.adler32(buf, checksum)
        values = struct.unpack(fixed_fmt, buf)
        r = {}
        readlist = []
        j = 0
        for i, c in enumerate(self.columns):
            if hlist[i] is False:
                pass
            elif c.type_ == 'u32le':
                r[c.name] = values[j]
                j += 1
            elif c.type_ == 'string':
                readlist.append((c.name, values[j], c.compression))
                j += 1
            elif c.type_ == 'u64le':
                r[c.name] = values[j]
                j += 1
            else:
                raise Exception('unk col type')
        for col_name, size, compression in readlist:
            b = f.read(size)
            checksum = zlib.adler32(b, checksum)
            if compression == 'lz4':
                b = lz4.frame.decompress(b)
            elif compression == 'brotli':
                b = brotli.decompress(b)
            elif compression == 'zlib':
                b = zlib.decompress(b)
            s = b.decode('utf-8')
            r[col_name] = s
        if self.checksum is not None:
            echecksum, = struct.unpack('<I', f.read(4))
            if checksum != echecksum:
                print ('CHECKSUM FAILED', checksum, echecksum)
            assert checksum == echecksum
        return r
