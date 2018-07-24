use types::{ColumnType, ColumnValue};
use std::str;
use std::io::{Read, Write};
use std::cmp::{min};
use v2::schema2::{Schema, Schema2};
use v2::buf::{ReadBuf, AppendBuf};
use v2::adlerbuf::{ReadBufAdler32, AppendBufAdler32};

use std::fs::{File, OpenOptions};
use v2::filebuf::{FileBuf, ReadFileBuf};
use v2::vecbuf::Vecbuf;

extern crate lz4;
extern crate zstd;

fn read_varint<B: ReadBuf>(b: &mut B) -> usize {
    let mut bits: usize = 0;
    let mut r : usize = 0;
    loop {
        let u = read_db(b);
        let v = (u & 0x7f) as usize;
        r |= v << bits;
        bits += 7;
        if u & 128 == 0 {
            break
        }
    }
    r
}

// writes at least a byte
fn write_varint<B: AppendBuf>(b: &mut B, v: usize) {
    let mut r = v;
    loop {
        let mut x7 = (r & 0x7f) as u8;
        r = r >> 7;
        if r != 0 {
            x7 |= 0x80 as u8;
        }
        write_db(b, x7);
        if r == 0 {
            break;
        }
    }
}

fn read_varstring<B: ReadBuf>(b: &mut B) -> Option<String> {
    let co = read_db(b);
    if co == 0 as u8 {
        let size = read_varint(b);
        let mut bytes = Vec::new();
        for i in 0..size {
            let byte = read_db(b);
            bytes.push(byte);
        }
        // convert bytes to string
        let s = str::from_utf8(bytes.as_slice()).unwrap();
        Some(s.to_string()) // TBD
    } else if co == 'Z' as u8 {
        let size = read_varint(b);
        let mut bytes = Vec::new();
        for i in 0..size {
            let byte = read_db(b);
            bytes.push(byte);
        }
        let mut d = zstd::Decoder::new(bytes.as_slice()).unwrap();
        let mut dbuf: Vec<u8> = Vec::new();
        let r = d.read_to_end(&mut dbuf);
        r.unwrap();
        d.finish();
        let s = str::from_utf8(&dbuf).unwrap();
        Some(s.to_string())
    } else if co == 'L' as u8 {
        let size = read_varint(b);
//        if !check(b, size) {
//            return None;
//        }
        let mut bytes = Vec::new();
        for i in 0..size {
            let byte = read_db(b);
            bytes.push(byte);
        }
        let mut d = lz4::Decoder::new(bytes.as_slice()).unwrap();
        let mut dbuf: Vec<u8> = Vec::new();
        let r = d.read_to_end(&mut dbuf);
        r.unwrap();
        d.finish();
        let s = str::from_utf8(&dbuf).unwrap();
        Some(s.to_string())
    } else {
        panic!("unknown compression type {}", co);
    }
}

// write variable sized string
fn write_varstring<B: AppendBuf>(b: &mut B, s: &str) {
    let buf = Vec::new();
    let mut compression = 0 as u8;

    // try compressing the string
    let outbuf = if s.len() < 4096 { // use lz4
        let mut co = lz4::EncoderBuilder::new()
            .checksum(lz4::ContentChecksum::NoChecksum)
            .block_size(lz4::BlockSize::Default)
            .block_mode(lz4::BlockMode::Linked)
            .build(buf)
            .unwrap();
        let wres = co.write(s.as_bytes());
        wres.expect("co.write");
        let (outbuf, fres) = co.finish();
        fres.expect("co.finish");
        compression = 'L' as u8;
        outbuf
    } else { // use zstd
        let level = 5;
        let mut encoder = zstd::stream::Encoder::new(buf, level).unwrap();
        let wres = encoder.write(s.as_bytes());
        wres.expect("zstd.write");
        let outbuf = encoder.finish().unwrap();
        compression = 'Z' as u8;
        outbuf
    };

    if outbuf.len() < s.as_bytes().len() {
        write_db(b, compression); // lz4/zstd mark
        write_varint(b, outbuf.len());
        for c in outbuf.as_slice() {
            write_db(b, *c);
        }
    } else {
        write_db(b, 0 as u8); // no compression
        write_varint(b, s.len());
        for c in s.as_bytes() {
            write_db(b, *c);
        }
    }
}

fn flush_buf<B: AppendBuf>(b: &mut B) {
    b.flush();
}

fn write_db<B: AppendBuf>(b: &mut B, v: u8) {
    b.writeb(v);
}

fn write_dw_le<B: AppendBuf>(b: &mut B, v: u16) {
    let b0 = (v & 0xff) as u8;
    let b1 = (v >> 8) as u8;
    b.writeb(b0);
    b.writeb(b1);
}

fn write_dd_le<B: AppendBuf>(b: &mut B, v: u32) {
    b.writeb((v & 0xff) as u8);
    b.writeb((v >> 8) as u8);
    b.writeb((v >> 16) as u8);
    b.writeb((v >> 24) as u8);
}

fn write_dq_le<B: AppendBuf>(b: &mut B, v: u64) {
    b.writeb(v as u8);
    b.writeb((v >> 8) as u8);
    b.writeb((v >> 16) as u8);
    b.writeb((v >> 24) as u8);
    b.writeb((v >> 32) as u8);
    b.writeb((v >> 40) as u8);
    b.writeb((v >> 48) as u8);
    b.writeb((v >> 56) as u8);
}

//fn check<B: Buf>(b: &B, len: usize) -> bool {
//    b.check(len)
//}

fn read_db<B: ReadBuf>(b: &mut B) -> u8 {
    b.readb()
}

fn read_dw_le<B: ReadBuf>(b: &mut B) -> u16 {
    let b0 = b.readb();
    let b1 = b.readb();
    (b0 as u16) | ((b1 as u16) << 8)
}

fn read_dd_le<B: ReadBuf>(b: &mut B) -> u32 {
    let b0 = b.readb();
    let b1 = b.readb();
    let b2 = b.readb();
    let b3 = b.readb();
    (b0 as u32) |
    (b1 as u32) << 8 |
    (b2 as u32) << 16 |
    (b3 as u32) << 24
}

fn read_dq_le<B: ReadBuf>(b: &mut B) -> u64 {
    let b0 = b.readb();
    let b1 = b.readb();
    let b2 = b.readb();
    let b3 = b.readb();
    let b4 = b.readb();
    let b5 = b.readb();
    let b6 = b.readb();
    let b7 = b.readb();
    (b0 as u64) |
    (b1 as u64) << 8 |
    (b2 as u64) << 16 |
    (b3 as u64) << 24 |
    (b4 as u64) << 32 |
    (b5 as u64) << 40 |
    (b6 as u64) << 48 |
    (b7 as u64) << 56
}

pub fn read_schema_v2<B: ReadBuf>(
  buf: &mut B,
  ) -> Option<Schema2> {
    let version = read_db(buf);
    if version == '2' as u8 {
        let mut schema = Schema2::new();
        let num_columns = read_varint(buf);
        for i in 0..num_columns {
            let vs = read_varstring(buf);
            let s = match vs {
                Some(x) => x,
                None => return None
            };
            let ct = read_db(buf);
            let n = read_db(buf);
            let ctype = match ct {
                b'4' => ColumnType::U32le,
                b'8' => ColumnType::U64le,
                b'S' => ColumnType::String,
                _ => {
                    return None; // ERROR TBD
                }
            };
            let nullable = n == 'N' as u8;
            schema.add(s.as_str(), ctype, nullable);
        }
        return Some(schema)
    }
    None
}

pub fn write_schema_v2<B: AppendBuf>(
  buf: &mut B,
  schema: &Schema2,
  ) {
    write_db(buf, '2' as u8); // version 2
    write_varint(buf, schema.names.len());

    for colidx in 0..schema.names.len() {
        // column name
        write_varstring(buf, schema.names[colidx].as_str());

        let ct = match schema.types[colidx] {
            ColumnType::U32le => '4' as u8,
            ColumnType::U64le => '8' as u8,
            ColumnType::String => 'S' as u8,
        };
        write_db(buf, ct);
        if schema.nullable[colidx] {
            write_db(buf, 'N' as u8);
        } else {
            write_db(buf, 0 as u8);
        }
    }
}


pub fn schema_write<B: AppendBuf>(
    mut buf: &mut B,
    values: &[ColumnValue],
    schema: &Schema2,
) -> bool {
    assert!(values.len() == schema.types.len());

    for i in 0..values.len() {
        if !schema.nullable[i] && values[i] == ColumnValue::Null {
            return false;
        }
        match (schema.types[i], &values[i]) {
            (ColumnType::U32le, &ColumnValue::U32 { v }) => {}
            (ColumnType::U32le, &ColumnValue::Null) => {},
            (ColumnType::U64le, &ColumnValue::U64 { v }) => {}
            (ColumnType::U64le, &ColumnValue::Null) => {},
            (ColumnType::String, &ColumnValue::String { ref v }) => {}
            (ColumnType::String, &ColumnValue::Null) => {},
             _ => {
                 return false
             }
        }
    }

    schema_write_row::<B>(&mut buf, &values);
    true
}

pub fn schema_read_row<B: ReadBuf>(
    mut buf: &mut B,
    values: &mut [ColumnValue],
    schema: &Schema2,
) -> bool {
    // read null bytes
    let modulo = schema.len() % 8;
    let aligned_len = schema.len() + if modulo > 0 { 8 - modulo } else { 0 };
    for i in 0..(aligned_len/8) {
        let hash = {
            // start checksum
            let mut adlerbuf = ReadBufAdler32::<B>::new(&mut buf);
//            if !check(buf, 1) {
//                return false;
//            }
            // read null byte giving the null state of next 8 values
            let b = read_db(&mut adlerbuf);
            // number of column remaining (0..8)
            let jmax = min(8, schema.len() - i * 8);

            for j in 0..jmax {
                let bit = 1 << j;
                if b & bit != 0 { // null bit set
                    values[i * 8 + j] = ColumnValue::Null;
                } else {
                    match schema.ctype(i * 8 + j) {
                        ColumnType::U32le => {
                            let v = read_dd_le(&mut adlerbuf);
                            values[i * 8 + j] = ColumnValue::U32 { v: v};
                        },
                        ColumnType::U64le => {
                            let v = read_dq_le(&mut adlerbuf);
                            values[i * 8 + j] = ColumnValue::U64 { v: v};
                        },
                        ColumnType::String => {
                            let v = read_varstring(&mut adlerbuf);
                            match v {
                                Some(s) => {
                                    values[i * 8 + j] = ColumnValue::String { v: s };
                                }
                                None => {
                                    values[i * 8 + j] = ColumnValue::Null;
//                                    return false; TODO: why?
                                }
                            }
                        },
                    }
                }
            }
            adlerbuf.hash()
        };
        let fhash = read_dd_le::<B>(&mut buf);
        if hash != fhash {
            return false;
        }
    }
    true
}

fn schema_write_row<B: AppendBuf>(
    mut buf: &mut B,
    values: &[ColumnValue],
) {
    for i in 0..(values.len() + 7)/8 {
        let hash = {
            let mut adlerbuf = AppendBufAdler32::<B>::new(&mut buf);

            let jmax = min(8, values.len() - i * 8);

            // write the nullbyte for 8 next values
            let mut nullbyte = 0 as u8;
            for j in 0..jmax {
                if values[i * 8 + j] == ColumnValue::Null {
                    nullbyte |= (1 << j) as u8;
                }
            }
            write_db(&mut adlerbuf, nullbyte);

            for j in 0..jmax {
                match &values[i * 8 + j] {
                    &ColumnValue::Null => {
                        // taken care of by the null bytes
                    },
                    &ColumnValue::U32 { v } => {
                        write_dd_le(&mut adlerbuf, v);
                    },
                    &ColumnValue::U64 { v } => {
                        write_dq_le(&mut adlerbuf, v);
                    },
                    &ColumnValue::String { ref v } => {
                        write_varstring(&mut adlerbuf, v);
                    },
                }
            }
            adlerbuf.hash()
        };
        write_dd_le(buf, hash);
    }
}

#[test]
fn test_overflow() {
    let mut sb = Vecbuf::new(8);
    for n in 0..sb.len() {
        sb.writeb(1 as u8);
    }
    sb.writeb(1 as u8);
//    assert!(sb.is_overflow());
}

#[test]
fn test_varint() {
    let mut sb = Vecbuf::new(128);
    {
        sb.seek(0);
        let u: usize = 0x12;
        write_varint(&mut sb, u);
        sb.seek(0);
        let v: usize = read_varint(&mut sb);
        assert!(u == v);
    }
    {
        sb.seek(0);
        let u: usize = 0x80;
        write_varint(&mut sb, u);
        sb.seek(0);
        let v: usize = read_varint(&mut sb);
        assert!(u == v);
    }
    {
        sb.seek(0);
        let u: usize = 0xFF;
        write_varint(&mut sb, u);
        sb.seek(0);
        let v: usize = read_varint(&mut sb);
        assert!(u == v);
    }
    {
        sb.seek(0);
        let u: usize = 0x17f;
        write_varint(&mut sb, u);
        sb.seek(0);
        let v: usize = read_varint(&mut sb);
        assert!(u == v);
    }
    {
        sb.seek(0);
        let u: u16 = 0x55AA;
        write_dw_le(&mut sb, u);
        sb.seek(0);
        let v: u16 = read_dw_le(&mut sb);
        assert!(u == v);
    }
    {
        sb.seek(0);
        let u: u32 = 0x55AA99CC;
        write_dd_le(&mut sb, u);
        sb.seek(0);
        let v: u32 = read_dd_le(&mut sb);
        assert!(u == v);
    }
    {
        sb.seek(0);
        let u: u64 = 0x55AA99EE;
        write_dq_le(&mut sb, u);
        sb.seek(0);
        let v: u64 = read_dq_le(&mut sb);
        assert!(u == v);
    }
}

#[test]
fn test_varstring() {
    let mut sb = Vecbuf::new(1024);
    {
        sb.seek(0);
        let u = "hello_world";
        write_varstring(&mut sb, u);
        sb.seek(0);
        let v = read_varstring(&mut sb);
        assert!(u == v.unwrap());
    }
}

#[test]
fn test_schema_rw() {
    let mut s = Schema2::new();
    s.add("first",
          ColumnType::U32le,
          false);
    s.add("second",
          ColumnType::U64le,
          true);
    s.add("third",
          ColumnType::String,
          true);
    s.add("fourth",
          ColumnType::String,
          false);

    let mut vb = Vecbuf::new(1024);
    write_schema_v2(&mut vb, &s);

    vb.seek(0);
    let mut sch = read_schema_v2(&mut vb).unwrap();

    for n in 0..3 {
        assert!(s.name(n) == sch.name(n));
        assert!(s.ctype(n) == sch.ctype(n));
        assert!(s.nullable(n) == sch.nullable(n));
    }
    assert!(sch.name(0) == "first");
    assert!(sch.name(1) == "second");
}

#[test]
fn test_schema_write() {
    {
        let mut sch = Schema2::new();
        sch.add("first",
              ColumnType::U32le,
              false);
        sch.add("second",
              ColumnType::U64le,
              false);
        sch.add("third",
              ColumnType::String,
              false);
        sch.add("fourth",
              ColumnType::String,
              false);
        sch.add("fifth",
              ColumnType::String,
              true);

        let cv1 = ColumnValue::U32 { v: 0x12345678 };
        let cv2 = ColumnValue::U64 { v: 0x22334455 };
        let cv3 = ColumnValue::String { v: "a_string".to_string() };
        let cv4 = ColumnValue::String { v: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string() };
        let cv5 = ColumnValue::Null;
        let mut vbuf = Vecbuf::new(1024);
        let mut vec = Vec::new();
        vec.push(cv1);
        vec.push(cv2);
        vec.push(cv3);
        vec.push(cv4);
        vec.push(cv5);
        let wr = schema_write(&mut vbuf, vec.as_slice(), &sch);
        assert!(wr == true);

        vbuf.seek(0);

        let mut rvec = Vec::new();
        rvec.push(ColumnValue::Null);
        rvec.push(ColumnValue::Null);
        rvec.push(ColumnValue::Null);
        rvec.push(ColumnValue::Null);
        rvec.push(ColumnValue::Null);
        let rr = schema_read_row(&mut vbuf, rvec.as_mut_slice(), &sch);
        assert!(rr == true);

        assert!(vec[0] == rvec[0]);
        assert!(vec[1] == rvec[1]);
        assert!(vec[2] == rvec[2]);
        assert!(vec[3] == rvec[3]);
        assert!(vec[4] == rvec[4]);
    }
}

#[test]
fn test_string_rw() {
    let mut n = 1;
    while n <= 8192 {
        let x = (0..n).map(|_| "X").collect::<String>();
        let y = (0..n).map(|_| "Y").collect::<String>();
        {
            let mut f = File::create("/tmp/_string.dat").unwrap();
            let mut wf = FileBuf::new(f, 4);
            write_varstring(&mut wf, x.as_str());
            write_varstring(&mut wf, y.as_str());
        }
        {
            let mut f = File::open("/tmp/_string.dat").unwrap();
            let mut rf = ReadFileBuf::new(f, 4);
            let rx = read_varstring(&mut rf).unwrap();
            let ry = read_varstring(&mut rf).unwrap();
            assert!(rx == x);
            assert!(ry == y);
        }
        n += 1024;
    }
}
