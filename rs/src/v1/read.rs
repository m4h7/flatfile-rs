use types::{ChecksumType, CompressionType, ColumnValue, ColumnType, Column};
use std::io::{Read};
extern crate lz4;
extern crate brotli2;
use self::brotli2::read;
use v1::row::Row;

extern crate adler32;
use self::adler32::RollingAdler32;
use ::std::str;

pub fn schema_read<'a, R: Read>(
    columns: &'a [Column],
    reader: &mut R,
    header_bytes: usize,
    checksum: &ChecksumType) -> Option<Row<'a>> {

    let mut adler = RollingAdler32::from_value(0);

    // read header_bits
    let header_bits = if header_bytes == 1 {
        let mut b = [0; 1];
        let r = reader.read(&mut b);
        r.expect("read h 1");
        adler.update_buffer(&b);
        b[0] as usize
    } else if header_bytes == 2 {
        let mut b = [0; 2];
        let r = reader.read(&mut b);
        r.expect("read h 2");
        adler.update_buffer(&b);
        (b[0] as usize) | ((b[1] as usize) << 8)
    } else if header_bytes == 3 || header_bytes == 4 {
        let mut b = [0; 4];
        let r = reader.read(&mut b);
        r.expect("read h 3/4");
        adler.update_buffer(&b);
        (b[0] as usize) | (b[1] as usize) << 8 | (b[2] as usize) << 16 | (b[3] as usize) << 24
    } else if header_bytes > 4 && header_bytes <= 8 {
        let mut b = [0; 8];
        let r = reader.read(&mut b);
        r.expect("read h 4-8");
        adler.update_buffer(&b);
        (b[0] as usize) | (b[1] as usize) << 8 | (b[2] as usize) << 16 |
        (b[3] as usize) << 24 | (b[4] as usize) << 32 | (b[5] as usize) << 40 |
        (b[6] as usize) << 48 | (b[7] as usize) << 56
    } else {
        // unknown header_bytes size
        0
    };

    let mut blobs: Vec<(usize, usize, CompressionType)> = Vec::with_capacity(columns
            .len()); // count only strings?
    let mut result = Row::new(&columns);

    for (colidx, c) in columns.iter().enumerate() {
        if header_bits & (1 << colidx) != 0 {
            // column is present
            match c.ctype {
                ColumnType::U32le => {
                    let mut buf = [0; 4];
                    let r = reader.read(&mut buf);
                    r.expect("read u32");
                    adler.update_buffer(&buf);
                    let v = (buf[0] as usize) | (buf[1] as usize) << 8 |
                            (buf[2] as usize) << 16 |
                            (buf[3] as usize) << 24;
                    result.push(colidx, ColumnValue::U32 { v: v as u32 });
                }
                ColumnType::U64le => {
                    let mut buf = [0; 8];
                    let r = reader.read(&mut buf);
                    r.expect("read u64");
                    adler.update_buffer(&buf);
                    let v = (buf[0] as usize) | (buf[1] as usize) << 8 |
                            (buf[2] as usize) << 16 |
                            (buf[3] as usize) << 24 |
                            (buf[4] as usize) << 32 |
                            (buf[5] as usize) << 40 |
                            (buf[6] as usize) << 48 |
                            (buf[7] as usize) << 56;
                    result.push(colidx, ColumnValue::U64 { v: v as u64 });
                }
                ColumnType::String => {
                    let mut buf = [0; 4];
                    let r = reader.read(&mut buf);
                    r.expect("read s");
                    adler.update_buffer(&buf);
                    let size = (buf[0] as usize) | (buf[1] as usize) << 8 |
                               (buf[2] as usize) << 16 |
                               (buf[3] as usize) << 24;
                    blobs.push((colidx, size, c.compression));
                }
                _ => {
                    println!("unknown column type");
                }
            }
        }
    }

    for &(colidx, size, ref compression) in blobs.iter() {
        let mut buf: Vec<u8> = vec![0; size]; // Vec::with_capacity(size);
        let r = reader.read(&mut buf);
        r.expect("r buf");
        adler.update_buffer(&buf);
        let s = match compression {
            &CompressionType::Brotli => {
                let u: &[u8] = &buf;
                let mut d = read::BrotliDecoder::new(u);
                let mut dbuf: Vec<u8> = Vec::new();
                let r = d.read_to_end(&mut dbuf);
                let s = str::from_utf8(&dbuf).unwrap();
                s.to_string()
            }
            &CompressionType::Lz4 => {
                // lz4 'block' container compression
                let u: &[u8] = &buf;
                let mut d = lz4::Decoder::new(u).unwrap();
                let mut dbuf: Vec<u8> = Vec::new();
                let r = d.read_to_end(&mut dbuf);
                r.unwrap();
                d.finish();
                let s = str::from_utf8(&dbuf).unwrap();
                s.to_string()
            }
            &CompressionType::Zlib => {
                let s = str::from_utf8(&buf).unwrap();
                s.to_string()
            }
            &CompressionType::None => {
                let s = str::from_utf8(&buf).unwrap();
                s.to_string()
            }
        };
        result.push(colidx, ColumnValue::String { v: s });
    }

    match *checksum {
        ChecksumType::Adler32 => {
            let mut buf = [0; 4];
            let r = reader.read(&mut buf);
            r.expect("read ch");
            let hash = buf[0] as u32 |
                       ((buf[1] as u32) << 8) |
                       ((buf[2] as u32) << 16) |
                       ((buf[3] as u32) << 24);
            let expected = adler.hash();
            if hash != expected {
                println!("incorrect checksum got={} exp={}", hash, expected);
                return None;
            }
        }
        ChecksumType::None => {
        }
    }

    Some(result)
}
