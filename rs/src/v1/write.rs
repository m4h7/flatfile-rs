extern crate lz4;
extern crate adler32;

use types::{Column, ColumnType, ColumnValue, ChecksumType, CompressionType};
use std::io::{Write};
use self::adler32::RollingAdler32;

pub fn schema_write<W: Write>(
    columns: &[Column],
    writer: &mut W,
    names: &[&str],
    values: &[ColumnValue],
    header_bytes: usize,
    checksum: &ChecksumType
    )
{
    // ordered[colidx] = valueindex|nameindex
    let oob = columns.len() + 1;
    let mut ordered: Vec<usize> = vec![oob; columns.len()];

    fn getindex(columns: &[Column], colname: &str) -> usize {
        for (i, col) in columns.iter().enumerate() {
            if col.name == colname {
                return i;
            }
        }
        panic!("getindex: column {} not found", colname);
    }

    for (colidx, name) in names.iter().enumerate() {
        let validx = getindex(columns, name);
        ordered[colidx] = validx;

        match columns[colidx].ctype {
            ColumnType::String => {
                match values[validx] {
                    ColumnValue::String { .. } => {}
                    ColumnValue::U32 { .. } => {
                        panic!("Column {} expected string received u32le", names[validx]);
                    }
                    ColumnValue::U64 { .. } => {
                        panic!("Column {} expected string received u64le", names[validx]);
                    }
                    ColumnValue::Null => {}
                }
            }
            ColumnType::U32le => {
                match values[validx] {
                    ColumnValue::String { .. } => {
                        panic!("Column {} expected u32le received string", names[validx]);
                    }
                    ColumnValue::U64 { .. } => {
                        panic!("Column {} expected u32le received u64le", names[validx]);
                    }
                    ColumnValue::U32 { .. } => {}
                    ColumnValue::Null => {}
                }
            }
            ColumnType::U64le => {
                match values[validx] {
                    ColumnValue::String { .. } => {
                        panic!("Column {} expected u64le received string", names[validx]);
                    }
                    ColumnValue::U32 { .. } => {
                        panic!("Column {} expected u63le received u32le", names[validx]);
                    }
                    ColumnValue::U64 { .. } => {}
                    ColumnValue::Null => {}
                }
            }
        }
    }

        let mut adler = RollingAdler32::from_value(0);

        let mut header_bits = 0;

        for colidx in 0..columns.len() {
            let validx = ordered[colidx];
            let val = &values[validx];
            match val {
                &ColumnValue::Null => {}
                _ => {
                    header_bits = header_bits | (1 << colidx);
                }
            }
        }

        match header_bytes {
            1 => {
                let mut b = [0; 1];
                b[0] = header_bits as u8;
                adler.update_buffer(&b);
                let r = writer.write(&b);
                r.expect("write h 1");
            }
            2 => {
                let mut b = [0; 2];
                b[0] = (header_bits & 0xff) as u8;
                b[1] = (header_bits >> 8) as u8;
                let r = writer.write(&b);
                r.expect("write h 2");
                adler.update_buffer(&b);
            }
            3 => {
                let mut b = [0; 4]; // 4!
                b[0] = (header_bits & 0xff) as u8;
                b[1] = (header_bits >> 8) as u8;
                b[2] = (header_bits >> 16) as u8;
                adler.update_buffer(&b);
                let r = writer.write(&b);
                r.expect("write h 3");
            }
            4 => {
                let mut b = [0; 4];
                b[0] = (header_bits & 0xff) as u8;
                b[1] = (header_bits >> 8) as u8;
                b[2] = (header_bits >> 16) as u8;
                b[3] = (header_bits >> 24) as u8;
                adler.update_buffer(&b);
                let r = writer.write(&b);
                r.expect("write h 4");
            }
            _ => {
                panic!("unsupported header_bytes value");
            }
        }

        let mut compressed: Vec<Option<Vec<u8>>> = vec![None; ordered.len()];

        for colidx in 0..columns.len() {
            let validx = ordered[colidx];
            let val = &values[validx];
            match val {
                &ColumnValue::String { ref v } => {
                    let col = &columns[colidx];
                    match col.compression {
                        CompressionType::Brotli => {
                            panic!("brotli compression not supported yet");
                        }
                        CompressionType::Lz4 => {
                            let buf = Vec::new();
                            let mut fo = lz4::EncoderBuilder::new()
                                .checksum(lz4::ContentChecksum::NoChecksum)
//                                .checksum(lz4::ContentChecksum::ChecksumEnabled)
//                                .block_size(lz4::BlockSize::Max64KB)
                                .block_size(lz4::BlockSize::Default)
                                .block_mode(lz4::BlockMode::Linked)
//                                .block_mode(lz4::BlockMode::Independent)
//                                .level(16)
                                .build(buf)
                                .unwrap();
                            let b = v.as_bytes();
                            let wr = fo.write(&b);
                            wr.expect("write wr");
                            let (w, res) = fo.finish();
                            res.expect("compression finish");
                            if b.len() > 0 {
                                assert!(w.len() != 0);
                            }
                            compressed[colidx] = Some(w);
                        }
                        CompressionType::Zlib => {
                            panic!("zlib compression not supported yet");
                        }
                        CompressionType::None => {}
                    }
                }
                _ => {}
            }
        }

        for colidx in 0..columns.len() {
            let validx = ordered[colidx];
            let val = &values[validx];
            match val {
                &ColumnValue::U32 { v } => {
                    let mut buf = [0; 4];
                    buf[0] = v as u8;
                    buf[1] = ((v >> 8) & 0xff) as u8;
                    buf[2] = ((v >> 16) & 0xff) as u8;
                    buf[3] = ((v >> 24) & 0xff) as u8;
                    let r = writer.write(&buf);
                    r.expect("write u32");
                    adler.update_buffer(&buf);
                }
                &ColumnValue::U64 { v } => {
                    let mut buf = [0; 8];
                    buf[0] = v as u8;
                    buf[1] = (v >> 8) as u8;
                    buf[2] = (v >> 16) as u8;
                    buf[3] = (v >> 24) as u8;
                    buf[4] = (v >> 32) as u8;
                    buf[5] = (v >> 40) as u8;
                    buf[6] = (v >> 48) as u8;
                    buf[7] = (v >> 56) as u8;
                    let r = writer.write(&buf);
                    r.expect("write u64");
                    adler.update_buffer(&buf);
                }
                &ColumnValue::String { ref v } => {
                    let mut buf = [0; 4];
                    let len = match compressed[colidx] {
                        Some(ref vec) => vec.len(),
                        None => v.as_bytes().len(),
                    };
                    buf[0] = len as u8;
                    buf[1] = ((len >> 8) & 0xff) as u8;
                    buf[2] = (len >> 16) as u8;
                    buf[3] = (len >> 24) as u8;
                    let r = writer.write(&buf);
                    r.expect("write s len");
                    adler.update_buffer(&buf);
                }
                &ColumnValue::Null => {}
            }
        }

        for colidx in 0..columns.len() {
            let validx = ordered[colidx];
            let val = &values[validx];
            match val {
                &ColumnValue::String { ref v } => {
                    match compressed[colidx] {
                        Some(ref cv) => {
                            let r = writer.write(&cv);
                            if r.unwrap() != cv.len() {
                                panic!("write failed (compressed/string)");
                            }
                            adler.update_buffer(&cv);
                        }
                        None => {
                            let b = &v.as_bytes();
                            let r = writer.write(b);
                            if r.unwrap() != b.len() {
                                panic!("write failed (uncompressed/string)");
                            }
                            adler.update_buffer(b);
                        }
                    }
                }
                _ => {}
            }
        }

        match *checksum {
            ChecksumType::Adler32 => {
                let mut b = [0; 4];
                let hash = adler.hash();
                b[0] = hash as u8;
                b[1] = (hash >> 8) as u8;
                b[2] = (hash >> 16) as u8;
                b[3] = (hash >> 24) as u8;
                let r = writer.write(&b);
                r.expect("write ch");
            }
            ChecksumType::None => {
            }
        }
}
