extern crate lz4;
extern crate brotli2;
extern crate adler32;

use std::io::{Error, ErrorKind, Read, Write};
use brotli2::read;
use adler32::RollingAdler32;

#[derive(PartialEq,Clone,Copy, Debug)]
enum ColumnType {
    u32le,
    u64le,
    string,
}

#[derive(PartialEq, Debug, Clone)]
pub enum ColumnValue {
    null,
    u32le {
        v: u32,
    },
    u64le {
        v: u64,
    },
    string {
        v: String,
    },
}

#[derive(Clone, Debug)]
enum ChecksumType {
    none,
    adler32,
}

#[derive(Clone, Copy, Debug)]
enum CompressionType {
    none,
    lz4,
    brotli,
    zlib,
}

// describes a column in a row
#[derive(Clone, Debug)]
pub struct Column {
    name: String,
    ctype: ColumnType,
    meaning: String, // arbitrary string
    compression: CompressionType,
}

#[derive(Debug)]
pub struct Row<'a> {
    columns: &'a [Column],
    values: Vec<ColumnValue>,
    nval: ColumnValue,
}

impl<'a> Row<'a> {
    fn new(cols: &'a [Column]) -> Row {
        Row {
            values: vec![ColumnValue::null; cols.len()],
            columns: cols,
            nval: ColumnValue::null,
        }
    }

    fn push(&mut self, colidx: usize, v: ColumnValue) {
        assert!(self.values[colidx] == ColumnValue::null);
        self.values[colidx] = v;
    }

    fn geti(&self, colidx: usize) -> &ColumnValue {
        if colidx < self.values.len() {
            &self.values[colidx]
        } else {
            &self.nval
        }
    }

    fn getn(&self, colname: &str) -> &ColumnValue {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name == colname {
                return &self.values[i];
            }
        }
        &self.nval
    }
}

pub struct Metadata {
    checksum: ChecksumType,
    header_bytes: usize,
    // ordered list of columns
    columns: Vec<Column>,
}

impl Metadata {
    pub fn parse_string(s: &str) -> Option<Self> {
        let mut checksum = ChecksumType::none;

        let mut columns: Vec<Column> = Vec::new();

        for line in s.lines() {
            let comment_start = line.find('#').unwrap_or(line.len());
            let (lnu, _) = line.split_at(comment_start);
            let ln = lnu.trim();

            let mut parts = ln.split(' ');
            let first = parts.next();
            match first {
                Some(s) => {
                    if s == "column" {
                        let name = parts.next().unwrap_or("");
                        if name == "" {
                            return None;
                        }
                        let type_string = parts.next();
                        let column_type = if let Some(ts) = type_string {
                            match ts {
                                "u32le" => ColumnType::u32le,
                                "u64le" => ColumnType::u64le,
                                "string" => ColumnType::string,
                                _ => {
                                    return None;
                                }
                            }
                        } else {
                            return None;
                        };
                        let meaning = parts.next().unwrap_or("");
                        let compression = parts.next().unwrap_or("");
                        let compression_type = match compression {
                            "lz4" => CompressionType::lz4,
                            "brotli" => CompressionType::brotli,
                            "zlib" => CompressionType::zlib,
                            "" => CompressionType::none,
                            _ => {
                                return None;
                            }
                        };
                        let c = Column {
                            name: name.to_string(),
                            ctype: column_type,
                            meaning: meaning.to_string(),
                            compression: compression_type,
                        };
                        columns.push(c);
                    } else if s == "reorder" {
                        panic!("reorder not supported!");
                    } else if s == "checksum" {
                        match parts.next() {
                            Some(c) => {
                                checksum = match c {
                                    "adler32" => ChecksumType::adler32,
                                    "none" => ChecksumType::none,
                                    _ => panic!("Unknown checksum type {}", c),
                                }
                            }
                            None => {
                                println!("missing checksum parameter");
                            }
                        }
                    } else {
                        // early return
                        return None;
                    }
                }
                None => {}
            }
        }

        let md = Metadata {
            checksum: checksum,
            header_bytes: (columns.len() + 7) / 8,
            columns: columns,
        };

        Some(md)
    }

    pub fn parse<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut s: String = String::new();
        match reader.read_to_string(&mut s) {
            Ok(_) => {
                Metadata::parse_string(&s).ok_or(Error::new(ErrorKind::Other, "parsing failed"))
            }
            Err(why) => Err(why),
        }
    }

    fn getindex(&self, colname: &str) -> usize {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name == colname {
                return i;
            }
        }
        panic!("getindex: column {} not found", colname);
    }

    pub fn write<W: Write>(&self, writer: &mut W, names: &[&str], values: &[ColumnValue]) {

        // ordered[colidx] = valueindex|nameindex
        let mut ordered: Vec<usize> = vec![0; self.columns.len()];

        for (i, name) in names.iter().enumerate() {
            let index = self.getindex(name);
            ordered[index] = i;

            match self.columns[index].ctype {
                ColumnType::string => {
                    match values[i] {
                        ColumnValue::string { .. } => {}
                        ColumnValue::u32le { .. } => {
                            panic!("Column {} expected string received u32le", names[i]);
                        }
                        ColumnValue::u64le { .. } => {
                            panic!("Column {} expected string received u64le", names[i]);
                        }
                        ColumnValue::null => {}
                    }
                }
                ColumnType::u32le => {
                    match values[i] {
                        ColumnValue::string { .. } => {
                            panic!("Column {} expected u32le received string", names[i]);
                        }
                        ColumnValue::u64le { .. } => {
                            panic!("Column {} expected u32le received u64le", names[i]);
                        }
                        ColumnValue::u32le { .. } => {}
                        ColumnValue::null => {}
                    }
                }
                ColumnType::u64le => {
                    match values[i] {
                        ColumnValue::string { .. } => {
                            panic!("Column {} expected u64le received string", names[i]);
                        }
                        ColumnValue::u32le { .. } => {
                            panic!("Column {} expected u63le received u32le", names[i]);
                        }
                        ColumnValue::u64le { .. } => {}
                        ColumnValue::null => {}
                    }
                }
            }
        }

        let mut adler = RollingAdler32::from_value(0);

        let mut header_bits = 0;

        for colidx in 0..self.columns.len() {
            let validx = ordered[colidx];
            let val = &values[validx];
            match val {
                &ColumnValue::null => {}
                _ => {
                    header_bits = header_bits | (1 << colidx);
                }
            }
        }

        match self.header_bytes {
            1 => {
                let mut b = [0; 1];
                b[0] = header_bits as u8;
                adler.update_buffer(&b);
                writer.write(&b);
            }
            2 => {
                let mut b = [0; 2];
                b[0] = (header_bits & 0xff) as u8;
                b[1] = (header_bits >> 8) as u8;
                writer.write(&b);
                adler.update_buffer(&b);
            }
            3 => {
                let mut b = [0; 4]; // 4!
                b[0] = (header_bits & 0xff) as u8;
                b[1] = (header_bits >> 8) as u8;
                b[2] = (header_bits >> 16) as u8;
                adler.update_buffer(&b);
                writer.write(&b);
            }
            4 => {
                let mut b = [0; 4];
                b[0] = (header_bits & 0xff) as u8;
                b[1] = (header_bits >> 8) as u8;
                b[2] = (header_bits >> 16) as u8;
                b[3] = (header_bits >> 24) as u8;
                adler.update_buffer(&b);
                writer.write(&b);
            }
            _ => {
                panic!("unknown header_bytes value");
            }
        }

        let mut compressed: Vec<Option<Vec<u8>>> = vec![None; ordered.len()];

        for (colidx, validx) in ordered.iter().enumerate() {
            let val = &values[*validx];
            match val {
                &ColumnValue::string { ref v } => {
                    let col = &self.columns[colidx];
                    match col.compression {
                        CompressionType::brotli => {
                            panic!("brotli compression not supported yet");
                        }
                        CompressionType::lz4 => {
                            let buf = Vec::new();
                            let mut fo = lz4::EncoderBuilder::new()
                                .checksum(lz4::ContentChecksum::NoChecksum)
//                                .checksum(lz4::ContentChecksum::ChecksumEnabled)
//                                .block_size(lz4::BlockSize::Max64KB)
                                .block_size(lz4::BlockSize::Default)
                                .block_mode(lz4::BlockMode::Linked)
//                                .block_mode(lz4::BlockMode::Independent)
                                .level(16)
                                .build(buf)
                                .unwrap();
                            let b = v.as_bytes();
                            let wr = fo.write(&b);
                            let (w, res) = fo.finish();
                            compressed[colidx] = Some(w);
                        }
                        CompressionType::zlib => {
                            panic!("zlib compression not supported yet");
                        }
                        CompressionType::none => {}
                    }
                }
                _ => {}
            }
        }

        for colidx in 0..self.columns.len() {
            let validx = ordered[colidx];
            let val = &values[validx];
            match val {
                &ColumnValue::u32le { v } => {
                    let mut buf = [0; 4];
                    buf[0] = v as u8;
                    buf[1] = ((v >> 8) & 0xff) as u8;
                    buf[2] = ((v >> 16) & 0xff) as u8;
                    buf[3] = ((v >> 24) & 0xff) as u8;
                    writer.write(&buf);
                    adler.update_buffer(&buf);
                }
                &ColumnValue::u64le { v } => {
                    let mut buf = [0; 8];
                    buf[0] = v as u8;
                    buf[1] = (v >> 8) as u8;
                    buf[2] = (v >> 16) as u8;
                    buf[3] = (v >> 24) as u8;
                    buf[4] = (v >> 32) as u8;
                    buf[5] = (v >> 40) as u8;
                    buf[6] = (v >> 48) as u8;
                    buf[7] = (v >> 56) as u8;
                    writer.write(&buf);
                    adler.update_buffer(&buf);
                }
                &ColumnValue::string { ref v } => {
                    let mut buf = [0; 4];
                    let len = match compressed[colidx] {
                        Some(ref vec) => vec.len(),
                        None => v.as_bytes().len(),
                    };
                    buf[0] = len as u8;
                    buf[1] = ((len >> 8) & 0xff) as u8;
                    buf[2] = (len >> 16) as u8;
                    buf[3] = (len >> 24) as u8;
                    writer.write(&buf);
                    adler.update_buffer(&buf);
                }
                &ColumnValue::null => {}
            }
        }

        for colidx in 0..self.columns.len() {
            let validx = ordered[colidx];
            let val = &values[validx];
            match val {
                &ColumnValue::string { ref v } => {
                    match compressed[colidx] {
                        Some(ref cv) => {
                            writer.write(&cv);
                            adler.update_buffer(&cv);
                        }
                        None => {
                            let b = &v.as_bytes();
                            writer.write(b);
                            adler.update_buffer(b);
                        }
                    }
                }
                _ => {}
            }
        }

        match self.checksum {
            ChecksumType::adler32 => {
                let mut b = [0; 4];
                let hash = adler.hash();
                b[0] = hash as u8;
                b[1] = (hash >> 8) as u8;
                b[2] = (hash >> 16) as u8;
                b[3] = (hash >> 24) as u8;
                writer.write(&b);
            }
            ChecksumType::none => {}
        }
    }

    pub fn read<R: Read>(&self, reader: &mut R) -> Row {
        let mut adler = RollingAdler32::from_value(0);

        let header_bits = if self.header_bytes == 1 {
            let mut b = [0; 1];
            reader.read(&mut b);
            adler.update_buffer(&b);
            b[0] as usize
        } else if self.header_bytes == 2 {
            let mut b = [0; 2];
            reader.read(&mut b);
            adler.update_buffer(&b);
            (b[0] as usize) | ((b[1] as usize) << 8)
        } else if self.header_bytes == 3 || self.header_bytes == 4 {
            let mut b = [0; 4];
            reader.read(&mut b);
            adler.update_buffer(&b);
            (b[0] as usize) | (b[1] as usize) << 8 | (b[2] as usize) << 16 | (b[3] as usize) << 24
        } else if self.header_bytes > 4 && self.header_bytes <= 8 {
            let mut b = [0; 8];
            reader.read(&mut b);
            adler.update_buffer(&b);
            (b[0] as usize) | (b[1] as usize) << 8 | (b[2] as usize) << 16 |
            (b[3] as usize) << 24 | (b[4] as usize) << 32 | (b[5] as usize) << 40 |
            (b[6] as usize) << 48 | (b[7] as usize) << 56
        } else {
            // unknown header_bytes size
            0
        };

        let mut blobs: Vec<(usize, usize, CompressionType)> = Vec::with_capacity(self.columns
            .len()); // count only strings?
        let mut result = Row::new(&self.columns);

        for (i, c) in self.columns.iter().enumerate() {
            if header_bits & (1 << i) != 0 {
                // column is present
                match c.ctype {
                    ColumnType::u32le => {
                        let mut buf = [0; 4];
                        reader.read(&mut buf);
                        adler.update_buffer(&buf);
                        let v = (buf[0] as usize) | (buf[1] as usize) << 8 |
                                (buf[2] as usize) << 16 |
                                (buf[3] as usize) << 24;
                        result.push(i, ColumnValue::u32le { v: v as u32 });
                    }
                    ColumnType::u64le => {
                        let mut buf = [0; 8];
                        reader.read(&mut buf);
                        adler.update_buffer(&buf);
                        let v = (buf[0] as usize) | (buf[1] as usize) << 8 |
                                (buf[2] as usize) << 16 |
                                (buf[3] as usize) << 24 |
                                (buf[4] as usize) << 32 |
                                (buf[5] as usize) << 40 |
                                (buf[6] as usize) << 48 |
                                (buf[7] as usize) << 56;
                        result.push(i, ColumnValue::u64le { v: v as u64 });
                    }
                    ColumnType::string => {
                        let mut buf = [0; 4];
                        reader.read(&mut buf);
                        adler.update_buffer(&buf);
                        let size = (buf[0] as usize) | (buf[1] as usize) << 8 |
                                   (buf[2] as usize) << 16 |
                                   (buf[3] as usize) << 24;
                        blobs.push((i, size, c.compression));
                    }
                    _ => {
                        println!("unknown column type");
                    }
                }
            }
        }


        for &(i, size, ref compression) in blobs.iter() {
            let mut buf: Vec<u8> = vec![0; size]; // Vec::with_capacity(size);
            reader.read(&mut buf);
            adler.update_buffer(&buf);
            let s = match compression {
                &CompressionType::brotli => {
                    let u: &[u8] = &buf;
                    let mut d = read::BrotliDecoder::new(u);
                    let mut dbuf: Vec<u8> = Vec::new();
                    let r = d.read_to_end(&mut dbuf);
                    let s = std::str::from_utf8(&dbuf).unwrap();
                    s.to_string()
                }
                &CompressionType::lz4 => {
                    // lz4 'block' container compression
                    let u: &[u8] = &buf;
                    let mut d = lz4::Decoder::new(u).unwrap();
                    let mut dbuf: Vec<u8> = Vec::new();
                    let r = d.read_to_end(&mut dbuf);
                    r.unwrap();
                    d.finish();
                    let s = std::str::from_utf8(&dbuf).unwrap();
                    s.to_string()
                }
                &CompressionType::zlib => {
                    let s = std::str::from_utf8(&buf).unwrap();
                    s.to_string()
                }
                &CompressionType::none => {
                    let s = std::str::from_utf8(&buf).unwrap();
                    s.to_string()
                }
            };
            result.push(i, ColumnValue::string { v: s });
        }

        match self.checksum {
            ChecksumType::adler32 => {
                let mut buf = [0; 4];
                reader.read(&mut buf);
                let hash = buf[0] as u32 | ((buf[1] as u32) << 8) | ((buf[2] as u32) << 16) |
                           ((buf[3] as u32) << 24);
                let expected = adler.hash();
                if hash != expected {
                    panic!("incorrect checksum");
                }
            }
            ChecksumType::none => {}
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::prelude::*;

    use super::{Metadata, ColumnValue};

    #[test]
    fn it_works() {
        let mut f = File::open("/tmp/_testfile.dat").unwrap();
        let md_string = "checksum adler32\ncolumn a string _ brotli\ncolumn b string _ \
                         lz4\ncolumn c u32le\ncolumn d u64le\n";
        let md = Metadata::parse_string(md_string).unwrap();
        {
            let r = md.read(&mut f);
            println!("read/1 {:?} {:?} {:?} {:?}",
                     r.getn("a"),
                     r.getn("b"),
                     r.getn("c"),
                     r.getn("d"));
        }
        {
            let r = md.read(&mut f);
            println!("read/2 {:?} {:?} {:?} {:?}",
                     r.geti(0),
                     r.geti(1),
                     r.geti(2),
                     r.geti(3));
        }
        {
            let r = md.read(&mut f);
            println!("read/3 {:?} {:?} {:?} {:?}",
                     r.getn("a"),
                     r.getn("b"),
                     r.getn("c"),
                     r.getn("d"));
        }
    }

    #[test]
    fn write_works() {
        let mut f = File::create("/tmp/_testfile_w.dat").unwrap();
        let md_string = "checksum adler32\ncolumn a string _ lz4\ncolumn b string\ncolumn c \
                         u32le\ncolumn d u64le\n";
        let md = Metadata::parse_string(md_string).unwrap();
        let values = [ColumnValue::string { v: "hello_world".to_string() },
                      ColumnValue::string { v: "something".to_string() },
                      ColumnValue::u64le { v: 987 },
                      ColumnValue::u32le { v: 123 }];
        let names = ["b", "a", "d", "c"];
        md.write(&mut f, &names, &values);
    }
}
