use types::{ChecksumType, CompressionType, ColumnType, Column};
use v1::schema::Metadata;

pub fn parse_string(s: &str) -> Option<Metadata> {
    let mut checksum = ChecksumType::None;

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
                            "u32le" => ColumnType::U32le,
                            "u64le" => ColumnType::U64le,
                            "string" => ColumnType::String,
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
                        "lz4" => CompressionType::Lz4,
                        "brotli" => CompressionType::Brotli,
                        "zlib" => CompressionType::Zlib,
                        "" => CompressionType::None,
                        _ => {
                            return None;
                        }
                    };
                    let c = Column {
                        name: name.to_string(),
                        ctype: column_type,
                        meaning: meaning.to_string(),
                        compression: compression_type,
                        nullable: false,
                    };
                    columns.push(c);
                } else if s == "reorder" {
                    panic!("reorder not supported!");
                } else if s == "checksum" {
                    match parts.next() {
                        Some(c) => {
                            checksum = match c {
                                "adler32" => ChecksumType::Adler32,
                                "none" => ChecksumType::None,
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
