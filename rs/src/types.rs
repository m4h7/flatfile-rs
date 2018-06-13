#[derive(PartialEq,Clone,Copy, Debug)]
pub enum ColumnType {
    U32le,
    U64le,
    String,
}

#[derive(Clone, Debug)]
pub enum ChecksumType {
    None,
    Adler32,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CompressionType {
    None,
    Lz4,
    Brotli,
    Zlib,
}

#[derive(PartialEq, Debug, Clone)]
pub enum ColumnValue {
    Null,
    U32 {
        v: u32,
    },
    U64 {
        v: u64,
    },
    String {
        v: String,
    },
}

// change from AOS to SOA?
// describes a column in a row
#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub ctype: ColumnType,
    pub meaning: String, // arbitrary string
    pub compression: CompressionType,
    pub nullable: bool, // TBD
}
