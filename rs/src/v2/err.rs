use std::error::Error;
use std::fmt;

#[derive(Debug)]
pub enum SchemaReadError {
    Eof,
    UnexpectedEof,
    DecompressionError,
    ChecksumError,
    BadUtf8
}

impl Error for SchemaReadError {
    fn description(&self) -> &str {
        match self {
            SchemaReadError::DecompressionError => "Decompression Error",
            SchemaReadError::Eof => "EOF",
            SchemaReadError::UnexpectedEof => "Unexpected end of file",
            SchemaReadError::ChecksumError => "Checksum error",
            SchemaReadError::BadUtf8 => "Bad UTF-8 encoding",
        }
    }
}

impl fmt::Display for SchemaReadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            ref e => f.write_str(e.description()),
        }
    }
}
