use types::{ChecksumType, Column, ColumnValue};
use v1::row::{Row};
use v1::write::{schema_write};
use v1::read::schema_read;
use v1::parse::parse_string;

use std::io::{Error, ErrorKind, Read, Write};
use std::str;

pub struct Metadata {
    pub checksum:     ChecksumType,
    pub header_bytes: usize,
                      // ordered list of columns
    pub columns:      Vec<Column>,
}

impl Metadata {

    pub fn parse<R: Read>(reader: &mut R) -> Result<Self, Error> {
        let mut s: String = String::new();
        match reader.read_to_string(&mut s) {
            Ok(_) => {
                parse_string(&s).ok_or(Error::new(ErrorKind::Other, "parsing failed"))
            }
            Err(why) => Err(why),
        }
    }

    pub fn write<W: Write>(&self,
                           writer: &mut W,
                           names: &[&str],
                           values: &[ColumnValue]) {
        schema_write(
            &self.columns,
            writer,
            names,
            values,
            self.header_bytes,
            &self.checksum
        );
    }

    pub fn read<R: Read>(&self, reader: &mut R) -> Row {
        loop {
            let r = schema_read(
                &self.columns,
                reader,
                self.header_bytes,
                &self.checksum
            );
            if r.is_some() {
                return r.unwrap();
            }
            println!("continuing with next row");
        }

    }
}
