#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::prelude::*;

    use v1::schema::Metadata;
    use types::ColumnValue;
    use v1::parse::parse_string;

//    use super::{Metadata, ColumnValue, parse_string};

    #[test]
    fn read_write_works() {
        let md_string = "\
           checksum adler32\n\
            column a string _ lz4\n\
            column b string _ lz4\n\
            column c u32le\n\
            column d u64le\n\
        ";

        {
            let mut f = File::create("/tmp/_testfile_w.dat").unwrap();
            let md = parse_string(md_string).unwrap();
            let values = [
                ColumnValue::String { v: "not_compressed".to_string() },
                ColumnValue::String { v: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string() },
                ColumnValue::U64 { v: 987 },
                ColumnValue::U32 { v: 123 }
            ];
            let names = ["b", "a", "d", "c"];
            md.write(&mut f, &names, &values);
            md.write(&mut f, &names, &values);
            md.write(&mut f, &names, &values);
        }

        {
            let mut f = File::open("/tmp/_testfile_w.dat").unwrap();
            let md = parse_string(md_string).unwrap();
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
    }

}
