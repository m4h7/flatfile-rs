use types::{ColumnValue, ColumnType, Relation};
use v2::schema2::{Schema, Schema2};
use v2::mmapbuf::MmapBuf;
use v2::write2::{read_schema_v2, schema_read_row};
use v2::ast::{Expr, eval, Value};
use v2::err::{SchemaReadError};

use std::collections::HashMap;
use std::fs::{File, read_dir};
use std::io::{Error, ErrorKind, Result};
//use std::rc::Rc;
//use std::cell::RefCell;
use std::borrow::Borrow;
use std::borrow::BorrowMut;

extern crate regex;
use self::regex::Regex;

pub struct EmptyRelation {
}

impl Relation for EmptyRelation {
    fn length(&self) -> usize {
      0
    }
    fn read(&mut self) -> bool {
      false
    }
    fn name(&self, _n: usize) -> String {
        "null".to_owned()
    }
    fn ctype(&self, _n: usize) -> ColumnType {
        ColumnType::String
    }
    fn nullable(&self, _n: usize) -> bool {
        false
    }
    fn value(&self, _n: usize) -> &ColumnValue {
        &ColumnValue::Null
    }
}

// physical layer
pub struct FileRelation {
    schema: Schema2,
    m: MmapBuf,
    current: Vec<ColumnValue>,
    done: bool,
    name: String, // used for printing errors
}

impl Relation for FileRelation {
    fn length(&self) -> usize {
        self.schema.len()
    }
    fn read(&mut self) -> bool {

        loop {
            let result = schema_read_row(
                &mut self.m,
                self.current.as_mut_slice(),
                &self.schema
            );

            match result {
                Ok(_) => return true, // have more data
                Err(e) => {
                    match e {
                        SchemaReadError::UnexpectedEof => {
                            println!("SchemaReadError::UnexpectedEof");
                            return false;
                        },
                        SchemaReadError::Eof => return false,
                        SchemaReadError::ChecksumError => {
                            println!("SchemaReadError::ChecksumError");
                            // continue to next row
                        },
                        SchemaReadError::BadUtf8 => {
                            println!("SchemaReadError::BadUtf8 {}", self.name);
                            // continue to next row
                        },
                        SchemaReadError::DecompressionError => {
                            println!("SchemaReadError::DecompressionError");
                            // continue to next row
                        },
                    }
                }
            }
        }
    }
    fn name(&self, n: usize) -> String {
        self.schema.name(n).to_owned()
    }
    fn ctype(&self, n: usize) -> ColumnType {
        self.schema.ctype(n)
    }
    fn nullable(&self, n: usize) -> bool {
        self.schema.nullable(n)
    }
    fn value(&self, n: usize) -> &ColumnValue {
        assert!(self.done == false);
        self.current[n].borrow()
    }
}

impl FileRelation {
    pub fn new(fname: &str) -> Result<FileRelation> {
        let mut readvec = Vec::new();

        let mut f = File::open(fname)?;

        match f.metadata() {
            Ok(md) => {
                if md.len() == 0 { // mmap will not work
                    println!("FileRelation::new(\"{}\") - file is empty", fname);
                    let empty_file = Error::new(ErrorKind::Other, "empty file");
                    return Err(empty_file);
                }
            }
            Err(e) => {
                return Err(e)
            }
        }

        let mut mmapbuf = MmapBuf::new(f);

        let sch = read_schema_v2(&mut mmapbuf).unwrap();

        for _ in 0..sch.len() {
            readvec.push(ColumnValue::Null);
        }

        let r = FileRelation {
            schema: sch,
            m: mmapbuf,
            current: readvec,
            done: false,
            name: fname.to_owned()
        };

        Ok(r)
    }
}

pub struct Restriction {
    rel: Box<Relation>,
    e:   Expr,
}

impl Restriction {
    pub fn new(base: Box<Relation>,
               e: Expr,
    ) -> Restriction {
        Restriction {
            rel: base,
            e: e,
        }
    }
}

impl Relation for Restriction {
    fn read(&mut self) -> bool {
        let mut found = false;
        while !found {
            let done = self.rel.read();
            if done {
                return done;
            }
            if eval(self.rel.borrow(), &self.e) {
                found = true;
            }
        }
        false
    }
    fn length(&self) -> usize {
        self.rel.length()
    }
    fn name(&self, n: usize) -> String {
        self.rel.name(n)
    }
    fn ctype(&self, n: usize) -> ColumnType {
        self.rel.ctype(n)
    }
    fn nullable(&self, n: usize) -> bool {
        self.rel.nullable(n)
    }
    fn value(&self, n: usize) -> &ColumnValue {
        self.rel.value(n)
    }
}

pub struct ConcatRelation {
    relations: Vec<Box<Relation >>,
    current: usize,
}

pub struct Projection {
    relation: Box<Relation>,
    colmap: Vec<usize>,
    colcount: usize,
}

impl<'a> Projection {
    pub fn new(rel: Box<Relation>, cols: Vec<String>) -> Projection {
        let mut colmap = Vec::new();

        // make colindexes [-1, -1, -1, ...] same size as cols
        let minusone = -1;
        for j in 0..cols.len() {
            colmap.push(minusone as usize);
        }

        for j in 0..cols.len() {
            let colname = &cols[j];

            for i in 0..rel.length() {
                if rel.name(i) == *colname {
                    colmap[j] = i;
                    println!("projection mapping column {} to {} ({})", j, i, colname);
                }
            }
        }

        Projection {
            relation: rel,
            colmap: colmap,
            colcount: cols.len(),
        }
    }
}

impl Relation for Projection {
    fn length(&self) -> usize {
        self.colmap.len()
    }
    fn read(&mut self) -> bool {
        self.relation.read()
    }
    fn name(&self, n: usize) -> String {
        let m = self.colmap[n];
        self.relation.name(m)
    }
    fn ctype(&self, n: usize) -> ColumnType {
        let m = self.colmap[n];
        self.relation.ctype(m)
    }
    fn nullable(&self, n: usize) -> bool {
        let m = self.colmap[n];
        self.relation.nullable(m)
    }
    fn value(&self, n: usize) -> &ColumnValue {
        let m = self.colmap[n];
        self.relation.value(m)
    }
}

impl ConcatRelation {
    pub fn new() -> ConcatRelation {
        ConcatRelation {
            relations: Vec::new(),
            current: 0,
        }
    }
    pub fn size(&self) -> usize {
        self.relations.len()
    }
    pub fn add(&mut self, rel: Box<Relation>) -> bool {
        // first check that the schema is the same
        if self.relations.len() > 0 {
            if self.relations[0].length() != rel.length() {
                println!("union: schema lengths are different {} vs {}",
                         self.relations[0].length(),
                         rel.length());
                return false;
            }
            for i in 0..rel.length() {
                if self.relations[0].name(i) != rel.name(i) {
                    println!("union: name {} is different: '{}' vs '{}'",
                             i,
                             self.relations[0].name(i),
                             rel.name(i));
                    return false;
                }
                if self.relations[0].ctype(i) != rel.ctype(i) {
                    println!("union: type {} is different: '{:?}' vs '{:?}'",
                             i,
                             self.relations[0].ctype(i),
                             rel.ctype(i));
                    return false;
                }
                if self.relations[0].nullable(i) != rel.nullable(i) {
                    println!("union: nullability {} is different: '{}' vs '{}'",
                             i,
                             self.relations[0].nullable(i),
                             rel.nullable(i));
                    return false;
                }
            }
        }
        self.relations.push(rel);
        true
    }
}

impl Relation for ConcatRelation {
    fn length(&self) -> usize {
        if self.relations.len() == 0 {
            panic!("ConcatRelation::length => no relations!");
        }
        self.relations[0].length()
    }
    fn read(&mut self) -> bool {
        loop {
            if self.current < self.relations.len() {
                let ok = self.relations[self.current].read();
                if !ok {
                    self.current += 1;
                } else {
                    return ok;
                }
            } else {
                // done=true because no relations
                return false;
            }
        }
    }
    fn name(&self, n: usize) -> String {
        assert!(self.current < self.relations.len());
        self.relations[self.current].name(n)
    }
    fn ctype(&self, n: usize) -> ColumnType {
        assert!(self.current < self.relations.len());
        self.relations[self.current].ctype(n)
    }
    fn nullable(&self, n: usize) -> bool {
        assert!(self.current < self.relations.len());
        self.relations[self.current].nullable(n)
    }
    fn value(&self, n: usize) -> &ColumnValue {
        assert!(self.current < self.relations.len());
        let cv = self.relations[self.current].value(n);
        cv
    }
}

struct ParseError {
}

const SPACE: u8 = ' ' as u8;
const TAB: u8 = '\t' as u8;
const CR: u8 = '\r' as u8;
const LF: u8 = '\n' as u8;
const DQUOTE: u8 = '"' as u8;
const LBRACE: u8 = '{' as u8;
const RBRACE: u8 = '}' as u8;
const RE: u8 = '/' as u8;

// parser
// -------------------------------------
// a = file "name"
// b = project a cid bid xid
// c = rename b cid -> id bix -> ix
// d = restrict c (id > 3 && id < 6)
// e = dedup d [col1] [col2] (cond1) (cond2) (cond3)
// f = diff a b  # items in 'a' that are not in 'b'
// f = diff a.id b.id
// i = sort h.xid asc
// j = inner_join h.id j.id # intersection
// k = left_join h.id j.id # j will have schema with nulls
// l = full_outer_join
// x = union "*.glob" "*.glob2" otherrel file_{variable}.ext
//


#[derive(Debug)]
struct Rels {
    namemap: HashMap<String, RelationParam>,
}

impl Rels {
    fn new() -> Rels {
        Rels { namemap: HashMap::new() }
    }
    fn add(&mut self, name: String, rel: RelationParam) {
        self.namemap.insert(name, rel);
    }
    fn get(&self, name: &str) -> Option<&RelationParam> {
        self.namemap.get(name)
    }
}
#[derive(Debug)]
enum RelationParam {
    File { filename: String },
    Union { relations: Vec<String> },
    Projection { base: String, columns: Vec<String> },
}

struct Tokenizer<'a> {
    s: &'a [u8],
    pos: usize,
}

impl<'a> Tokenizer<'a> {
    fn new(s: &'a [u8]) -> Tokenizer<'a> {
        Tokenizer { s: s, pos: 0 }
    }

    fn skip_whitespace(&mut self, a: u8, b: u8, c: u8, d: u8) -> bool {
        let mut k = self.pos;
        let s = self.s;
        while k < s.len() && (s[k] == a || s[k] == b || s[k] == c || s[k] == d) {
            k += 1;
        }
        let result = self.pos != k;
        self.pos = k;
        result
    }

    fn eos(&self) -> bool {
        self.pos >= self.s.len()
    }

    fn parse_token(&mut self) -> Option<String> {
        let mut k = self.pos;
        let s = self.s;

        if k < s.len() && (s[k] == DQUOTE || s[k] == RE || s[k] == LBRACE) { // quoted text
            // char that ends this quote
            let endchar = if s[k] == LBRACE { RBRACE } else { s[k] };
            k += 1;
            while k < s.len() && s[k] != endchar {
                k += 1;
            }
            // include the endchar
            if k < s.len() {
                assert!(s[k] == endchar);
                k += 1;
            } else {
                return None; // unterminated quote
            }
        } else {
            // standard token, take chars until whitespace
            while k < s.len() && s[k] != SPACE && s[k] != TAB && s[k] != CR && s[k] != LF {
                k += 1;
            }
        }
        if self.pos == k {
            None
        } else {
            let t = String::from_utf8_lossy(&s[self.pos..k]).to_string(); // TODO
            // update pos
            self.pos = k;
            Some(t)
        }
    }

    fn expect(&mut self, s2: &str) -> bool {
        let u = s2.as_bytes();
        let mut k = 0;
        while self.pos < self.s.len() && k < u.len() && self.s[self.pos + k] == u[k] {
            k += 1;
        }
        // return j + k if u fully matched
        if k == u.len() {
            // update pos
            self.pos = self.pos + k;
            true
        } else {
            false
        }
    }
}

fn parse_relalgs(s: &[u8]) -> Option<Rels> { // Result
    let mut r = Rels::new();
    let mut pos = 0;

    let mut t = Tokenizer::new(s);

    // skip newlines and whitespace
    t.skip_whitespace(SPACE, TAB, CR, LF);

    loop {
        t.skip_whitespace(SPACE, TAB, TAB, TAB);
        let token = t.parse_token();
        if token.is_none() {
            break;
        }
        let name = token.unwrap();
        t.skip_whitespace(SPACE, TAB, TAB, TAB);
        if !t.expect("=") {
            println!("expected = after {}", name);
            return None;
        }

        t.skip_whitespace(SPACE, TAB, TAB, TAB);

        let reltypetoken = t.parse_token();
        if reltypetoken.is_none() {
            println!("expected relation_type after =");
            return None;
        }

        let reltype = reltypetoken.unwrap();
        match reltype.as_str() {
            "file" => {
                t.skip_whitespace(SPACE, TAB, TAB, TAB);
                let filename = t.parse_token();
                if filename.is_none() {
                    println!("expecting filename after 'file' relation type");
                    return None;
                }
                if !t.skip_whitespace(CR, LF, LF, LF) && !t.eos() {
                    println!("expecting CRLF or end-of-string after filename ({:?}) in 'file', eos={}", filename, t.eos());
                    return None;
                }
                let fr = RelationParam::File { filename: filename.unwrap() }; // FileRelation::new(filename.as_str()).unwrap();
                r.add(name, fr);
            },
            "project" => {
                let mut base = None;
                let mut columns = Vec::<String>::new();
                loop {
                    t.skip_whitespace(SPACE, TAB, TAB, TAB);
                    let relname = t.parse_token();
                    if let Some(name) = relname {
                        if base.is_none() {
                            // first token is base relation
                            base = Some(name);
                        } else {
                            columns.push(name);
                        }
                    } else { // end of string
                        break;
                    }
                    t.skip_whitespace(SPACE, TAB, TAB, TAB);
                    // stop parsing projection on newline
                    if t.skip_whitespace(CR, LF, LF, LF) {
                        break;
                    }
                }
                let p = RelationParam::Projection{ base: base.unwrap(), columns: columns };
                r.add(name, p);
            },
            "union" => {
                let mut relations = Vec::<String>::new(); // ConcatRelation::new();
                loop {
                    t.skip_whitespace(SPACE, TAB, TAB, TAB);
                    let relname = t.parse_token();
                    if let Some(name) = relname {
                        relations.push(name);
                    } else {
                        // end of string
                        break;
                    }
                    t.skip_whitespace(SPACE, TAB, TAB, TAB);
                    // if endline then parse next statement
                    if t.skip_whitespace(CR, LF, LF, LF) {
                        break;
                    }
                }
                let u = RelationParam::Union { relations: relations };
                r.add(name, u);
            },
            _ => {
                println!("unknown reltype '{}'", reltype);
            }
        }
    }

    Some(r)
}

fn resolve_relation(name: &str, r: &Rels, variables: &HashMap<String, String>) -> Option<Box<Relation>> {
    if let Some(top) = r.get(name) {
        match top {
            RelationParam::File { filename } => {
                let v: Vec<char> = filename.chars().collect();
                let first = v[0];
                let last = v[v.len() - 1];
                let fr = if first == '"' && last == '"' { // filename
                    let name = &filename[1..v.len()-1];
                    println!("resolve fname ({})", name);
                    FileRelation::new(name).unwrap()
                } else {
                    FileRelation::new(&filename).unwrap()
                };
                let r : Box<Relation> = Box::new(fr);
                let result = Some(r);
                result
            },
            RelationParam::Projection { base, columns } => {
                let r_base = resolve_relation(base, &r, &variables).unwrap();
                let p = Projection::new(r_base, columns.to_owned());
                let r : Box<Relation> = Box::new(p);
                let result = Some(r);
                result
            },
            RelationParam::Union { relations } => {
                let mut co = ConcatRelation::new();
                for relation in relations {
                    let v: Vec<char> = relation.chars().collect();
                    let first = v[0];
                    let last = v[v.len() - 1];
                    if first == '"' && last == '"' { // filename
                        let fr = FileRelation::new(&relation[1..v.len()-1]).unwrap();
                        let r : Box<Relation> = Box::new(fr);
                        co.add(r);
                    } else if first == '\'' && last == '\'' { // regex over filenames
                        let unquoted = &relation[1..v.len()-1];

                        let (dir, regexp) = if let Some(index) = unquoted.rfind('/') {
                            let d = &unquoted[..index];
                            let r = &unquoted[index+1..];
                            (d, r)
                        } else { // no path component
                            (".", unquoted)
                        };

                        let re = Regex::new(regexp).unwrap();

                        for entry in read_dir(dir).unwrap() {
                            if let Ok(e) = entry {
                                let name = e.file_name();
                                match name.into_string() {
                                    Ok(s) => {
//                                        println!("union: found file {} match: {}", s, re.is_match(&s));
                                        if re.is_match(&s) {
                                            let fr = FileRelation::new(e.path().to_str().unwrap()).unwrap();
                                            let r: Box<Relation> = Box::new(fr);
                                            let added = co.add(r);
                                            if !added {
                                                println!("unable to add relation because of schema mismatch");
                                                return None;
                                            }
                                        }
                                    },
                                    Err(os) => {
                                        println!("read_dir string not valid unicode {:?}", os);
                                    },
                                };
                            } else {
                                println!("read_dir fail {:?}", entry);
                            }
                        }
                    } else { // name of some other rel
                        let rel = resolve_relation(relation, &r, &variables);
                        let urel = rel.unwrap();
                        co.add(urel);
                    }
                }
                if co.size() > 0 {
                    Some(Box::new(co))
                } else {
                    println!("resolve_relation: union rel has no members");
                    None
                }
            },
        }
    } else {
        println!("resolve_relation: name not found");
        None
    }
}

pub fn create_relation(name: &str, rel: &str, variables: &HashMap<String, String>) -> Option<Box<Relation>> {
    let rels = parse_relalgs(rel.as_bytes()).unwrap();
//    println!("creating relation {} from {:?} ({})", name, rels, rel);
    let z = resolve_relation(name, &rels, &variables);
//    println!("created = {:?}", z.is_some());
    z
}


#[test]
fn test_parsing() {
    let s = "hello = world {b r a c e} \"q u o t {e} d\"";
    let u = s.as_bytes();
    let mut t = Tokenizer::new(&u);
    let t1 = t.parse_token().unwrap();
    t.skip_whitespace(SPACE, TAB, TAB, TAB);
    let z = t.expect("=");
    assert!(z);
    t.skip_whitespace(SPACE, TAB, TAB, TAB);
    let t2 = t.parse_token().unwrap();
    t.skip_whitespace(SPACE, TAB, TAB, TAB);
    let t3 = t.parse_token().unwrap();
    t.skip_whitespace(SPACE, TAB, TAB, TAB);
    let t4 = t.parse_token().unwrap();
    assert!(t1 == "hello");
    assert!(t2 == "world");
    assert!(t3 == "{b r a c e}");
    assert!(t4 == "\"q u o t {e} d\"");
}

#[test]
fn test_union() {
    let rel = "a = file \"/tmp/_test1.dat\"\nb = union '/tmp/_test[XY].dat'";
    let rels = parse_relalgs(rel.as_bytes()).unwrap();
    let variables = HashMap::new();

    let mut rr : Box<Relation> = resolve_relation("b", &rels, &variables).unwrap();
    let len = rr.length();
    while true {
        let r: &mut Relation = rr.borrow_mut();
        let done = r.read();
//        println!("union: read done {}", done);
        if done {
            break;
        }
//        for i in 0..len {
//            println!("union: val {} is {:?}", i, r.value(i));
//        }
    }
}

#[test]
fn test_concat() {
    let r1 = Box::new(FileRelation::new("/tmp/_test1.dat").unwrap());
    let r2 = Box::new(FileRelation::new("/tmp/_test2.dat").unwrap());
    let mut co = ConcatRelation::new();
    co.add(r1);
    co.add(r2);
    let mut prj = Projection::new(Box::new(co), vec!("i32".to_owned(), "i64".to_owned(), "s".to_owned()));

    let e = Expr::Equal { l: Value::Ref { col: 0 }, r: Value::Val { val: ColumnValue::U32 { v: 4 } } };

    let mut rr = Restriction::new(Box::new(prj), e);
    while true {
        let done = rr.read();
//        println!("read done {}", done);
        if done {
            break;
        }
//        for i in 0..rr.length() {
//            println!("val {} is {:?}", i, rr.value(i));
//        }
    }
//    println!("done");
}
