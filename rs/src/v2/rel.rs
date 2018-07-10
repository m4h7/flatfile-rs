use types::{ColumnValue, ColumnType, Relation};
use v2::schema2::{Schema, Schema2};
use v2::mmapbuf::MmapBuf;
use v2::write2::{read_schema_v2, schema_read_row};
use v2::ast::{Expr, eval, Value};

use std::collections::HashMap;
use std::fs::File;
use std::io::Result;
//use std::rc::Rc;
//use std::cell::RefCell;
use std::borrow::Borrow;

pub struct EmptyRelation {
}

impl<'r> Relation<'r> for EmptyRelation {
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
}

impl<'r> Relation<'r> for FileRelation {
    fn length(&self) -> usize {
        self.schema.len()
    }
    fn read(&mut self) -> bool {

        self.done = !schema_read_row(
            &mut self.m,
            self.current.as_mut_slice(),
            &self.schema
        );

        self.done
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
        self.current[n].borrow()
    }
}

impl FileRelation {
    pub fn new(fname: &str) -> Result<FileRelation> {
        let mut readvec = Vec::new();

        let mut f = File::open(fname)?;

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
        };

        Ok(r)
    }
}

pub struct Restriction<'a> {
    rel: Box<Relation<'a>>,
    e:   Expr,
}

impl<'a> Restriction<'a> {
    pub fn new(base: Box<Relation<'a>>,
               e: Expr,
    ) -> Restriction<'a> {
        Restriction {
            rel: base,
            e: e,
        }
    }
}

impl<'a, 'r> Relation<'r> for Restriction<'a> {
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

pub struct ConcatRelation<'a> {
    relations: Vec<Box<Relation<'a> >>,
    current: usize,
}

pub struct Projection<'a> {
    relation: Box<Relation<'a>>,
    colmap: Vec<usize>,
    colcount: usize,
}

impl<'a> Projection<'a> {
    pub fn new(rel: Box<Relation<'a>>, cols: Vec<String>) -> Projection<'a> {
        let mut colindexes = Vec::new();

        // make colindexes [-1, -1, -1, ...] same size as cols
        let minusone = -1;
        for j in 0..cols.len() {
            colindexes.push(minusone as usize);
        }

        for j in 0..cols.len() {
            let colname = &cols[j];

            for i in 0..rel.length() {
                if rel.name(i) == *colname {
                    colindexes[j] = i;
                }
            }
        }

        Projection {
            relation: rel,
            colmap: colindexes,
            colcount: cols.len(),
        }
    }
}

impl<'a, 'r> Relation<'r> for Projection<'a> {
    fn length(&self) -> usize {
        self.relation.length()
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

impl<'a> ConcatRelation<'a> {
    pub fn new() -> ConcatRelation<'a> {
        ConcatRelation {
            relations: Vec::new(),
            current: 0,
        }
    }
    pub fn add(&mut self, rel: Box<Relation<'a>>) {
        self.relations.push(rel);
    }
}

impl<'a, 'r> Relation<'r> for ConcatRelation<'a> {
    fn length(&self) -> usize {
        self.relations[self.current].length()
    }
    fn read(&mut self) -> bool {
        loop {
            if self.current < self.relations.len() {
                let done = self.relations[self.current].read();
                if done {
                    self.current += 1;
                } else {
                    return false;
                }
            } else {
                // done=true because no relations
                println!("no rels!");
                return true;
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

struct Rels<'a> {
    namemap: HashMap<String, Box<Relation<'a>>>,
}

impl<'a> Rels<'a> {
    fn new() -> Rels<'a> {
        Rels { namemap: HashMap::new() }
    }
    fn add(&mut self, name: String, rel: Box<Relation<'a>>) {
        self.namemap.insert(name, rel);
    }
}

struct ParseError {
}

const space: u8 = ' ' as u8;
const tab: u8 = '\t' as u8;
const cr: u8 = '\r' as u8;
const lf: u8 = '\n' as u8;
const dquote: u8 = '"' as u8;
const lbrace: u8 = '{' as u8;
const rbrace: u8 = '}' as u8;

fn expect(s: &[u8], s2: &str, i: usize) -> usize {
    let mut j = i;
    let u = s2.as_bytes();
    while j < s.len() && (s[j] == space || s[j] == tab) {
        j += 1;
    }
    let mut k = 0;
    while j < s.len() && k < u.len() && s[j + k] == u[k] {
        k += 1;
    }
    // return j + k if u fully matched
    if k == u.len() {
        j + k
    } else {
        i
    }
}

fn parse_token(s: &[u8], i: usize) -> (String, usize) {
    let mut j = i;
    while j < s.len() && (s[j] == space || s[j] == tab) {
        j += 1;
    }
    let mut k = j;
    if k < s.len() && s[k] == dquote { // quoted text
        k += 1;
        while k < s.len() && s[k] != dquote {
            k += 1;
        }
        if k < s.len() && s[k] == dquote {
            k += 1;
        }
        // signal an error on CRLF/EOF ?
    } else if k < s.len() && s[k] == lbrace { // { braces }
        while k < s.len() && s[k] != rbrace {
            k += 1;
        }
        if k < s.len() && s[k] == rbrace {
            k += 1;
        }
        // signal an error on CRLF/EOF ?
    } else {
        // standard token
        while k < s.len() && s[k] != space && s[k] != tab && s[k] != cr && s[k] != lf {
            k += 1;
        }
    }
    let t = String::from_utf8_lossy(&s[j..k]).to_string(); // TODO
    (t, k)
}

// parser
// -------------------------------------
// a = file "name"
// b = project a cid bid xid
// c = rename b cid -> id bix -> ix
// d = restrict c {id > 3 && id < 6}
// e = dedup d [col1] [col2] {cond1} {cond2} {cond3}
// f = diff a b  # items in 'a' that are not in 'b'
// f = diff a.id b.id
// g = union a b # a and b must have same schema
// i = sort h.xid asc
// j = inner_join h.id j.id # intersection
// k = left_join h.id j.id # j will have schema with nulls
// l = outer_join
// x = file_union "*.glob" "*.glob2"
//
fn parse_relalg(s: &[u8]) -> Option<Rels> { // Result
    let mut r = Rels::new();

    let (name, i) = parse_token(s, 0);
    let j = expect(s, "=", i);
    let (reltype, k) = parse_token(s, j);
    match reltype.as_str() {
        "file" => {
            let (filename, l) = parse_token(s, k);
            let m = expect(s, "\n", l);
            if m == l {
                println!("expecting eol after filename in 'file'");
                return None;
            }
            let fr = FileRelation::new(filename.as_str()).unwrap();
            r.add(name, Box::new(fr));
        },
        "file_union" => {
            let mut co = ConcatRelation::new();
            let mut start = k;
            loop {
                let (filename, l) = parse_token(s, start);

                let fr = FileRelation::new(filename.as_str()).unwrap();
                co.add(Box::new(fr));

                let m = expect(s, "\n", l);
                if m != l {
                    break;
                } else {
                    start = m;
                }
            }
            r.add(name, Box::new(co));
        },
        _ => {
            println!("unknown rel {}", reltype);
        }
    }

    Some(r)
}

#[test]
fn test_parsing() {
    let s = "hello = world {b r a c e} \"q u o t {e} d\"";
    let u = s.as_bytes();
    let (t1, i) = parse_token(u, 0);
    let j = expect(u, "=", i);
    let (t2, k) = parse_token(u, j);
    let (t3, l) = parse_token(u, k);
    let (t4, m) = parse_token(u, l);
    assert!(t1 == "hello");
    assert!(i != j);
    assert!(t2 == "world");
    assert!(t3 == "{b r a c e}");
    assert!(t4 == "\"q u o t {e} d\"");
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
        println!("read done {}", done);
        if done {
            break;
        }
        for i in 0..rr.length() {
            println!("val {} is {:?}", i, rr.value(i));
        }
    }
    println!("done");
}
