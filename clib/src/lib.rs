use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_uint, c_ulong};
use std::io::{Seek, SeekFrom};
use std::fs::{File, OpenOptions};
use std::cmp::min;
use std::sync::{Once, ONCE_INIT};
extern crate libc;
use libc::{memcpy, c_void};
use std::mem::transmute;
use flatfile::v2::schema2::Schema;

extern crate flatfile;
use flatfile::v2::schema2::Schema2;
use flatfile::{ColumnType, MmapBuf, read_schema_v2, ColumnValue, schema_read_row, write_schema_v2, schema_write, ReadFileBuf, FileBuf};

struct WriteFile {
    f: FileBuf,
    schema_handle: usize,
    current: Vec<ColumnValue>,
}

struct ReadFile {
    m: MmapBuf,
    schema: Schema2,
    current: Vec<ColumnValue>,
}

struct State {
    schema_counter: usize,
    vec: Vec<Schema2>,

    reading: Vec<Option<ReadFile>>,
    writing: Vec<Option<WriteFile>>,
}

fn get_state() -> &'static mut State {
    static mut STATE: *mut State = 0 as *mut State;
    static ONCE: Once = ONCE_INIT;

    unsafe {
        ONCE.call_once(|| {
            let state = State {
                vec: Vec::new(),
                schema_counter: 1,
                reading: Vec::new(),
                writing: Vec::new(),
            };
            STATE = transmute(Box::new(state));
        });
        &mut *STATE
    }
}

fn put_read_handle(rf: ReadFile) -> usize {
    let st = get_state();

    for n in 0..st.reading.len() {
        if st.reading[n].is_none() {
            st.reading[n] = Some(rf);
            return n;
        }
    }

    st.reading.push(Some(rf));
    st.reading.len() - 1
}

fn put_write_handle(rf: WriteFile) -> usize {
    let st = get_state();

    for n in 0..st.writing.len() {
        if st.writing[n].is_none() {
            st.writing[n] = Some(rf);
            return n;
        }
    }

    st.writing.push(Some(rf));
    st.writing.len() - 1
}

fn get_read_handle(handle: usize) -> &'static mut ReadFile {
    let st = get_state();
    st.reading[handle].as_mut().unwrap()
}

fn get_write_handle(handle: usize) -> &'static mut WriteFile {
    let st = get_state();
    st.writing[handle].as_mut().unwrap()
}

fn clear_write_handle(handle: usize) {
    let st = get_state();
    st.writing[handle] = None
}

fn clear_read_handle(handle: usize) {
    let st = get_state();
    st.reading[handle] = None
}

fn put_handle(sch: Schema2) -> usize {
    let st = get_state();
    st.vec.push(sch);
    let mut z = st.vec.len() - 1;
    z = z ^ 0x55AA55AA;
    z
}

fn get_schema_by_handle(handle: usize) -> &'static mut Schema2 {
    let st = get_state();
    let decoded = handle ^ 0x55AA55AA;
    if decoded >= st.vec.len() {
        panic!("get_schema_by_handle: handle={:x} decoded={} len={}",
               handle, decoded, st.vec.len());
    }
    &mut st.vec[decoded]
}

#[no_mangle]
pub extern fn schema2_create() -> c_ulong {
    let sch = Schema2::new();
    let x = put_handle(sch);
    x as c_ulong
}

#[no_mangle]
pub extern fn schema2_len(handle: usize) -> c_uint {
    get_schema_by_handle(handle).len() as c_uint
}

#[no_mangle]
pub extern fn schema2_destroy(handle: usize) {
}

#[no_mangle]
pub extern fn schema2_add_column(handle: usize,
                                 name: *const c_char,
                                 ctype: *const c_char,
                                 nullable: bool) {
    let n = unsafe { CStr::from_ptr(name) }.to_str().unwrap();
    let cts = unsafe { CStr::from_ptr(ctype) }.to_str().unwrap();
    let sch = get_schema_by_handle(handle);
    let ct = match cts {
        "u32" => ColumnType::U32le,
        "u64" => ColumnType::U64le,
        "string" => ColumnType::String,
        _ => panic!("unknown ctype in schema2_add_column"),
    };
    sch.add(n, ct, nullable);
}

#[no_mangle]
pub extern fn schema2_get_column_name(handle: usize,
                                      index: usize,
                                      buf: *mut c_char) {
    let sch = get_schema_by_handle(handle);
    let name = sch.name(index);
    let s = CString::new(name).unwrap();
    unsafe {
        libc::strcpy(buf, s.as_ptr());
    }
}

#[no_mangle]
pub extern fn schema2_get_column_type(handle: usize,
                                      index: usize,
                                      buf: *mut c_char) {
    let sch = get_schema_by_handle(handle);
    let name = match sch.ctype(index) {
        ColumnType::U32le => "u32",
        ColumnType::U64le => "u64",
        ColumnType::String => "string",
    };
    let s = CString::new(name).unwrap();
    unsafe {
        libc::strcpy(buf, s.as_ptr());
    }
}

#[no_mangle]
pub extern fn schema2_get_column_nullable(handle: usize,
                                          index: usize) -> bool {
    let sch = get_schema_by_handle(handle);
    sch.nullable(index)
}

#[no_mangle]
pub extern fn writef_get_schema(handle: c_uint) -> c_uint {
    let wf = get_write_handle(handle as usize);
    wf.schema_handle as c_uint
}

#[no_mangle]
pub extern fn writef_create(name: *const c_char,
                            schema_handle: usize) -> c_uint {
    let fname = unsafe { CStr::from_ptr(name) }.to_str().unwrap();
    let mut f = File::create(fname).unwrap();
    let schema = get_schema_by_handle(schema_handle);
    let mut filebuf = FileBuf::new(f, 4096);
    write_schema_v2(&mut filebuf, &schema);

    let mut writevec = Vec::new();
    for n in 0..schema.len() {
        writevec.push(ColumnValue::Null);
    }

    let h = put_write_handle(WriteFile {
        f: filebuf,
        schema_handle: schema_handle,
        current: writevec,
    });

    h as c_uint
}

#[no_mangle]
pub extern fn writef_close(handle: c_uint) {
    let h = handle as usize; // TBD
    clear_write_handle(h);
}

#[no_mangle]
pub extern fn writef_flush(handle: c_uint) {
    let h = handle as usize; // TBD
    let wf = get_write_handle(h);
    wf.f.flush_all();
}

#[no_mangle]
pub extern fn readf_clone_schema(handle: c_uint) -> c_uint {
    let rf = get_read_handle(handle as usize);
    let s = rf.schema.clone();
    let slen = s.len();
    let sch = put_handle(s) as c_uint;
    sch
}

#[no_mangle]
pub extern fn writef_open(name: *const c_char) -> c_uint {
    let fname = unsafe { CStr::from_ptr(name) }.to_str().unwrap();

    // read schema from the file
    let sch = {
        let mut f = File::open(fname).unwrap();
        let mut filebuf = ReadFileBuf::new(f, 4096);
        read_schema_v2(&mut filebuf).unwrap()
    };

    let mut writevec = Vec::new();
    for n in 0..sch.len() {
        writevec.push(ColumnValue::Null);
    }

    let mut f = OpenOptions::new().append(true).open(fname).unwrap();

    let mut filebuf = FileBuf::new(f, 4096);

    let schema_handle = put_handle(sch);

    let h = put_write_handle(WriteFile {
        f: filebuf,
        schema_handle: schema_handle,
        current: writevec,
    });

    h as c_uint
}

#[no_mangle]
pub extern fn readf_open(name: *const c_char) -> c_uint {
    let fname = unsafe { CStr::from_ptr(name) }.to_str().unwrap();

    match File::open(fname) {
        Ok(mut f) => {
            let mut mmapbuf = MmapBuf::new(f);
            let sch = read_schema_v2(&mut mmapbuf).unwrap();

            let mut readvec = Vec::new();
            for n in 0..sch.len() {
                readvec.push(ColumnValue::Null);
            }

            let h = put_read_handle(ReadFile { m: mmapbuf,
                                               schema: sch,
                                               current: readvec });
            h as c_uint
        },
        Err(e) => {
            println!("File::open {:?}", e);
            let r: i32 = -1;
            r as c_uint
        }
    }
}

#[no_mangle]
pub extern fn readf_close(handle: c_uint) {
    let h = handle as usize; // TBD
    clear_read_handle(h);
}

#[no_mangle]
pub extern fn readf_row_start(fhandle: c_uint) -> c_uint {
    let rf = get_read_handle(fhandle as usize);
    for n in 0..rf.current.len() {
        rf.current[n] = ColumnValue::Null;
    }

    let result = schema_read_row(&mut rf.m, rf.current.as_mut_slice(),
                                 &rf.schema);
    result as c_uint
}

#[no_mangle]
pub extern fn readf_row_end(fhandle: c_uint) {
}

#[no_mangle]
pub extern fn readf_row_get_column(fhandle: c_uint) {
    let rf = get_read_handle(fhandle as usize);

}

#[no_mangle]
pub extern fn readf_row_is_null(fhandle: c_uint, index: c_uint) -> c_uint {
    let rf = get_read_handle(fhandle as usize);
    let uindex = index as usize;
    let r = match rf.current[uindex] {
        ColumnValue::Null => true,
        _ => false,
    };
    r as c_uint
}

#[no_mangle]
pub extern fn readf_row_get_u32(fhandle: c_uint, index: c_uint) -> c_uint {
    let rf = get_read_handle(fhandle as usize);
    let uindex = index as usize;
    match rf.current[uindex] {
        ColumnValue::U32 { v } => v,
        _ => panic!("column type not u32, index: {}, debug: {:?}", uindex, rf.current[uindex])
    }
}

#[no_mangle]
pub extern fn readf_row_get_u64(fhandle: c_uint, index: c_uint) -> c_ulong {
    let rf = get_read_handle(fhandle as usize);
    let uindex = index as usize;
    match rf.current[uindex] {
        ColumnValue::U64 { v } => v,
        _ => panic!("column type not u64 but {:?}", rf.current[uindex])
    }
}

#[no_mangle]
pub extern fn readf_row_get_string_len(fhandle: c_uint, index: c_uint) -> c_ulong {
    let rf = get_read_handle(fhandle as usize);
    let uindex = index as usize;
    match rf.current[uindex] {
        ColumnValue::String { ref v } => {
            let u = v.as_str().as_bytes();
            v.len() as c_ulong
        }
        ColumnValue::Null => {
            0
        },
        _ => panic!("column type not string rh={} i={} sch_len={}", fhandle, uindex, rf.schema.len())
    }
}

#[no_mangle]
pub extern fn readf_row_get_string(fhandle: c_uint, index: c_uint, out: *mut c_void, size: c_ulong) -> c_ulong {
    let rf = get_read_handle(fhandle as usize);
    let uindex = index as usize;
    match rf.current[uindex] {
        ColumnValue::String { ref v } => {
            let u = v.as_str().as_bytes();
            let sz = min(size as usize, u.len());
            unsafe {
                memcpy(out, u.as_ptr() as *const c_void, sz);
            }
            u.len() as c_ulong
        }
        ColumnValue::Null => {
            0
        },
        _ => panic!("column type not string rh={} i={} sch_len={}", fhandle, uindex, rf.schema.len())
    }
}

#[no_mangle]
pub extern fn writef_row_start(fhandle: c_uint) {
    let wf = get_write_handle(fhandle as usize);
    for n in 0..wf.current.len() {
        wf.current[n] = ColumnValue::Null;
    }
}

#[no_mangle]
pub extern fn writef_row_end(fhandle: c_uint) -> bool {
    let wf = get_write_handle(fhandle as usize);
    let schema = get_schema_by_handle(wf.schema_handle);
    schema_write(&mut wf.f, wf.current.as_slice(), &schema)
}

#[no_mangle]
pub extern fn writef_row_set_u32(fhandle: c_uint, index: c_uint, value: c_uint) {
    let wf = get_write_handle(fhandle as usize);
    let schema = get_schema_by_handle(wf.schema_handle);
    let uindex = index as usize;
    if uindex >= schema.len() {
        panic!("row_write_set_u32 uindex > schema.len()");
    }
    if schema.ctype(uindex) != ColumnType::U32le {
        panic!("row_write_set_u32 incorrect type");
    }
    wf.current[uindex] = ColumnValue::U32 { v: value as u32 };
}

#[no_mangle]
pub extern fn writef_row_set_u64(fhandle: c_uint, index: c_uint, value: c_ulong) {
    let wf = get_write_handle(fhandle as usize);
    let schema = get_schema_by_handle(wf.schema_handle);
    let uindex = index as usize;
    if uindex >= schema.len() {
        panic!("row_write_set_u64 uindex > schema.len()");
    }
    if schema.ctype(uindex) != ColumnType::U64le {
        panic!("row_write_set_u64 incorrect type");
    }
    wf.current[uindex] = ColumnValue::U64 { v: value as u64 };
}

#[no_mangle]
pub extern fn writef_row_set_string(fhandle: c_uint, index: c_uint, value: *const c_char) {
    let wf = get_write_handle(fhandle as usize);
    let schema = get_schema_by_handle(wf.schema_handle);
    let uindex = index as usize;
    if uindex >= schema.len() {
        panic!("row_write_set_string uindex > schema.len()");
    }
    if schema.ctype(uindex) != ColumnType::String {
        panic!("row_write_set_string incorrect type");
    }
    let value = unsafe { CStr::from_ptr(value) }.to_str().unwrap();
    wf.current[uindex] = ColumnValue::String { v: value.to_owned() };
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
