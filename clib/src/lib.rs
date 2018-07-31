use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uint, c_ulong};
use std::fs::{File, OpenOptions};
use std::cmp::min;
use std::sync::{Once, ONCE_INIT};
extern crate libc;
use libc::{memcpy, c_void};
use std::mem::transmute;
use std::collections::HashMap;
use flatfile::v2::schema2::Schema;

extern crate flatfile;
use flatfile::v2::schema2::Schema2;
use flatfile::v2::filebuf::FileBuf;
use flatfile::{ColumnType, read_schema_v2, ColumnValue, write_schema_v2, schema_write, ReadFileBuf, FileRelation, create_relation, Relation };

enum Handle {
    WriteFile {
        f: FileBuf,
        schema: Schema2,
        current: Vec<ColumnValue>,
    },
    ReadRelation {
        rel: Box<Relation>,
    },
    Schema {
        schema: Schema2,
    },
    Freed,
}

struct State {
    handles: Vec<Handle>,
}

fn get_state() -> &'static mut State {
    static mut STATE: *mut State = 0 as *mut State;
    static ONCE: Once = ONCE_INIT;

    unsafe {
        ONCE.call_once(|| {
            let state = State {
                handles: Vec::new(),
            };
            STATE = transmute(Box::new(state));
        });
        &mut *STATE
    }
}

fn put_handle(h: Handle) -> usize {
    let st = get_state();

    st.handles.push(h);

    let k = st.handles.len() - 1;
    let z = k ^ 0x55AA55AA;
    z
}

fn get_handle(handle: usize) -> &'static mut Handle {
    let st = get_state();
    let k = handle ^ 0x55AA55AA;

    if k >= st.handles.len() {
        panic!("get_handle: invalid handle passed to get_handle. handle={:x} decoded={} len={}",
               handle, k, st.handles.len());
    }
    &mut st.handles[k]
}

fn clear_handle(handle: usize) {
    let st = get_state();
    let k = handle ^ 0x55AA55AA;
    st.handles[k] = Handle::Freed
}

#[no_mangle]
pub extern fn schema2_create() -> c_ulong {
    let sch = Schema2::new();
    let x = put_handle(Handle::Schema { schema: sch });
    x as c_ulong
}

#[no_mangle]
pub extern fn schema2_len(handle: usize) -> c_uint {
    let h = get_handle(handle);
    let len = match h {
        Handle::Freed => {
            panic!("schema2_len called on a freed handle");
        },
        Handle::WriteFile { schema, .. } => {
            schema.len()
        },
        Handle::ReadRelation { rel } => {
            rel.length()
        },
        Handle::Schema { schema } => {
            schema.len()
        }
    };
    len as c_uint
}

#[no_mangle]
pub extern fn schema2_destroy(handle: usize) {
    clear_handle(handle);
}

#[no_mangle]
pub extern fn schema2_add_column(handle: usize,
                                 name: *const c_char,
                                 ctype: *const c_char,
                                 nullable: bool) {
    let n = unsafe { CStr::from_ptr(name) }.to_str().unwrap();
    let cts = unsafe { CStr::from_ptr(ctype) }.to_str().unwrap();
    let handle = get_handle(handle);

    let ct = match cts {
        "u32" => ColumnType::U32le,
        "u64" => ColumnType::U64le,
        "string" => ColumnType::String,
        _ => panic!("unknown ctype in schema2_add_column"),
    };

    match handle {
        Handle::Schema { schema } => {
            schema.add(n, ct, nullable);
        },
        _ => {
            panic!("schema2_add_column: operation not supported for this type");
        },
    };
}

#[no_mangle]
pub extern fn schema2_get_column_name(handle: usize,
                                      index: usize,
                                      buf: *mut c_char) {
    let h = get_handle(handle);
    let name = match h {
        Handle::Schema { schema } => CString::new(schema.name(index)),
        Handle::WriteFile { schema, .. } => CString::new(schema.name(index)),
        Handle::ReadRelation { rel } => CString::new(rel.name(index)),
        Handle::Freed => panic!("schema2_get_column_name called on a freed handle"),
    };

    let s = name.unwrap();

    unsafe {
        libc::strcpy(buf, s.as_ptr());
    }
}

#[no_mangle]
pub extern fn schema2_get_column_type(handle: usize,
                                      index: usize,
                                      buf: *mut c_char) {
    let h = get_handle(handle);

    let ct = match h {
        Handle::Freed => panic!("schema2_get_column_type called on a freed handle"),
        Handle::WriteFile { schema, .. } => schema.ctype(index),
        Handle::Schema { schema } => schema.ctype(index),
        Handle::ReadRelation { rel, .. } => rel.ctype(index),
    };

    let name = match ct {
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
    let h = get_handle(handle);
    match h {
        Handle::Freed => panic!("schema2_get_column_nullable called on a freed handle"),
        Handle::WriteFile { schema, .. } => schema.nullable(index),
        Handle::ReadRelation { rel } => rel.nullable(index),
        Handle::Schema { schema } => schema.nullable(index),
    }
}

#[no_mangle]
pub extern fn writef_get_schema(handle: c_uint) -> c_uint {
    handle
}

#[no_mangle]
pub extern fn writef_create(name: *const c_char,
                            schema_handle: usize) -> c_uint {
    let fname = unsafe { CStr::from_ptr(name) }.to_str().unwrap();
    let f = File::create(fname).unwrap();

    match get_handle(schema_handle) {
        Handle::Schema { schema } => {
            let mut filebuf = FileBuf::new(f, 4096);
            write_schema_v2(&mut filebuf, &schema);

            // create a row to store values for write
            let mut writevec = Vec::new();
            for _ in 0..schema.len() {
                writevec.push(ColumnValue::Null);
            }

            let h = put_handle(Handle::WriteFile {
                f: filebuf,
                schema: schema.to_owned(),
                current: writevec,
            });

            h as c_uint
        },
        _ => {
            panic!("schema handle passed to writef_create is not a schema");
        }
    }
}

#[no_mangle]
pub extern fn writef_close(handle: c_uint) {
    let h = handle as usize; // TBD
    clear_handle(h);
}

#[no_mangle]
pub extern fn writef_flush(handle: c_uint) -> bool {
    let h = handle as usize; // TBD
    match get_handle(h) {
        Handle::WriteFile { f, .. } => f.flush_all(),
        _ => panic!("writef_flush() called with no write handle")
    }
}

#[no_mangle]
pub extern fn readf_clone_schema(handle: c_uint) -> c_uint {
    handle
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
    for _ in 0..sch.len() {
        writevec.push(ColumnValue::Null);
    }

    let f = OpenOptions::new().append(true).open(fname).unwrap();

    let filebuf = FileBuf::new(f, 4096);

    let h = put_handle(Handle::WriteFile {
        f: filebuf,
        schema: sch,
        current: writevec,
    });

    h as c_uint
}

#[no_mangle]
pub extern fn readf_open_relation(name: *const c_char, reldef: *const c_char) -> c_int {
    let rname = unsafe { CStr::from_ptr(name) }.to_str().unwrap();
    let def = unsafe { CStr::from_ptr(reldef) }.to_str().unwrap();
    let vars = HashMap::new();

    let r = create_relation(&rname, &def, &vars);
    match r {
        Some(b) => {
            let h = put_handle(Handle::ReadRelation { rel: b });
            h as c_int
        },
        None => {
            let z: u32 = (-1 as i32) as u32;
            z as c_int
        }
    }
}

#[no_mangle]
pub extern fn readf_open(name: *const c_char) -> c_int {
    let fname = unsafe { CStr::from_ptr(name) }.to_str().unwrap();

    let filerel = FileRelation::new(fname);

    match filerel {
        Ok(rel) => {
            let h = put_handle(Handle::ReadRelation { rel: Box::new(rel) });
            h as c_int
        },
        Err(e) => {
            println!("readf_open(): error={:?} fname={}", e, fname);
            let r: i32 = -1;
            r as c_int
        }
    }
}

#[no_mangle]
pub extern fn readf_close(handle: c_uint) {
    let h = handle as usize; // TBD
    clear_handle(h);
}

#[no_mangle]
pub extern fn readf_row_start(fhandle: c_uint) -> c_uint {
    match get_handle(fhandle as usize) {
        Handle::ReadRelation { rel } => rel.read() as c_uint,
        _ => panic!("readf_row_start called on a non-read handle"),
    }
}

#[no_mangle]
pub extern fn readf_row_end(_fhandle: c_uint) {
}

#[no_mangle]
pub extern fn readf_row_is_null(fhandle: c_uint, index: c_uint) -> c_uint {
    let result = match get_handle(fhandle as usize) {
        Handle::ReadRelation { rel } => {
            match rel.value(index as usize) {
                ColumnValue::Null => true,
                _ => false,
            }
        },
        _ => panic!("readf_row_is_null called on a non read handle"),
    };
    result as c_uint
}

#[no_mangle]
pub extern fn readf_row_get_u32(fhandle: c_uint, index: c_uint) -> c_uint {
    let value = match get_handle(fhandle as usize) {
        Handle::ReadRelation { rel } => {
            match rel.value(index as usize) {
                ColumnValue::U32 { v } => *v,
                _ => panic!("column type not u32, index: {}, debug: {:?}", index, rel.ctype(index as usize)),
            }
        },
        _ => panic!("readf_row_get_u32 called on a non-read handle"),
    };
    value as c_uint
}

#[no_mangle]
pub extern fn readf_row_get_u64(fhandle: c_uint, index: c_uint) -> c_ulong {
    let result = match get_handle(fhandle as usize) {
        Handle::ReadRelation { rel } => {
            match rel.value(index as usize) {
                ColumnValue::U64 { v } => *v,
                _ => panic!("column type not u64 but {:?}", rel.ctype(index as usize)),
            }
        },
        _ => panic!("readf_row_get_u64 called on a non-read handle"),
    };
    result as c_ulong
}

#[no_mangle]
pub extern fn readf_row_get_string_len(fhandle: c_uint, index: c_uint) -> c_ulong {
    match get_handle(fhandle as usize) {
        Handle::ReadRelation { rel } => {
            match rel.value(index as usize) {
                ColumnValue::String { ref v } => {
                    let u = v.as_str().as_bytes();
                    u.len() as c_ulong
                }
                ColumnValue::Null => {
                    0
                },
                _ => panic!("column type not string rh={} i={} sch_len={}", fhandle, index, rel.length())
            }
        },
        _ => panic!("readf_row_get_string_len called on a non-read handle"),
    }
}

#[no_mangle]
pub extern fn readf_row_get_string(fhandle: c_uint, index: c_uint, out: *mut c_void, size: c_ulong) -> c_ulong {
    match get_handle(fhandle as usize) {
        Handle::ReadRelation { rel } => {
            match rel.value(index as usize) {
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
                _ => panic!("column type not string rh={} i={} sch_len={}", fhandle, index, rel.length())
            }
        }
        _ => panic!("readf_row_get_string called on a non-read handle")
    }
}

#[no_mangle]
pub extern fn writef_row_start(fhandle: c_uint) {
    match get_handle(fhandle as usize) {
        Handle::WriteFile { current, .. } => {
            for n in 0..current.len() {
                current[n] = ColumnValue::Null;
            }
        },
        _ => panic!("writef_row_start called on a non-write handle"),
    }
}

#[no_mangle]
pub extern fn writef_row_end(fhandle: c_uint) -> bool {
    match get_handle(fhandle as usize) {
        Handle::WriteFile { ref mut f, ref current, schema } => {
            schema_write(f, current.as_slice(), &schema)
        }
        _ => panic!("writef_row_end called on a non-write handle"),
    }
}

#[no_mangle]
pub extern fn writef_row_set_u32(fhandle: c_uint, index: c_uint, value: c_uint) {
    match get_handle(fhandle as usize) {
        Handle::WriteFile { schema, current, .. } => {
            if (index as usize) >= schema.len() {
                panic!("row_write_set_u32 uindex > schema.len()");
            }
            if schema.ctype(index as usize) != ColumnType::U32le {
                panic!("row_write_set_u32 incorrect type");
            }
            current[index as usize] = ColumnValue::U32 { v: value as u32 };
        },
        _ => panic!("writef_row_set_u32 called on a non-write handle"),
    }
}

#[no_mangle]
pub extern fn writef_row_set_u64(fhandle: c_uint, index: c_uint, value: c_ulong) {
    match get_handle(fhandle as usize) {
        Handle::WriteFile { schema, current, .. } => {
            if (index as usize) >= schema.len() {
                panic!("row_write_set_u64 uindex > schema.len()");
            }
            if schema.ctype(index as usize) != ColumnType::U64le {
                panic!("row_write_set_u64 incorrect type");
            }
            current[index as usize] = ColumnValue::U64 { v: value as u64 };
        },
        _ => panic!("writef_row_set_u64 called on a non-write handle"),
    }
}

#[no_mangle]
pub extern fn writef_row_set_string(fhandle: c_uint, index: c_uint, value: *const c_char) {
    match get_handle(fhandle as usize) {
        Handle::WriteFile { schema, current, .. } => {
            if (index as usize) >= schema.len() {
                panic!("row_write_set_string uindex > schema.len()");
            }
            if schema.ctype(index as usize) != ColumnType::String {
                panic!("row_write_set_string incorrect type");
            }
            let value = unsafe { CStr::from_ptr(value) }.to_str().unwrap();
            current[index as usize] = ColumnValue::String { v: value.to_owned() };
        },
        _ => panic!("writef_row_set_string called on a non-write handle"),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
