from flatfile import Reader, Writer, Appender
import _flatfile as flatfile

sch = flatfile.schema2_create()
flatfile.schema2_add_column(sch, "first", "u32", False)
flatfile.schema2_add_column(sch, "second", "u64", False)
flatfile.schema2_add_column(sch, "third", "string", False)

def test_schema():
    assert flatfile.schema2_len(sch) == 3

    assert flatfile.schema2_get_column_name(sch, 0) == "first"
    assert flatfile.schema2_get_column_type(sch, 0) == "u32"
    assert flatfile.schema2_get_column_nullable(sch, 0) is False

    assert flatfile.schema2_get_column_name(sch, 1) == "second"
    assert flatfile.schema2_get_column_type(sch, 1) == "u64"
    assert flatfile.schema2_get_column_nullable(sch, 1) is False

    assert flatfile.schema2_get_column_name(sch, 2) == "third"
    assert flatfile.schema2_get_column_type(sch, 2) == "string"
    assert flatfile.schema2_get_column_nullable(sch, 2) is False

rw_schema = [
    [ "a", "u32", False ],
    [ "b", "u32", False ],
    [ "c", "u32", True ],
    [ "d", "u64", True ],
    [ "e", "string", True ],
]

def test_writer():
    with Writer("/tmp/_test2.dat", rw_schema) as w:
        w.write_row([1, 2, None, 64, "hello"])

def test_reader():
    with Reader("/tmp/_test2.dat", rw_schema) as r:
        for row in r.fetch():
            print (row)

def test_reader_no_schema():
    with Reader("/tmp/_test2.dat", None) as r:
        for row in r.fetch():
            print (row)

def test_appender():
    with Appender("/tmp/_test2.dat", rw_schema) as a:
        print("a", a.schema)
        r = a.write_row([2, 4, 5, None, "world"])
        assert r is True

def test_write():
    wh = flatfile.writef_create("/tmp/_test.dat", sch)
    flatfile.writef_row_start(wh)
    flatfile.writef_row_set_u32(wh, 0, 12345678)
    flatfile.writef_row_set_u64(wh, 1, 33445566)
    flatfile.writef_row_set_string(wh, 2, "qwertystring")
    flatfile.writef_row_end(wh)

    flatfile.writef_row_start(wh)
    flatfile.writef_row_set_u32(wh, 0, 55555555)
    flatfile.writef_row_set_u64(wh, 1, 99999999)
    flatfile.writef_row_set_string(wh, 2, "anotherstring")
    flatfile.writef_row_end(wh)

    flatfile.writef_close(wh)

def read_row(rh, types):
    if flatfile.readf_row_start(rh):
        r = []
        for index, t in enumerate(types):
            if t == 'u32':
                val = flatfile.readf_row_get_u32(rh, index)
            elif t == 'u64':
                val = flatfile.readf_row_get_u64(rh, index)
            elif t == 'string':
                val = flatfile.readf_row_get_string(rh, index)
            r.append(val)
        flatfile.readf_row_end(rh)
        return r
    return None

def test_read():
    rh = flatfile.readf_open("/tmp/_test.dat")
    print (read_row(rh, ['u32', 'u64', 'string']))
    print (read_row(rh, ['u32', 'u64', 'string']))
    print (read_row(rh, ['u32', 'u64', 'string']))
    flatfile.readf_close(rh)

    flatfile.schema2_destroy(sch)

def test_read2():
    rh = flatfile.readf_open("/tmp/_test.dat")
    sch = flatfile.readf_clone_schema(rh);
    assert flatfile.schema2_len(sch) == 3

    assert flatfile.schema2_get_column_name(sch, 0) == "first"
    assert flatfile.schema2_get_column_type(sch, 0) == "u32"
    assert flatfile.schema2_get_column_nullable(sch, 0) is False

    assert flatfile.schema2_get_column_name(sch, 1) == "second"
    assert flatfile.schema2_get_column_type(sch, 1) == "u64"
    assert flatfile.schema2_get_column_nullable(sch, 1) is False

    assert flatfile.schema2_get_column_name(sch, 2) == "third"
    assert flatfile.schema2_get_column_type(sch, 2) == "string"
    assert flatfile.schema2_get_column_nullable(sch, 2) is False
    flatfile.readf_close(rh);
    flatfile.schema2_destroy(sch)

test_schema()
test_write()
test_read()
test_read2()
test_writer()
test_appender()
test_reader()
test_reader_no_schema()
