import flatfile

sch = flatfile.schema2_create()
flatfile.schema2_add_column(sch, "first", "u32", False)
flatfile.schema2_add_column(sch, "second", "u64", False)
flatfile.schema2_add_column(sch, "third", "string", False)

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

rh = flatfile.readf_open("/tmp/_test.dat")
print (read_row(rh, ['u32', 'u64', 'string']))
print (read_row(rh, ['u32', 'u64', 'string']))
print (read_row(rh, ['u32', 'u64', 'string']))
flatfile.readf_close(rh)

flatfile.schema2_destroy(sch)

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
