import sys
import _flatfile as flatfile

rh = flatfile.readf_open(sys.argv[1])
sch = flatfile.readf_clone_schema(rh)
slen = flatfile.schema2_len(sch)

types = []
for i in range(0, slen):
    n = flatfile.schema2_get_column_name(sch, i)
    t = flatfile.schema2_get_column_type(sch, i)
    nl = flatfile.schema2_get_column_nullable(sch, i)
    print("{} {} {}".format(n, t, nl))
    types.append(t)

def read_row(rh, types):
    if flatfile.readf_row_start(rh):
        r = []
        for index, t in enumerate(types):
            if flatfile.readf_row_is_null(rh, index):
                val = None
            elif t == "u32":
                val = flatfile.readf_row_get_u32(rh, index)
            elif t == "u64":
                val = flatfile.readf_row_get_u64(rh, index)
            elif t == "string":
                val = flatfile.readf_row_get_string(rh, index)
            else:
                print("unknown type", t)
            r.append(val)
        flatfile.readf_row_end(rh)
        return r
    print("read_row_start -> false")
    return None


counter = 0
while True:
    r = read_row(rh, types)
    if r is not None:
        counter += 1
        print(r)
    else:
        break
print ("Number of rows", counter)
