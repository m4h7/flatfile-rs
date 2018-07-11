import _flatfile


class Reader:
    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.h = None
        self.sch = None

    def fetch(self):
        while True:
            row = self.read_row()
            if row:
                yield row
            else:
                break

    def read_row(self):
        if not _flatfile.readf_row_start(self.h):
            return None
        val = []
        for index, item in enumerate(self.schema):
            name, _type, nullable = item
            if nullable and _flatfile.readf_row_is_null(self.h, index):
                v = None
            elif _type == "u32":
                v = _flatfile.readf_row_get_u32(self.h, index)
            elif _type == "u64":
                v = _flatfile.readf_row_get_u64(self.h, index)
            elif _type == "string":
                v = _flatfile.readf_row_get_string(self.h, index)
            else:
                raise Exception(
                    "unknown type in schema: #{}. {} {} {}",
                    index,
                    name,
                    _type,
                    nullable,
                )
            val.append(v)
        _flatfile.readf_row_end(self.h)
        return val

    def print_schema(self, schema):
        for n in range(0, len(schema)):
            name, _type, nullable = schema[n]
            print("{} {} {} {}", n, name, _type, nullable)

    def schema_error(self, expected, fileschema, reason):
        print("expected schema:")
        self.print_schema(expected)
        print("file schema:")
        self.print_schema(fileschema)
        print("reason:", reason)
        raise Exception("schema different")

    def __enter__(self):
        self.h = _flatfile.readf_open(self.filename)
        self.sch = _flatfile.readf_clone_schema(self.h)
        schema = []
        for n in range(0, _flatfile.schema2_len(self.sch)):
            item = [
                _flatfile.schema2_get_column_name(self.sch, n),
                _flatfile.schema2_get_column_type(self.sch, n),
                _flatfile.schema2_get_column_nullable(self.sch, n),
            ]
            schema.append(item)
        if self.schema is not None:  # compare
            if len(self.schema) != len(schema):
                self.schema_error(self.schema, schema, "length")
            for j in range(0, len(schema)):
                if schema[j][0] != self.schema[j][0]:
                    self.schema_error(self.schema, schema, "name")
                elif schema[j][1] != self.schema[j][1]:
                    self.schema_error(self.schema, schema, "type")
                elif schema[j][2] != self.schema[j][2]:
                    self.schema_error(self.schema, schema, "nullable")
        else:
            self.schema = schema
        return self

    def __exit__(self, *args):
        _flatfile.readf_close(self.h)
        self.h = None


class Writer:
    def __init__(self, filename, schema):
        self.filename = filename
        self.sch = _flatfile.schema2_create()
        for name, type_, nullable in schema:
            _flatfile.schema2_add_column(self.sch, name, type_, nullable)
        self.schema = schema

    def write_row(self, values):
        _flatfile.writef_row_start(self.h)
        for i in range(0, len(self.schema)):
            if i >= len(values) or values[i] is None:
                pass  # set nothing
            elif self.schema[i][1] == "u32":
                _flatfile.writef_row_set_u32(self.h, i, values[i])
            elif self.schema[i][1] == "u64":
                _flatfile.writef_row_set_u64(self.h, i, values[i])
            elif self.schema[i][1] == "string":
                _flatfile.writef_row_set_string(self.h, i, values[i])
            else:
                raise Exception("unknown type in schema {}".format(self.schema[i][1]))
        return _flatfile.writef_row_end(self.h)

    def __enter__(self):
        self.h = _flatfile.writef_create(self.filename, self.sch)
        return self

    def __exit__(self, *args):
        _flatfile.writef_close(self.h)
        self.h = None


class Appender:
    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema

    def write_dict(self, d):
        _flatfile.writef_row_start(self.h)
        for i in range(0, len(self.schema)):
            name, _, _ = self.schema[i]
            if name not in d or d[name] is None:
                pass  # set nothing
            elif self.schema[i][1] == "u32":
                _flatfile.writef_row_set_u32(self.h, i, d[name])
            elif self.schema[i][1] == "u64":
                _flatfile.writef_row_set_u64(self.h, i, d[name])
            elif self.schema[i][1] == "string":
                _flatfile.writef_row_set_string(self.h, i, d[name])
            else:
                raise Exception("unknown type in schema {}".format(self.schema[i][1]))
        return _flatfile.writef_row_end(self.h)

    def write_row(self, values):
        _flatfile.writef_row_start(self.h)
        for i in range(0, len(self.schema)):
            if i >= len(values) or values[i] is None:
                pass  # set nothing
            elif self.schema[i][1] == "u32":
                _flatfile.writef_row_set_u32(self.h, i, values[i])
            elif self.schema[i][1] == "u64":
                _flatfile.writef_row_set_u64(self.h, i, values[i])
            elif self.schema[i][1] == "string":
                _flatfile.writef_row_set_string(self.h, i, values[i])
            else:
                raise Exception("unknown type in schema {}".format(self.schema[i][1]))
        return _flatfile.writef_row_end(self.h)

    def schema_error(self, expected, got, reason):
        print("appender schema error")
        print("expected", expected)
        print("got", got)
        print("reason", reason)
        raise Exception("Appender: {}".format(reason))

    def __enter__(self):
        self.h = _flatfile.writef_open(self.filename)
        self.sch = _flatfile.writef_get_schema(self.h)
        schema = []
        for n in range(0, _flatfile.schema2_len(self.sch)):
            item = [
                _flatfile.schema2_get_column_name(self.sch, n),
                _flatfile.schema2_get_column_type(self.sch, n),
                _flatfile.schema2_get_column_nullable(self.sch, n),
            ]
            schema.append(item)
        if self.schema is not None:  # compare
            if len(self.schema) != len(schema):
                self.schema_error(self.schema, schema, "length")
            for j in range(0, len(schema)):
                if schema[j][0] != self.schema[j][0]:
                    self.schema_error(self.schema, schema, "name")
                elif schema[j][1] != self.schema[j][1]:
                    self.schema_error(self.schema, schema, "type")
                elif schema[j][2] != self.schema[j][2]:
                    self.schema_error(self.schema, schema, "nullable")
        else:
            self.schema = schema
        return self

    def __exit__(self, *args):
        _flatfile.writef_close(self.h)
        self.h = None
