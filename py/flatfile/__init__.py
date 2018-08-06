import _flatfile
import os.path

class OpenError(Exception): pass

class Reader:
    def __init__(self, filename, schema = None, reldef = None):
        self.filename = filename
        self.schema = schema
        self.h = None
        self.sch = None
        self.reldef = reldef
        self._open()

    def _open(self):
        if self.reldef is not None:
            h = _flatfile.readf_open_relation(self.filename, self.reldef)
        else:
            h = _flatfile.readf_open(self.filename)

        if h == -1:
            raise OpenError("unable to open file/relation {}".format(self.filename))
        self.h = h

        self.sch = _flatfile.readf_clone_schema(self.h)

        schema = []
        schemalen = _flatfile.schema2_len(self.sch)

        for n in range(0, schemalen):
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

    def _close(self):
        if self.h is not None:
            _flatfile.readf_close(self.h)
        self.h = None

    def fetch_columns(self, columns):
        while True:
            row = self.read_columns(columns)
            if row is not None:
                yield row
            else:
                break

    def fetch(self):
        while True:
            row = self.read_row()
            if row is not None:
                yield row
            else:
                break

    def read_columns(self, columns):
        if not hasattr(columns, "__iter__"):
            raise Exception("columns argument must be an iterable")
        if isinstance(columns, str):
            raise Exception("columns argument is a string, expected iterable")
        for col in columns:
            found = False
            for name, _, _ in self.schema:
                if col == name:
                    found = True
            if not found:
                raise Exception(
                    "column {} is not in schema: {}".format(
                        col, self.schema
                        ))
        if not _flatfile.readf_row_start(self.h):
            return None
        val = []
        for index, item in enumerate(self.schema):
            name, _type, nullable = item
            if name in columns:
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
        print("========== SCHEMA ERROR ==========")
        print("EXPECTED SCHEMA:")
        self.print_schema(expected)
        print("FILE SCHEMA:")
        self.print_schema(fileschema)
        print("REASON:", reason)
        raise Exception("File schema is different from expected schema")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self._close()


class Writer:
    def __init__(self, filename, schema):
        self.filename = filename
        self.sch = _flatfile.schema2_create()
        for name, type_, nullable in schema:
            _flatfile.schema2_add_column(self.sch, name, type_, nullable)
        self.schema = schema
        self._open()

    def _open(self):
        h = _flatfile.writef_create(self.filename, self.sch)
        if h == -1:
            raise OpenError("Unable to create file {}".format(self.filename))
        self.h = h

    def _close(self):
        if self.h is not None:
            _flatfile.writef_close(self.h)
            self.h = None

    def write_row(self, values):
        _flatfile.writef_row_start(self.h)
        for i in range(0, len(self.schema)):
            if i >= len(values) or values[i] is None:
                pass  # set nothing (keep implicit null value)
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
        return self

    def __exit__(self, *args):
        self._close()

class Appender:
    def __init__(self, filename, schema):
        self.filename = filename
        self.schema = schema
        self.h = None
        self._open()

    def _open(self):
        if os.path.exists(self.filename) and os.path.getsize(self.filename) > 0:
            h = _flatfile.writef_open(self.filename)
            if h == -1:
                raise OpenError("Unable to open {} for writing".format(self.filename))
            self.h = h

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
        else:
            # file does not exist or is zero sized
            self.sch = _flatfile.schema2_create()
            for name, type_, nullable in self.schema:
                _flatfile.schema2_add_column(self.sch, name, type_, nullable)
            h = _flatfile.writef_create(self.filename, self.sch)
            if h == -1:
                raise OpenError("Unable to create file {}".format(self.filename))
            self.h = h

    def _close(self):
        if self.h is not None:
            _flatfile.writef_close(self.h)
            self.h = None

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
        self._open()
        return self

    def __exit__(self, *args):
        self._close()
