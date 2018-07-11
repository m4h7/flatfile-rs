#include "Python.h"
#include "flatfile.h"

static PyObject *
flatfile_schema2_create(PyObject *self, PyObject *args)
{
    unsigned long rc = schema2_create();
    return PyLong_FromLong(rc);
}

static PyObject *
flatfile_schema2_destroy(PyObject *self, PyObject *args)
{
}

static PyObject *
flatfile_schema2_len(PyObject *self, PyObject *args)
{
    unsigned int handle;
    if (!PyArg_ParseTuple(args, "i", &handle))
        return NULL;
    unsigned long rc = schema2_len(handle);
    return PyLong_FromLong(rc);
}

static PyObject *
flatfile_schema2_add_column(PyObject *self, PyObject *args)
{
    unsigned int handle;
    char const* name;
    char const* ctype;
    _Bool nullable;
    if (!PyArg_ParseTuple(args, "Issp", &handle, &name, &ctype, &nullable)) {
        return NULL;
    }
    schema2_add_column(handle, name, ctype, nullable);
    return PyLong_FromLong(0);
}

static PyObject *
flatfile_schema2_get_column_name(PyObject *self, PyObject *args)
{
    unsigned int handle = 0;
    unsigned int index = 0;
    if (!PyArg_ParseTuple(args, "II", &handle, &index)) {
        return NULL;
    }
    char buf[1024] = { 0 };
    schema2_get_column_name(handle, index, &buf);
    return PyUnicode_FromString(buf);
}

static PyObject *
flatfile_schema2_get_column_type(PyObject *self, PyObject *args)
{
    unsigned int handle = 0;
    unsigned int index = 0;
    if (!PyArg_ParseTuple(args, "II", &handle, &index)) {
        return NULL;
    }
    char buf[1024] = { 0 };
    schema2_get_column_type(handle, index, &buf);
    return PyUnicode_FromString(buf);
}

static PyObject *
flatfile_schema2_get_column_nullable(PyObject *self, PyObject *args)
{
    unsigned int handle = 0;
    unsigned int index = 0;
    if (!PyArg_ParseTuple(args, "II", &handle, &index)) {
        return NULL;
    }
    _Bool b = schema2_get_column_nullable(handle, index);
    return PyBool_FromLong(b);
}

static PyObject*
flatfile_readf_open(PyObject* self, PyObject* args) {
    char const* name = NULL;
    if (!PyArg_ParseTuple(args, "s", &name)) {
        return NULL;
    }
    unsigned int fhandle = readf_open(name);
    return PyLong_FromLong(fhandle);
}

static PyObject*
flatfile_writef_create(PyObject* self, PyObject* args) {
    char const* name = NULL;
    unsigned int schandle = 0;
    if (!PyArg_ParseTuple(args, "sI", &name, &schandle)) {
        return NULL;
    }
    unsigned int fhandle = writef_create(name, schandle);
    return PyLong_FromLong(fhandle);
}

static PyObject*
flatfile_writef_open(PyObject* self, PyObject* args) {
    char const* name = NULL;
    unsigned int schandle = 0;
    if (!PyArg_ParseTuple(args, "s", &name)) {
        return NULL;
    }
    unsigned int fhandle = writef_open(name);
    return PyLong_FromLong(fhandle);
}

static PyObject*
flatfile_readf_close(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    if (!PyArg_ParseTuple(args, "I", &fhandle)) {
        return NULL;
    }
    readf_close(fhandle);
    return PyLong_FromLong(0);
}

static PyObject*
flatfile_writef_close(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    if (!PyArg_ParseTuple(args, "I", &fhandle)) {
        return NULL;
    }
    writef_close(fhandle);
    return PyLong_FromLong(0);
}

static PyObject*
flatfile_readf_row_start(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    if (!PyArg_ParseTuple(args, "I", &fhandle)) {
        return NULL;
    }
    unsigned int r = readf_row_start(fhandle);
    return PyLong_FromLong(r);
}

static PyObject*
flatfile_writef_row_start(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    if (!PyArg_ParseTuple(args, "I", &fhandle)) {
        return NULL;
    }
    writef_row_start(fhandle);
    return PyLong_FromLong(0);
}

static PyObject*
flatfile_readf_row_end(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    if (!PyArg_ParseTuple(args, "I", &fhandle)) {
        return NULL;
    }
    readf_row_end(fhandle);
    return PyLong_FromLong(0);
}

static PyObject*
flatfile_writef_row_end(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    if (!PyArg_ParseTuple(args, "I", &fhandle)) {
        return NULL;
    }
    _Bool r = writef_row_end(fhandle);
    return PyBool_FromLong(r);
}

static PyObject*
flatfile_readf_row_is_null(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    unsigned int index = 0;
    if (!PyArg_ParseTuple(args, "II", &fhandle, &index)) {
        return NULL;
    }
    unsigned int u32val = readf_row_is_null(fhandle, index);
    return PyBool_FromLong(u32val);
}

static PyObject*
flatfile_readf_row_get_u32(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    unsigned int index = 0;
    if (!PyArg_ParseTuple(args, "II", &fhandle, &index)) {
        return NULL;
    }
    unsigned int u32val = readf_row_get_u32(fhandle, index);
    return PyLong_FromLong(u32val);
}

static PyObject*
flatfile_readf_row_get_u64(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    unsigned int index = 0;
    if (!PyArg_ParseTuple(args, "II", &fhandle, &index)) {
        return NULL;
    }
    unsigned long u64val = readf_row_get_u64(fhandle, index);
    return PyLong_FromLong(u64val);
}

static PyObject*
flatfile_readf_clone_schema(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    if (!PyArg_ParseTuple(args, "I", &fhandle, &index)) {
        return NULL;
    }
    unsigned long val = readf_clone_schema(fhandle);
    return PyLong_FromLong(val);
}

static PyObject*
flatfile_writef_get_schema(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    if (!PyArg_ParseTuple(args, "I", &fhandle, &index)) {
        return NULL;
    }
    unsigned long val = writef_get_schema(fhandle);
    return PyLong_FromLong(val);
}

static PyObject*
flatfile_readf_row_get_string(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    unsigned int index = 0;
    if (!PyArg_ParseTuple(args, "II", &fhandle, &index)) {
        return NULL;
    }
    char buf[4096] = { 0 } ;
    unsigned long buflen = sizeof(buf);
    unsigned long len = readf_row_get_string(fhandle, index, buf, buflen);
    return PyUnicode_FromStringAndSize(buf, len);
}

static PyObject*
flatfile_writef_row_set_u32(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    unsigned int index = 0;
    unsigned int value = 0;
    if (!PyArg_ParseTuple(args, "III", &fhandle, &index, &value)) {
        return NULL;
    }
    writef_row_set_u32(fhandle, index, value);
    return PyLong_FromLong(0);
}

static PyObject*
flatfile_writef_row_set_u64(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    unsigned int index = 0;
    unsigned long long value = 0;
    if (!PyArg_ParseTuple(args, "III", &fhandle, &index, &value)) {
        return NULL;
    }
    writef_row_set_u64(fhandle, index, value);
    return PyLong_FromLong(0);
}

static PyObject*
flatfile_writef_row_set_string(PyObject* self, PyObject* args) {
    unsigned int fhandle = 0;
    unsigned int index = 0;
    char const* value = NULL;
    if (!PyArg_ParseTuple(args, "IIs", &fhandle, &index, &value)) {
        return NULL;
    }
    writef_row_set_string(fhandle, index, value);
    return PyLong_FromLong(0);
}

static PyMethodDef SpamMethods[] = {
    {"schema2_create", flatfile_schema2_create, METH_VARARGS, "schema2_create doc."},
    {"schema2_destroy", flatfile_schema2_destroy, METH_VARARGS, "schema2_destroy doc."},
    {"schema2_len", flatfile_schema2_len, METH_VARARGS, "schema2_len doc."},
    {"schema2_add_column", flatfile_schema2_add_column, METH_VARARGS, "schema2_add_column doc"},
    {"schema2_get_column_name", flatfile_schema2_get_column_name, METH_VARARGS, "schema2_get_column_name doc."},
    {"schema2_get_column_type", flatfile_schema2_get_column_type, METH_VARARGS, "schema2_get_column_name doc."},
    {"schema2_get_column_nullable", flatfile_schema2_get_column_nullable, METH_VARARGS, "schema2_get_column_name doc."},

    {"writef_open", flatfile_writef_open, METH_VARARGS, "writef_open_doc"},
    {"writef_get_schema", flatfile_writef_get_schema, METH_VARARGS, "writef_get_schema"},
    {"writef_create", flatfile_writef_create, METH_VARARGS, "writef_create doc"},
    {"writef_row_start", flatfile_writef_row_start, METH_VARARGS, "writef_row_start"},
    {"writef_row_end", flatfile_writef_row_end, METH_VARARGS, "writef_row_start"},
    {"writef_close", flatfile_writef_close, METH_VARARGS, "writef_close"},
    {"writef_row_set_u32", flatfile_writef_row_set_u32, METH_VARARGS, "writef_row_set_u32" },
    {"writef_row_set_u64", flatfile_writef_row_set_u64, METH_VARARGS, "writef_row_set_u64" },
    {"writef_row_set_string", flatfile_writef_row_set_string, METH_VARARGS, "writef_row_set_string" },

    {"readf_open", flatfile_readf_open, METH_VARARGS, "readf_open_doc"},
    {"readf_close", flatfile_readf_close, METH_VARARGS, "readf_close_doc"},
    {"readf_row_start", flatfile_readf_row_start, METH_VARARGS, "readf_row_start_doc"},
    {"readf_row_end", flatfile_readf_row_end, METH_VARARGS, "readf_row_end_doc"},

    {"readf_row_is_null", flatfile_readf_row_is_null, METH_VARARGS, "readf_row_is_null_doc"},
    {"readf_row_get_u32", flatfile_readf_row_get_u32, METH_VARARGS, "readf_row_get_u32_doc"},
    {"readf_row_get_u64", flatfile_readf_row_get_u64, METH_VARARGS, "readf_row_get_u64_doc"},
    {"readf_row_get_string", flatfile_readf_row_get_string, METH_VARARGS, "readf_row_get_string_doc"},

    {"readf_clone_schema", flatfile_readf_clone_schema, METH_VARARGS, "readf_clone_schema_doc"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef flatfilemodule = {
    PyModuleDef_HEAD_INIT,
    "_flatfile",   /* name of module */
    NULL, /* module documentation, may be NULL */
    -1,       /* size of per-interpreter state of the module,
                 or -1 if the module keeps state in global variables. */
    SpamMethods
};

PyMODINIT_FUNC
PyInit__flatfile(void)
{
    return PyModule_Create(&flatfilemodule);
}
