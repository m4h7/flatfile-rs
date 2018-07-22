#include <node_api.h>

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "flatfile.h"

napi_status napi_create_error2(napi_env env,
                               char const* str,
                               napi_value* result)
{
    fprintf(stderr, "napi_create_error2 %s\n", str);
    napi_value code, msg;
    napi_status status;

    status = napi_get_null(env, &code);
    if (status != napi_ok) {
        return status;
    }

    status = napi_create_string_utf8(env, str, strlen(str), &msg);
    if (status != napi_ok) {
        return status;
    }

     status = napi_create_error(env, NULL, msg, result);
     return status;
}

typedef struct {
    int w_handle;   // write_handle
    napi_ref array;    // array with values to write
    napi_ref callback; // callback
    napi_ref ctx;      // this

    napi_async_work work;
    napi_value async_resource_name;

    napi_value error;
} write_row_data_t;


typedef struct {
    int r_handle;
    int result; // result of schema_read_row

    napi_ref error_callback;
    napi_ref next_callback;
    napi_ref complete_callback;

    napi_async_work work;
    napi_value async_resource_name;
    napi_value error;
} read_row_data_t;


void writef_row_execute_callback(napi_env env, void *data)
{
    write_row_data_t* self = data;
    if (self->error) {
        fprintf(stderr, "Exec callback skip due to error\n");
        return;
    }
    writef_row_end(self->w_handle);
    writef_flush(self->w_handle);
}


void writef_row_complete_callback(napi_env env, napi_status status, void* data)
{
    write_row_data_t* self = data;

    napi_handle_scope scope;
    status = napi_open_handle_scope(env, &scope);
    assert(status == napi_ok);

    napi_value ctx;
    if (self->ctx) {
        status = napi_get_reference_value(env, self->ctx, &ctx);
        assert(status == napi_ok);
    } else {
        napi_get_null(env, &ctx);
    }

    napi_value callback;
    status = napi_get_reference_value(env, self->callback, &callback);
    assert(status == napi_ok);

    napi_value argv[2];
    napi_value error;

    if (self->error == NULL) {
        status = napi_get_null(env, &error);
    } else {
        error = self->error;
    }

    argv[0] = error;

    status = napi_create_uint32(env, 123, &(argv[1]));
    assert(status == napi_ok);

    napi_call_function(env, ctx, callback, 2, argv, NULL);

    status = napi_close_handle_scope(env, scope);
    assert(status == napi_ok);

    // delete callback reference
    status = napi_delete_reference(env, self->callback);
    assert(status == napi_ok);
    self->callback = NULL;

    // delete array reference
    status = napi_delete_reference(env, self->array);
    assert(status == napi_ok);
    self->array = NULL;

    // delete ctx reference if set
    if (self->ctx) {
        status = napi_delete_reference(env, self->ctx);
        assert(status == napi_ok);
    }

    free(self);

    // TODO: does not work in node 10.4.0
//    napi_delete_async_work(env, self->work);
}


napi_value f_writef_row(napi_env env, napi_callback_info info) {
    size_t argc = 4;

    napi_value argv[4];
    napi_status status;

    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse argument");
        return NULL;
    }

    write_row_data_t* self = calloc(sizeof(write_row_data_t), 1);
    self->error = NULL;

    status = napi_get_value_int32(env, argv[0], &self->w_handle);
    if (status != napi_ok) {
        napi_create_error2(env, "Invalid number was passed as argument (handle)", &self->error);
    }

//        napi_extended_error_info* extended;
//        napi_get_last_error_info(env, &extended);
//        fprintf(stderr, "status 0 %d %s", status, extended->error_message);

    status = napi_create_reference(env, argv[1], 1, &(self->array));
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse argument 1");
        return NULL;
    }

    status = napi_create_reference(env, argv[2], 1, &(self->callback));
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse argument 2");
        return NULL;
    }

    napi_valuetype argv3type;
    status = napi_typeof(env, argv[3], &argv3type);
    if (status == napi_ok && (argv3type == napi_null || argv3type == napi_undefined)) {
        self->ctx = NULL;
    } else {
        status = napi_create_reference(env, argv[3], 1, &(self->ctx));

        if (status != napi_ok) {
            napi_throw_error(env, NULL, "Failed to parse argument 3");
            return NULL;
        }
    }

    status = napi_create_string_utf8(env, "writef_row", -1, &(self->async_resource_name));
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "failed to create resource name");
        return NULL;
    }

    napi_value array;
    status = napi_get_reference_value(env, self->array, &array);
    if (status != napi_ok) {
        napi_create_error2(env, "get array error", &self->error);
        return NULL;
    }

    _Bool is_array = false;
    status = napi_is_array(env, array, &is_array);
    if (status != napi_ok) {
        napi_create_error2(env, "napi_is_array failed", &self->error);
        return NULL;
    }

    if (!is_array) {
        napi_create_error2(env, "second arg is not an array", &self->error);
        return NULL;
    }

    unsigned int arraylen = 0;
    status = napi_get_array_length(env, array, &arraylen);
    if (status != napi_ok) {
        napi_create_error2(env, "cannot get array length", &self->error);
        return NULL;
    }

    writef_row_start(self->w_handle);

    for (size_t i = 0; i < arraylen; ++i) {
        napi_handle_scope scope;
        napi_status status = napi_open_handle_scope(env, &scope);

        if (status != napi_ok) {
            // napi_open_handle_scope failed
            break;
        }

        napi_value value;
        status = napi_get_element(env, array, i, &value);
        if (status != napi_ok) {
            napi_close_handle_scope(env, scope);
            break;
        }

        napi_valuetype valuetype;
        status = napi_typeof(env, value, &valuetype);
        if (status != napi_ok) {
            napi_close_handle_scope(env, scope);
            break;
        }

        if (valuetype == napi_undefined || valuetype == napi_null) {
            // do nothing
        } else if (valuetype == napi_number) {

            unsigned int sch = writef_get_schema(self->w_handle);

            char column_type[1024] = { 0 };
            char column_name[128] = { 0 };

            schema2_get_column_name(sch, i, column_name);
            schema2_get_column_type(sch, i, column_type);

            if (!strcmp(column_type, "u32")) {
                int i32val = -1;
                status = napi_get_value_int32(env, value, &i32val);
                if (status != napi_ok) {
                    napi_create_error2(env, "Invalid number was passed as argument (u32)", &self->error);
                } else {
                    writef_row_set_u32(self->w_handle, i, i32val);
                }
            } else if (!strcmp(column_type, "u64")) {
                int i64val = -1;
                status = napi_get_value_int32(env, value, &i64val);
                if (status != napi_ok) {
                    napi_create_error2(env, "Invalid number was passed as argument (u64)", &self->error);
                } else {
                    writef_row_set_u64(self->w_handle, i, i64val);
                }
            } else {
                fprintf(stderr, "schema type is %s expecting u32/64 for number (name %s)\n", column_type, column_name);
                napi_create_error2(
                    env,
                    "Schema type is not u32/u64 but expecting number",
                    &self->error);
            }

        } else if (valuetype == napi_string) {
            char buf[4096] = { 0 };
            size_t buflen = 0;
            status = napi_get_value_string_utf8(env, value, buf, sizeof(buf), &buflen);
            if (status != napi_ok) {
                napi_create_error2(env, "not a string", &self->error);
            } else {
                writef_row_set_string(self->w_handle, i, buf);
            }
        } else {
            napi_create_error2(env, "unknown type", &self->error);
        }

        status = napi_close_handle_scope(env, scope);
        if (status != napi_ok) {
            napi_create_error2(env, "napi_close_handle_scope error", &self->error);
            break;
        }
    }

    status = napi_create_async_work(env,
                           NULL,
                           self->async_resource_name,
                           writef_row_execute_callback,
                           writef_row_complete_callback,
                           self,
                           &(self->work));

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "napi_create_async_work failed");
        return NULL;
    }

    status = napi_queue_async_work(env, self->work);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "napi_queue_async_work failed");
        return NULL;
    }

    napi_value undef;
    status = napi_get_undefined(env, &undef);
    if (status != napi_ok) {
        return NULL;
    }
    return undef;
}


napi_value f_schema2_create(napi_env env, napi_callback_info info) {
    unsigned int rc = schema2_create();

    napi_value retval;
    napi_create_int32(env, rc, &retval);
    return retval;
}


napi_value f_schema2_destroy(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int sch_handle = -1;

    status = napi_get_value_int32(env, argv[0], &sch_handle);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    } else {
        schema2_destroy(sch_handle);
    }

    napi_value undef;
    status = napi_get_undefined(env, &undef);
    if (status != napi_ok) {
        return NULL;
    }
    return undef;
}


napi_value f_schema2_len(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int number = -1;

    status = napi_get_value_int32(env, argv[0], &number);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    }

    unsigned long len = schema2_len(number);

    napi_value retval;
    napi_create_int32(env, len, &retval);
    return retval;
}


napi_value f_schema2_get_column_name(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    napi_status status;

    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int number = -1;

    status = napi_get_value_int32(env, argv[0], &number);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
        return NULL;
    }

    int column_index = -1;
    status = napi_get_value_int32(env, argv[1], &column_index);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument #2");
        return NULL;
    }

    char name[4096] = { 0 };

    schema2_get_column_name(number, column_index, name);

    napi_value retval;
    napi_create_string_utf8(env, name, strlen(name), &retval);
    return retval;
}


napi_value f_schema2_get_column_type(napi_env env, napi_callback_info info) {
    size_t argc = 2;

    napi_value argv[2];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int number = -1;
    status = napi_get_value_int32(env, argv[0], &number);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    }

    int column_index = -1;
    status = napi_get_value_int32(env, argv[1], &column_index);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument #2");
    }

    char typename[1024] = { 0 };
    schema2_get_column_type(number, column_index, typename);

    napi_value retval;
    napi_create_string_utf8(env, typename, strlen(typename), &retval);
    return retval;
}


napi_value f_schema2_get_column_nullable(napi_env env, napi_callback_info info) {
    size_t argc = 2;

    napi_value argv[2];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int number = -1;
    status = napi_get_value_int32(env, argv[0], &number);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
        return NULL;
    }

    int column_index = -1;
    status = napi_get_value_int32(env, argv[1], &column_index);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument #2");
        return NULL;
    }

    unsigned int is_nullable = schema2_get_column_nullable(number, column_index);

    napi_value retval;
    napi_create_int32(env, is_nullable, &retval);
    return retval;
}


napi_value f_schema2_add_column(napi_env env, napi_callback_info info) {
    size_t argc = 4;

    napi_value argv[4];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int number = -1;
    status = napi_get_value_int32(env, argv[0], &number);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    }

    char name[4096] = { 0 };
    size_t namelen = 0;
    status = napi_get_value_string_utf8(env, argv[1], name, sizeof(name), &namelen);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument #2");
    }

    char typename[4096] = { 0 };
    size_t typenamelen = 0;
    status = napi_get_value_string_utf8(env, argv[2], typename, sizeof(typename), &typenamelen);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument #3");
    }

    _Bool is_nullable = false;
    status = napi_get_value_bool(env, argv[3], &is_nullable);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid type was passed as argument #4");
    }

    schema2_add_column(number, name, typename, is_nullable);

    napi_value retval;
    napi_create_int32(env, 0, &retval);
    return retval;
}


napi_value f_writef_create(napi_env env, napi_callback_info info) {
    size_t argc = 2;

    napi_value argv[2];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    char name[4096] = { 0 };
    size_t namelen = 0;
    status = napi_get_value_string_utf8(env, argv[0], name, sizeof(name), &namelen);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument #2");
    }

    int sch_handle = -1;
    status = napi_get_value_int32(env, argv[1], &sch_handle);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    }

    unsigned int whandle = writef_create(name, sch_handle);
    writef_flush(whandle);

    napi_value retval;
    napi_create_int32(env, whandle, &retval);
    return retval;

}


napi_value f_writef_open(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    char name[4096] = { 0 };
    size_t namelen = 0;
    status = napi_get_value_string_utf8(env, argv[0], name, sizeof(name), &namelen);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument #2");
    }

    unsigned int whandle = writef_open(name);

    napi_value retval;
    napi_create_int32(env, whandle, &retval);
    return retval;

}


napi_value f_writef_close(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int w_handle = -1;
    status = napi_get_value_int32(env, argv[0], &w_handle);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    }

    writef_close(w_handle);

    napi_value undef;
    status = napi_get_undefined(env, &undef);

    if (status != napi_ok) {
        return NULL;
    }

    return undef;
}


napi_value f_writef_flush(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int w_handle = -1;
    status = napi_get_value_int32(env, argv[0], &w_handle);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    }

    writef_flush(w_handle);

    napi_value undef;
    status = napi_get_undefined(env, &undef);

    if (status != napi_ok) {
        return NULL;
    }

    return undef;
}


napi_value f_writef_get_schema(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int w_handle = -1;
    status = napi_get_value_int32(env, argv[0], &w_handle);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    }

    int s_handle = writef_get_schema(w_handle);

    napi_value retval;
    napi_create_int32(env, s_handle, &retval);
    return retval;
}


napi_value f_readf_get_schema(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;

    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
        return NULL;
    }

    int w_handle = -1;

    status = napi_get_value_int32(env, argv[0], &w_handle);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
        return NULL;
    }

    unsigned int s_handle = readf_clone_schema(w_handle);

    napi_value retval;
    napi_create_int32(env, s_handle, &retval);
    return retval;
}


napi_value f_readf_open(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    char name[4096] = { 0 };
    size_t namelen = 0;
    status = napi_get_value_string_utf8(env, argv[0], name, sizeof(name), &namelen);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument #2");
    }

    unsigned int rhandle = readf_open(name);

    napi_value retval;
    napi_create_int32(env, rhandle, &retval);
    return retval;

}


napi_value f_readf_close(napi_env env, napi_callback_info info) {
    size_t argc = 1;

    napi_value argv[1];
    napi_status status;
    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse arguments");
    }

    int w_handle = -1;
    status = napi_get_value_int32(env, argv[0], &w_handle);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Invalid number was passed as argument");
    }

    readf_close(w_handle);

    napi_value undef;
    status = napi_get_undefined(env, &undef);

    if (status != napi_ok) {
        return NULL;
    }

    return undef;
}


void readf_row_execute_callback(napi_env env, void *data)
{
    read_row_data_t* self = data;
    if (self->error) {
        fprintf(stderr, "Exec callback skip due to error\n");
        return;
    }
    self->result = readf_row_start(self->r_handle);
}


void readf_row_complete_callback(napi_env env, napi_status status, void* data)
{
    read_row_data_t* self = data;

    // TODO: does not work in node 10.4.0
//    napi_delete_async_work(env, self->work);

    napi_handle_scope scope;
    napi_open_handle_scope(env, &scope);

    napi_value value;

    napi_value callback;
    if (self->error) {
        napi_get_reference_value(env, self->error_callback, &callback);
        value = self->error;
    } else if (self->result == 0) {
        status = napi_get_reference_value(env, self->complete_callback, &callback);
        if (status != napi_ok) {
            fprintf(stderr, "XX\n");
        }
        status = napi_get_null(env, &value);
    } else {
        napi_get_reference_value(env, self->next_callback, &callback);

        int sch = readf_clone_schema(self->r_handle);
        int sch_length = schema2_len(sch);

        status = napi_create_array_with_length(env, sch_length, &value);

        for (int i = 0; i < sch_length; ++i) {
            char typebuf[64] = { 0 };
            schema2_get_column_type(sch, i, typebuf);
            bool nullable = schema2_get_column_nullable(sch, i);

            napi_value element = NULL;
            if (!strcmp(typebuf, "u32")) {
                if (nullable && readf_row_is_null(self->r_handle, i)) {
                    napi_get_null(env, &element);
                } else {
                    int v = readf_row_get_u32(self->r_handle, i);
                    napi_create_uint32(env, v, &element);
                }
            } else if (!strcmp(typebuf, "u64")) {
                if (nullable && readf_row_is_null(self->r_handle, i)) {
                    napi_get_null(env, &element);
                } else {
                    int v = readf_row_get_u64(self->r_handle, i);
                    napi_create_int64(env, v, &element);
                }
            } else if (!strcmp(typebuf, "string")) {
                if (nullable && readf_row_is_null(self->r_handle, i)) {
                    napi_get_null(env, &element);
                } else {
                    unsigned long stringlen = readf_row_get_string_len(self->r_handle, i);
                    if (stringlen < 1024) {
                        char buf[1024] = { 0 } ;
                        unsigned long buflen = sizeof(buf);
                        unsigned long len = readf_row_get_string(self->r_handle, i, buf, buflen);
                        assert(len < sizeof(buf));
                        status = napi_create_string_utf8(env, buf, strlen(buf), &element);
                        assert(status == napi_ok);
                    } else {
                        char* buf = malloc(stringlen + 1);
                        memset(buf, 0, stringlen);
                        unsigned long buflen = stringlen;
                        unsigned long len = readf_row_get_string(self->r_handle, i, buf, buflen);
                        assert(len == buflen);
                        status = napi_create_string_utf8(env, buf, strlen(buf), &element);
                        assert(status == napi_ok);
                    }
                }
            }
            status = napi_set_element(env, value, i, element);
            assert(status == napi_ok);
        }
    }

    napi_value args[1];
    args[0] = value;

    napi_value ctx;
    status = napi_get_null(env, &ctx);

    status = napi_call_function(env, ctx, callback, 1, args, NULL);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to call callback");
    }
    napi_close_handle_scope(env, scope);

    status = napi_delete_reference(env, self->error_callback);
    assert(status == napi_ok);
    status = napi_delete_reference(env, self->next_callback);
    assert(status == napi_ok);
    status = napi_delete_reference(env, self->complete_callback);
    assert(status == napi_ok);

    free(self);
}


napi_value f_readf_row(napi_env env, napi_callback_info info) {
    size_t argc = 4;

    napi_value argv[4];
    napi_status status;

    status = napi_get_cb_info(env, info, &argc, argv, NULL, NULL);
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse argument");
        return NULL;
    }

    read_row_data_t* self = calloc(sizeof(read_row_data_t), 1);
    self->error = NULL;

    status = napi_get_value_int32(env, argv[0], &self->r_handle);
    if (status != napi_ok) {
        napi_create_error2(env, "Invalid number was passed as argument (handle)", &self->error);
        free(self);
        self = NULL;
        return NULL;
    }

    status = napi_create_reference(env, argv[1], 1, &(self->error_callback));
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse argument 1");
        free(self);
        self = NULL;
        return NULL;
    }

    status = napi_create_reference(env, argv[2], 1, &(self->next_callback));
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse argument 2");
        return NULL;
    }

    status = napi_create_reference(env, argv[3], 1, &(self->complete_callback));
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "Failed to parse argument 3");
        return NULL;
    }


    status = napi_create_string_utf8(env, "readf_row", -1, &(self->async_resource_name));
    if (status != napi_ok) {
        napi_throw_error(env, NULL, "failed to create resource name");
        return NULL;
    }

    status = napi_create_async_work(env,
                           NULL,
                           self->async_resource_name,
                           readf_row_execute_callback,
                           readf_row_complete_callback,
                           self,
                           &(self->work));

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "napi_create_async_work failed");
        return NULL;
    }

    status = napi_queue_async_work(env, self->work);

    if (status != napi_ok) {
        napi_throw_error(env, NULL, "napi_queue_async_work failed");
        return NULL;
    }

    napi_value undef;
    status = napi_get_undefined(env, &undef);
    if (status != napi_ok) {
        return NULL;
    }
    return undef;
}

struct {
    char const* name;
    napi_callback cb;
} functions[] = {
    { "schema2_create", f_schema2_create },
    { "schema2_destroy", f_schema2_destroy },
    { "schema2_len", f_schema2_len },
    { "schema2_add_column", f_schema2_add_column },
    { "schema2_get_column_name", f_schema2_get_column_name },
    { "schema2_get_column_type", f_schema2_get_column_type },
    { "schema2_get_column_nullable", f_schema2_get_column_nullable },

    { "readf_open", f_readf_open },
    { "readf_close", f_readf_close },
    { "readf_get_schema", f_readf_get_schema },
    { "readf_row", f_readf_row },

    { "writef_create", f_writef_create },
    { "writef_open", f_writef_open },
    { "writef_row", f_writef_row },
    { "writef_close", f_writef_close },
    { "writef_flush", f_writef_flush },
    { "writef_get_schema", f_writef_get_schema },
};


napi_value Init(napi_env env, napi_value exports) {

    for (size_t i = 0; i < sizeof(functions) / sizeof(*functions) ; ++i) {
        napi_status status;
        napi_value fn;

        // Arguments 2 and 3 are function name and length respectively
        // We will leave them as empty for this example
        status = napi_create_function(env, NULL, 0, functions[i].cb, NULL, &fn);
        if (status != napi_ok) {
            napi_throw_error(env, NULL, "Unable to wrap native function");
        }

        status = napi_set_named_property(env, exports, functions[i].name, fn);
        if (status != napi_ok) {
            napi_throw_error(env, NULL, "Unable to populate exports");
        }
    }

    return exports;
}


NAPI_MODULE(NODE_GYP_MODULE_NAME, Init)
