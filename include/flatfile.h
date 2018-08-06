#ifndef FLATFILE_H_INCLUDED
#define FLATFILE_H_INCLUDED

#include <stdbool.h>

/**
 * schema functions
 */
unsigned long schema2_create(void);
int schema2_len(unsigned int schema_handle);
void schema2_destroy(unsigned int schema_handle);
int schema2_add_column(unsigned long schema_handle, char const* name,
                       char const* ctype, _Bool nullable);
int schema2_get_column_name(unsigned int schema_handle, int index, char* buf);

int schema2_get_column_type(unsigned int schema_handle, int index, char* buf);

bool schema2_get_column_nullable(unsigned int schema_handle, int index);

/**
 * write functions
 */

unsigned int writef_create(char const* filename, unsigned long schema_handle);
int writef_open(char const* filename);
void writef_close(unsigned int handle);
int writef_get_schema(int handle);

void writef_row_start(unsigned int handle);
void writef_row_set_u32(unsigned int handle, unsigned int index,
                        unsigned int value);
void writef_row_set_u64(unsigned int handle, unsigned int index,
                        unsigned long value);
void writef_row_set_string(unsigned int handle, unsigned int index,
                           char const* s);
bool writef_row_end(unsigned int handle);
bool writef_flush(unsigned int handle);

unsigned long readf_row_get_string_len(unsigned int fhandle,
                                       unsigned int index);
unsigned long readf_row_get_string(unsigned int fhandle, unsigned int index,
                                   void* out, unsigned long size);
unsigned long readf_row_get_u64(unsigned int fhandle, unsigned int index);
unsigned int readf_row_get_u32(unsigned int fhandle, unsigned int index);
unsigned int readf_row_is_null(unsigned int fhandle, unsigned int index);
unsigned int readf_row_start(unsigned int fhandle);
void readf_close(unsigned int fhandle);
void readf_row_end(unsigned int fhandle);
int readf_open(char const* name);
int readf_open_relation(char const* name, char const* reldef);
unsigned int readf_clone_schema(unsigned int fhandle);

#endif  // FLATFILE_H_INCLUDED
