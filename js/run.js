const addon = require('./build/Release/module');
const fs = require('fs');

function extract_schema(schema_handle) {
  const schema = [];
  for (let i = 0; i < addon.schema2_len(schema_handle); ++i) {
      const r = [
          addon.schema2_get_column_name(schema_handle, i),
          addon.schema2_get_column_type(schema_handle, i),
          Boolean(addon.schema2_get_column_nullable(schema_handle, i))
      ];
      schema.push(r);
  }
  return schema;
}

class FlatfileWriter {
  constructor(filename, schema) {
    if (fs.existsSync(filename)) {
      this.handle = addon.writef_open(filename);
      const schema_handle = addon.writef_get_schema(this.handle);
      this.schema = extract_schema(schema_handle);
    } else {
      this.schema = schema;
      const schema_handle = addon.schema2_create();
      for (let i = 0; i < schema.length; ++i) {
        const [ name, type, nullable = false ] = schema[i];
        addon.schema2_add_column(schema_handle, name, type, nullable);
      }
      this.handle = addon.writef_create(filename, schema_handle);
    }
  }

  write(obj) {
    let values = [];
    for (let i = 0; i < this.schema.length ; ++i) {
        const [ key, type_ ] = this.schema[i];
        if (obj[key] !== undefined) {
          // simple type check
          if (type_ == 'string' && typeof obj[key] !== 'string') {
              console.log(`${ key } is not a string`);
          } else if ((type_ == 'u32' || type_ == '64') && typeof obj[key] !== 'number') {
              console.log(`${ key } is not a number`);
          }
          values.push(obj[key]);
        } else {
          values.push(null); // substitute null for undefined
        }
    }
    return this.pwrite(values);
  }

  pwrite(values) {
    let handle = this.handle;
    return new Promise(function(resolve, reject) {
      addon.writef_row(handle, values, function(err, value) {
        if (err) {
          console.log('written', values);
          console.log('writef_row err', err);
          reject(err);
        } else {
          resolve(value);
        }
      }, null);
    });
  }

  close() {
    addon.writef_close(this.handle);
    this.handle = null;
  }
}

class Reader {
    constructor(filename) {
        this.rhandle = addon.readf_open(filename);
        if (this.rhandle >= 0) {
            const schema_handle = addon.readf_get_schema(this.rhandle);
            this._schema = extract_schema(schema_handle);
        } else {
            // file not opened
            this._schema = [];
        }
    }

    schema() {
        return this._schema;
    }

    oread(observer) {
        function err(e) { observer.error(e); }
        function next(v) { observer.next(v); }
        function complete() { observer.complete(); }
        addon.readf_row(this.rhandle, err, next, complete);
    }

    read(cb_err, cb_next, cb_done) {
        if (this.rhandle >= 0) {
            addon.readf_row(this.rhandle, cb_err, cb_next, cb_done);
        } else {
            // empty file
            setTimeout(cb_done, 0);
        }
    }

    close() {
        if (this.rhandle >= 0) {
            addon.readf_close(this.rhandle);
        }
        delete this.rhandle;
    }
}

exports.FlatfileWriter = FlatfileWriter;
exports.Reader = Reader;
