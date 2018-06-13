const addon = require('./build/Release/module');
const fs = require('fs');

class FlatfileWriter {
  constructor(filename, schema) {
    if (fs.existsSync(filename)) {
      this.handle = addon.writef_open(filename);
    } else {
      const schema_handle = addon.schema2_create();
      for (let i = 0; i < schema.length; ++i) {
        const [ name, type, nullable = false ] = schema[i];
        addon.schema2_add_column(schema_handle, name, type, nullable);
      }
      this.handle = addon.writef_create(filename, schema_handle);
    }
  }

  pwrite(values) {
    let handle = this.handle;
    return new Promise(function(resolve, reject) {
      addon.writef_row(handle, values, function(err, value) {
        if (err) {
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

exports.FlatfileWriter = FlatfileWriter;
