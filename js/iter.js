const { Reader } = require('./run');

function column_get(filename, column_name, cb_err, cb_next, cb_complete) {
    const r = new Reader(filename);
    const s = r.schema();

    let colidx = null;

    for (let i = 0 ; i < s.length ; ++i) {
        const [ name ] = s[i];
        if (name === column_name) {
            colidx = i;
        }
    }

    let f_next = function(value) {
        let result = cb_next(value[colidx]);
        if (result !== false) {
          f();
        }
    };

    function f() {
        r.read(cb_err, f_next, cb_complete);
    }

    if (colidx === null) {
        r.close();
        console.log('iter_over_column_unique: column not found!', column_name, s);
        setTimeout(cb_complete, 0);
    } else {
        setTimeout(f, 0);
    }
}

function column_distinct(filename, column_name, cb, ctx) {
    const seen = new Set();
    column_distinct(filename, column_name, function(err, value) {
        if (!err) {
            if (!seen.has(value)) {
                cb(null, value);
                seen.add(value);
            }
        } else {
            cb(err, null);
        }
    });
}

function column_difference(f1, c1, f2, c2, err, next, complete) {
    const v2 = new Set();
    column_get(f2, c2, e => err(e), v => v2.add(v), () => {
        column_get(f1, c1,
            e => err(e),
            v => { if (!v2.has(v)) { next(v) } },
            () => { complete(); }
        );
    });
}

exports.column_get = column_get;
exports.column_difference = column_difference;
