use types::{Column, ColumnValue};

#[derive(Debug)]
pub struct Row<'a> {
    columns: &'a [Column],
    values: Vec<ColumnValue>,
    nullval: ColumnValue,
}

impl<'a> Row<'a> {
    pub fn new(cols: &'a [Column]) -> Row {
        Row {
            values: vec![ColumnValue::Null; cols.len()],
            columns: cols,
            nullval: ColumnValue::Null,
        }
    }

    pub fn push(&mut self, colidx: usize, v: ColumnValue) {
        assert!(self.values[colidx] == ColumnValue::Null);
        self.values[colidx] = v;
    }

    pub fn geti(&self, colidx: usize) -> &ColumnValue {
        if colidx < self.values.len() {
            &self.values[colidx]
        } else {
            &self.nullval
        }
    }

    pub fn getn(&self, colname: &str) -> &ColumnValue {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name == colname {
                return &self.values[i];
            }
        }
        &self.nullval
    }
}
