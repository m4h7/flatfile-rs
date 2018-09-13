use types::{ColumnType};

pub trait Schema {
    fn len(&self) -> usize;
    fn name(&self, index: usize) -> &str;
    fn ctype(&self, index: usize) -> ColumnType;
    fn nullable(&self, index: usize) -> bool;
}

#[derive(Clone)]
pub struct Schema2 {
    pub names: Vec<String>,
    pub types: Vec<ColumnType>,
    pub nullable: Vec<bool>,
}

impl Schema2 {
    pub fn new() -> Self {
        Schema2 {
            names: Vec::new(),
            types: Vec::new(),
            nullable: Vec::new(),
        }
    }

    pub fn add(&mut self,
               name: &str,
               ctype: ColumnType,
               nullable: bool) {
        self.names.push(name.to_string());
        self.types.push(ctype);
        self.nullable.push(nullable);
    }

    pub fn set_nullable(&mut self, index: usize, nullability: bool) {
        self.nullable[index] = nullability;
    }
}

impl Schema for Schema2 {
    fn len(&self) -> usize {
        self.names.len()
    }

    fn name(&self, index: usize) -> &str {
        self.names[index].as_str()
    }

    fn ctype(&self, index: usize) -> ColumnType {
        self.types[index]
    }

    fn nullable(&self, index: usize) -> bool {
        self.nullable[index]
    }
}
