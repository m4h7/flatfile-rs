use types::{ColumnType};

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

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn add(&mut self,
               name: &str,
               ctype: ColumnType,
               nullable: bool) {
        self.names.push(name.to_string());
        self.types.push(ctype);
        self.nullable.push(nullable);
    }

    pub fn name(&self, index: usize) -> &str {
        self.names[index].as_str()
    }

    pub fn ctype(&self, index: usize) -> ColumnType {
        self.types[index]
    }

    pub fn nullable(&self, index: usize) -> bool {
        self.nullable[index]
    }
}
