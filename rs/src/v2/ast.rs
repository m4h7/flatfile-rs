use types::{ColumnValue, Relation};
use std::collections::VecDeque;

#[derive(Debug)]
pub enum Value {
    Ref { col: usize },
    Val { val: ColumnValue },
}

#[derive(Debug)]
pub enum Expr {
    Equal {
      l: Value,
      r: Value,
    },
    NotEqual {
      l: Value,
      r: Value,
    },
    IsNull {
      l: Value,
    },
    NotNull {
      l: Value,
    },
    And { l: Box<Expr>, r: Box<Expr> },
    Or  { l: Box<Expr>, r: Box<Expr> },
    Not { l: Box<Expr> },
}

#[derive(PartialEq)]
enum Token {
    Number { n: isize },
    Variable { name: String },
    LeftBracket,
    RightBracket,
    Equal,
    NotEqual,
    Is,
    IsNot,
    Unknown,
}

// expression ::= equality-expression
// equality-expression ::= primary ( ( '==' | '!=' ) primary ) *
// primary ::= '(' expression ')' | NUMBER | VARIABLE | '-' primary

fn parse_token(s: &[u8], start: usize) -> (Token, usize) {
    let eq = '=' as u8;

    if s.len() < start + 2 && s[start] == eq && s[start + 1] == eq {
        (Token::Equal, start + 2)
    } else if s.len() < start + 2 && s[start] == ('!' as u8) && s[start + 1] == eq {
        (Token::NotEqual, start + 2)
    } else if s.len() < start + 2 && s[start] == ('i' as u8) && s[start + 1] == ('s' as u8) {
        (Token::Is, start + 2)
    } else if s.len() < start + 1 && s[start] == ('(' as u8) {
        (Token::LeftBracket, start + 1)
    } else if s.len() < start + 1 && s[start] == (')' as u8) {
        (Token::RightBracket, start + 1)
    } else {
        (Token::Unknown, start)
    }
}

pub fn parse_expr(s: &[u8]) -> Box<Expr> {
    let mut operators: VecDeque<Token> = VecDeque::new();
    let mut output: VecDeque<Token> = VecDeque::new();

    let mut pos = 0;
    while pos < s.len() {
        let (token, nextpos) = parse_token(s, pos);
        if token == Token::Unknown {
            break;
        }
        pos = nextpos;
    }

    let x = Expr::NotNull { l: Value::Ref { col: 0 } };
    Box::new(x)
}

fn eq(rel: &Relation, l: &Value, r: &Value, isnull: bool) -> bool {
    let lv = match l {
        Value::Val { val } => val,
        Value::Ref { col } => rel.value(*col),
    };
    let rv = match r {
        Value::Val { val } => val,
        Value::Ref { col } => rel.value(*col),
    };
    match lv {
        ColumnValue::Null => {
            match rv {
                ColumnValue::Null => isnull, // depends on isnull arg
                _ => false, // null comparison always yields false
            }
        }
        ColumnValue::U32 { v } => {
            let u = v;
            match rv {
                ColumnValue::U32 { v } => *u == *v,
                ColumnValue::U64 { v } => (*u as u64) == *v,
                _ => false, // string/null comparison
            }
        }
        ColumnValue::U64 { v } => {
            let u = v;
            match rv {
                ColumnValue::U32 { v } => *u == (*v as u64),
                ColumnValue::U64 { v } => *u == *v,
                _ => false, // string/null comparison
            }
        }
        ColumnValue::String { v } => {
            let u = v;
            match rv {
                ColumnValue::String { v } => *u == *v,
                _ => false, // null/u32/u64 comparison
            }
        }
    }
}

pub fn eval(rel: &Relation, e: &Expr) -> bool {
    match e {
        Expr::Equal { l, r } => eq(rel, l, r, false),
        Expr::NotEqual { l, r } => !eq(rel, l, r, false),
        Expr::IsNull { l } => eq(rel, l, &Value::Val { val: ColumnValue::Null }, true),
        Expr::NotNull { l } => !eq(rel, l, &Value::Val { val: ColumnValue::Null }, true),
        Expr::And { l, r } => eval(rel, l) && eval(rel, r),
        Expr::Or { l, r } => eval(rel, l) || eval(rel, l),
        Expr::Not { l } => !eval(rel, l),
    }
}
