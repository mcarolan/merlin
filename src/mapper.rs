use crate::{sql_parser, table};

pub struct ColumnSpecMapper {}

impl ColumnSpecMapper {
  pub fn sql_parser_to_table(column_spec: &sql_parser::ColumnSpec) -> table::ColumnSpec {
    table::ColumnSpec {
        column_name: column_spec.name.clone(),
        column_type: ColumnTypeMapper::sql_parser_to_table(&column_spec.column_type),
    }
  }
}

struct ColumnTypeMapper {}

impl ColumnTypeMapper {
  pub fn sql_parser_to_table(column_type: &sql_parser::ColumnType) -> table::ColumnType {
    match column_type {
        sql_parser::ColumnType::Varchar { max_length } => table::ColumnType::Varchar { max_len: *max_length as usize },
        sql_parser::ColumnType::Number => table::ColumnType::Number,
        sql_parser::ColumnType::Boolean => table::ColumnType::Boolean,
    }
  }
}

pub struct InsertValueMapper {}

impl InsertValueMapper {
  pub fn sql_parser_to_table(insert_value: &sql_parser::InsertValue) -> table::Value {
    match insert_value {
        sql_parser::InsertValue::Varchar { value } => table::Value::Varchar { value: value.clone() },
        sql_parser::InsertValue::Number { value } => table::Value::Number { value: *value },
        sql_parser::InsertValue::Boolean { value } => table::Value::Boolean { value: *value },
    }
  }
}