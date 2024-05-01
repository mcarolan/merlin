#![allow(dead_code)]

mod cli;
mod mapper;
mod sql_parser;
mod table;

use std::{collections::HashMap, sync::Mutex};

use cli::*;
use lazy_static::lazy_static;
use mapper::ColumnSpecMapper;
use sql_parser::CreateTable;
use table::{ColumnSpec, Table};

use crate::sql_parser::Statement;

lazy_static! {
    static ref TABLES: Mutex<HashMap<String, Table>> = Mutex::new(HashMap::new());
}

fn exec_create_table(fields: &CreateTable) {
    let column_specs: Vec<ColumnSpec> = fields
        .column_specs
        .iter()
        .map(ColumnSpecMapper::sql_parser_to_table)
        .collect();
    let table = Table::new(&column_specs);
    let mut map = TABLES.lock().unwrap();
    map.insert(fields.table_name.clone(), table);
}

fn exec_show_tables() {
    let map = TABLES.lock().unwrap();
    println!();
    for (name, table) in map.iter() {
        print_table(name, table);
    }
    println!();
}

fn main() {
    print_wizard();
    println!("");

    loop {
        let input = read_input();
        let statement = sql_parser::Statement::parse(input.as_str());

        match statement {
            Ok((_, Statement::CreateTable(fields))) => exec_create_table(&fields),
            Ok((_, Statement::Select(_))) => todo!(),
            Ok((_, Statement::ShowTables)) => exec_show_tables(),
            Ok((_, Statement::Insert(_))) => todo!(),
            Err(error_message) => {
                print_invalid_statement_syntax(format!("{}", error_message).as_str())
            }
        }
    }
}
