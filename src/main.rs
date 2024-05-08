#![allow(dead_code)]

mod cli;
mod mapper;
mod sql_parser;
mod table;

use std::{collections::HashMap, iter, sync::Mutex};

use cli::*;
use lazy_static::lazy_static;
use mapper::ColumnSpecMapper;
use sql_parser::{CreateTable, CsvImport, Insert, Select};
use table::{ColumnSpec, Table};

use crate::{mapper::InsertValueMapper, sql_parser::Statement, table::Row};

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

fn exec_insert(insert: &Insert) {
    let mut map = TABLES.lock().unwrap();
    let table = map.get_mut(&insert.table_name);

    match table {
        Some(table) => {
            let values: Vec<table::Value> = insert.column_values.iter().map(|v| InsertValueMapper::sql_parser_to_table(&v)).collect();
            let name_values = insert.column_refs.iter().chain(iter::repeat(&String::new())).zip(values).map(|(k, v)| (k.clone(), v)).collect();
            let row_build = Row::new(&name_values, &table.column_specs);

            match row_build {
                Ok(row) => {
                    table.insert(row);
                    print_insert_success(&insert.table_name, table.row_count);
                },
                Err(err) => print_error(format!("Insert failed. {:?}", err).as_str())
            }
        },
        None => {
            print_error(format!("Insert failed. No table named '{}' is defined.", insert.table_name).as_str());
        },
    }
}

fn exec_select(select: &Select) {
    let mut map = TABLES.lock().unwrap();
    let table = map.get_mut(&select.table_name);

    match table {
        Some(table) => {
            let named_columns: Vec<String> = select.column_refs.iter().map(|c| match c {
                sql_parser::SelectColumnReference::Named { column_name } => Some(column_name.clone()),
                sql_parser::SelectColumnReference::Wildcard => None,
            }).flatten().collect();
            let unknown_columns: Vec<&String> = named_columns.iter().filter(|c1| {
                table.column_specs.iter().filter(|c2| c2.column_name == **c1).count() == 0
            }).collect();
            
            todo!()
        },
        None => {
            print_error(format!("Insert failed. No table named '{}' is defined.", select.table_name).as_str());
        }
    }
}

fn exec_csv_import(import: &CsvImport) {
    let mut map = TABLES.lock().unwrap();
    let table = map.get_mut(&import.table_name);

    match table {
        Some(table) => {
            match table.csv_import(&import.file_path, &import.column_mapping) {
                Ok(_) => todo!(),
                Err(err) => print_error(format!("CSV import failed. {:?}", err).as_str()),
            }
        },
        None => {
            print_error(format!("Insert failed. No table named '{}' is defined.", import.table_name).as_str());
        }
    }
}

fn main() {
    print_wizard();
    println!("");

    loop {
        let input = read_input();
        let statement = sql_parser::Statement::parse(input.as_str());

        match statement {
            Ok((_, Statement::CreateTable(fields))) => exec_create_table(&fields),
            Ok((_, Statement::Select(fields))) => exec_select(&fields),
            Ok((_, Statement::ShowTables)) => exec_show_tables(),
            Ok((_, Statement::Insert(insert))) => exec_insert(&insert),
            Ok((_, Statement::CsvImport(fields))) => exec_csv_import(&fields),
            Err(error_message) => {
                print_invalid_statement_syntax(format!("{}", error_message).as_str())
            }
        }
    }
}
