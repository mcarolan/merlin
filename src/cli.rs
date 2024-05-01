use std::{
    collections::HashMap,
    io::{stdin, stdout, Write},
};

use console::Style;

use crate::table::{self, Table};

pub fn print_wizard() {
    println!("               _");
    println!("              / \\");
    println!("  .||,       /_ _\\");
    println!(" \\.`',/      |'L'|");
    println!(" = ,. =      | -,|");
    println!(" / || \\    ,-'\\\"/,'`.");
    println!("   ||     ,'   `,,. `.");
    println!("   ,|____,' , ,;' \\| |");
    println!("  (3|\\    _/|/'   _| |");
    println!("   ||/,-''  | >-'' _,\\\\");
    println!("   ||'      ==\\ ,-'  ,'");
    println!("   ||       |  V \\ ,|");
    println!("   ||       |    |` |");
    println!("   ||       |    |   \\");
    println!("   ||       |    \\    \\");
    println!("   ||       |     |    \\");
    println!("   ||       |      \\_,-'");
    println!("   ||       |___,,--\")_\\");
    println!("   ||         |_|   ccc/");
    println!("   ||        ccc/");
    println!("   ||                merlin");
}

pub fn read_input() -> String {
    let spell: Style = Style::new().green();
    let arrow: Style = Style::new().cyan().bold();
    print!("{} {} ", spell.apply_to("spell"), arrow.apply_to("🡆"));
    stdout().flush().unwrap();

    let mut res = String::new();

    loop {
        stdin().read_line(&mut res).unwrap();
        res = res.trim_end().to_string();

        if res.ends_with("\\") {
            res.pop();
            res.push_str("\n");
            print!("      {} ", arrow.apply_to("🡆"));
            stdout().flush().unwrap();
        } else {
            break;
        }
    }

    res
}

pub fn print_invalid_statement_syntax(error_message: &str) {
    let error: Style = Style::new().red().bold();
    let message: Style = Style::new().italic();
    println!(
        "{}: {}",
        error.apply_to("Invalid statement syntax"),
        message.apply_to(error_message)
    );
}

pub fn print_table(name: &String, table: &Table) {
    let name_style: Style = Style::new().yellow().bold();
    println!("{}", name_style.apply_to(name));

    let fields: (String, Vec<String>) = (
        "Field".to_string(),
        table
            .column_specs
            .iter()
            .map(|cs| cs.column_name.clone())
            .collect(),
    );
    let types: (String, Vec<String>) = (
        "Type".to_string(),
        table
            .column_specs
            .iter()
            .map(|cs| format!("{}", cs.column_type))
            .collect(),
    );
    draw_string_table(vec![fields, types]);
}

fn draw_string_table(table: Vec<(String, Vec<String>)>) {
    const PADDING_H: usize = 1;

    let column_widths: Vec<usize> = table
        .iter()
        .map(|(k, v)| {
            v.iter()
                .map(|v| v.len())
                .max()
                .unwrap_or(k.len())
                .max(k.len())
        })
        .collect();

    print!("┏");
    for (i, width) in column_widths.iter().enumerate() {
        for _ in 0..*width + (PADDING_H * 2) {
            print!("━");
        }

        if i == column_widths.len() - 1 {
            print!("┓");
        } else {
            print!("┳");
        }
    }

    println!();
    for (i, width) in column_widths.iter().enumerate() {
        print!("┃");
        for _ in 0..PADDING_H {
            print!(" ");
        }
        let header = table
            .get(i)
            .map(|(k, _)| k.clone())
            .unwrap_or_else(|| " ".repeat(*width));
        print!("{}", header);
        for _ in 0..PADDING_H + width - header.len() {
            print!(" ");
        }
    }
    print!("┃");
    println!();
    print!("┣");
    for (i, width) in column_widths.iter().enumerate() {
        for _ in 0..width + (PADDING_H * 2) {
            print!("━");
        }

        if i == column_widths.len() - 1 {
            print!("┫");
        } else {
            print!("╋");
        }
    }

    let rows = table.first().map(|(_, v)| v.len()).unwrap_or_default();

    for i in 0..rows {
        println!();
        for (j, width) in column_widths.iter().enumerate() {
            print!("┃");
            for _ in 0..PADDING_H {
                print!(" ");
            }
            let value = table
                .get(j)
                .and_then(|(_, v)| v.get(i).map(|s| s.clone()))
                .unwrap_or_else(|| " ".repeat(*width));

            print!("{}", value);
            for _ in 0..PADDING_H + width - value.len() {
                print!(" ");
            }
        }
        print!("┃");
    }

    println!();
    print!("┗");
    for (i, width) in column_widths.iter().enumerate() {
        for _ in 0..*width + (PADDING_H * 2) {
            print!("━");
        }

        if i == column_widths.len() - 1 {
            print!("┛");
        } else {
            print!("┻");
        }
    }

    println!();
}

impl std::fmt::Display for table::ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            table::ColumnType::Varchar { max_len } => write!(f, "Varchar({})", max_len)?,
            table::ColumnType::Number => write!(f, "number")?,
            table::ColumnType::Boolean => write!(f, "boolean")?,
        }
        Ok(())
    }
}

impl std::fmt::Display for table::Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}
