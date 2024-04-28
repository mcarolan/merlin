#![allow(dead_code)]

mod cli;
mod sql_parser;
mod table;

use cli::*;

fn main() {
    print_wizard();
    println!("");

    let line = read_input();

    println!("you said {}", line);
}
