mod cli;
mod sql_parser;

use cli::*;

fn main() {
    print_wizard();
    println!("");

    let line = read_input();

    println!("you said {}", line);
}
