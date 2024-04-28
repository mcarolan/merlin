use std::io::{stdin, stdout, Write};

use console::Style;

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
    }
    else {
      break
    }
  }

  res
}