use nom::{branch::alt, bytes::complete::{tag, tag_no_case}, character::complete::{alpha1, alphanumeric0, char, multispace0, multispace1}, combinator::{map, recognize, value}, multi::separated_list1, sequence::{preceded, terminated, tuple}, *};

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Statement {
  CreateTable { table_name: String, column_specs: Vec<ColumnSpec> },
  ShowTables {}
}

impl Statement {
  fn parse_create_table(input: &str) -> IResult<&str, Statement> {
    let (input, _) = parse_keyword("create")(input)?;
    let (input, _) = parse_keyword("table")(input)?;
    let (input, table_name) = parse_id(input)?;
    let (input, _) = recognize(char('('))(input)?;
    let (input, column_specs) = separated_list1(tag(","), ColumnSpec::parse)(input)?;
    let (input, _) = recognize(char(')'))(input)?;

    Ok((input, Statement::CreateTable{
      table_name,
      column_specs
    }))
  }

  fn parse_show_tables(input: &str) -> IResult<&str, Statement> {
    let (input, _) = parse_keyword("show")(input)?;
    value(Statement::ShowTables {}, parse_keyword("tables"))(input)
  }

  pub fn parse(input: &str) -> IResult<&str, Statement> {
    alt((
      Statement::parse_create_table,
      Statement::parse_show_tables
    ))(input)
  }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ColumnSpec {
  name: String,
  column_type: ColumnType
}

impl ColumnSpec {
  fn parse(input: &str) -> IResult<&str, ColumnSpec> {
    map(tuple((parse_id, ColumnType::parse)), |(name, column_type)| {
      ColumnSpec { name, column_type }
    })(input)
  }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum ColumnType {
  String,
  Number,
  Boolean
}

impl ColumnType {
  fn parse(input: &str) -> IResult<&str, ColumnType> {
    alt((
      value(ColumnType::String, parse_keyword("string")),
      value(ColumnType::Number, parse_keyword("number")),
      value(ColumnType::Boolean, parse_keyword("boolean"))
    ))(input)
  }
}

fn parse_keyword<'a>(expected_keyword: &'a str) -> impl Fn(&'a str) -> IResult<&'a str, &'a str> {
  move |input| {
    recognize(preceded(multispace0, terminated(tag_no_case(expected_keyword), multispace0)))(input)
  }
}

fn parse_id(input: &str) -> IResult<&str, String> {
  map(tuple((preceded(multispace0, alpha1), alphanumeric0)), |(start, rest)| {
    format!("{}{}", start, rest)
  })(input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword() {
      let (remaining, matched) = parse_keyword("select")(" select  ").unwrap();
      assert_eq!("", remaining);
      assert_eq!(" select  ", matched);

      let (remaining, matched) = parse_keyword("select")("select  ").unwrap();
      assert_eq!("", remaining);
      assert_eq!("select  ", matched);

      let (remaining, matched) = parse_keyword("select")("SELECT").unwrap();
      assert_eq!("", remaining);
      assert_eq!("SELECT", matched);
    }

    #[test]
    fn test_id() {
      let (remaining, matched) = parse_id("foobar").unwrap();
      assert_eq!("", remaining);
      assert_eq!("foobar", matched);

      let (remaining, matched) = parse_id("foobar1").unwrap();
      assert_eq!("", remaining);
      assert_eq!("foobar1", matched);
      
      assert!(parse_id("1foobar").is_err());

      let (remaining, matched) = parse_id("foobar, ").unwrap();
      assert_eq!(", ", remaining);
      assert_eq!("foobar", matched);

      let (remaining, matched) = parse_id("foobar  ").unwrap();
      assert_eq!("  ", remaining);
      assert_eq!("foobar", matched);
    }

    #[test]
    fn test_create_table() {
      let (remaining, matched) = Statement::parse_create_table("CREATE TABLE person(name string, age number, male boolean)").unwrap();
      assert_eq!("", remaining);
      assert_eq!(Statement::CreateTable {
        table_name: "person".to_string(),
        column_specs: vec![
          ColumnSpec { name: "name".to_string(), column_type: ColumnType::String },
          ColumnSpec { name: "age".to_string(), column_type: ColumnType::Number },
          ColumnSpec { name: "male".to_string(), column_type: ColumnType::Boolean },
        ]
      }, matched);

      let (remaining, matched) = Statement::parse_create_table("   CREATE     TABLE person(  name   string\n,   age  number,    male   boolean)\n").unwrap();
      assert_eq!("\n", remaining);
      assert_eq!(Statement::CreateTable {
        table_name: "person".to_string(),
        column_specs: vec![
          ColumnSpec { name: "name".to_string(), column_type: ColumnType::String },
          ColumnSpec { name: "age".to_string(), column_type: ColumnType::Number },
          ColumnSpec { name: "male".to_string(), column_type: ColumnType::Boolean },
        ]
      }, matched);
    }

  }