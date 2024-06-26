use std::collections::HashMap;

use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until},
    character::complete::{self, *},
    combinator::*,
    multi::{many0, many1, separated_list1},
    sequence::{preceded, terminated, tuple},
    *,
};

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct CreateTable {
    pub table_name: String,
    pub column_specs: Vec<ColumnSpec>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Select {
    pub column_refs: Vec<SelectColumnReference>,
    pub table_name: String,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Insert {
    pub column_refs: Vec<String>,
    pub column_values: Vec<InsertValue>,
    pub table_name: String,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct CsvImport {
    pub column_mapping: HashMap<String, String>,
    pub file_path: String,
    pub table_name: String,
    pub with_truncate: bool
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum Statement {
    CreateTable(CreateTable),
    ShowTables,
    Select(Select),
    Insert(Insert),
    CsvImport(CsvImport),
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum InsertValue {
    Varchar { value: String },
    Number { value: u64 },
    Boolean { value: bool },
}

fn parse_string(input: &str) -> IResult<&str, String> {
    let (input, _) = preceded(multispace0, tag("\""))(input)?;
    let (input, value) = take_until("\"")(input)?;
    let (input, _) = terminated(tag("\""), multispace0)(input)?;
    Ok((input, value.to_string()))
}

impl InsertValue {
    //TODO: allow escapes
    fn parse_varchar(input: &str) -> IResult<&str, InsertValue> {
        let (input, value) = parse_string(input)?;
        Ok((input, InsertValue::Varchar { value: value }))
    }

    fn parse_number(input: &str) -> IResult<&str, InsertValue> {
        map(
            terminated(preceded(multispace0, complete::u64), multispace0),
            |value| InsertValue::Number { value },
        )(input)
    }

    fn parse_boolean(input: &str) -> IResult<&str, InsertValue> {
        alt((
            value(InsertValue::Boolean { value: true }, parse_keyword("true")),
            value(
                InsertValue::Boolean { value: false },
                parse_keyword("false"),
            ),
        ))(input)
    }

    fn parse(input: &str) -> IResult<&str, InsertValue> {
        alt((
            InsertValue::parse_varchar,
            InsertValue::parse_number,
            InsertValue::parse_boolean,
        ))(input)
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum SelectColumnReference {
    Named { column_name: String },
    Wildcard,
}

impl SelectColumnReference {
    fn parse(input: &str) -> IResult<&str, SelectColumnReference> {
        alt((
            value(SelectColumnReference::Wildcard, parse_keyword("*")),
            map(parse_id, |column_name| SelectColumnReference::Named {
                column_name,
            }),
        ))(input)
    }
}

impl Statement {
    fn parse_create_table(input: &str) -> IResult<&str, Statement> {
        let (input, _) = parse_keyword("create")(input)?;
        let (input, _) = parse_keyword("table")(input)?;
        let (input, table_name) = parse_id(input)?;
        let (input, _) = recognize(char('('))(input)?;
        let (input, column_specs) = separated_list1(tag(","), ColumnSpec::parse)(input)?;
        let (input, _) = recognize(char(')'))(input)?;

        Ok((
            input,
            Statement::CreateTable(CreateTable {
                table_name,
                column_specs,
            }),
        ))
    }

    fn parse_select(input: &str) -> IResult<&str, Statement> {
        let (input, _) = parse_keyword("select")(input)?;
        let (input, column_refs) = separated_list1(tag(","), SelectColumnReference::parse)(input)?;
        let (input, _) = parse_keyword("from")(input)?;
        let (input, table_name) = parse_id(input)?;
        Ok((
            input,
            Statement::Select(Select {
                column_refs,
                table_name,
            }),
        ))
    }

    fn parse_csv_column_mapping(input: &str) -> IResult<&str, (String, String)> {
        let (input, id1) = parse_id(input)?;
        let (input, _) = tag("=")(input)?;
        let (input, id2) = parse_id(input)?;
        Ok((input, (id1, id2)))
    }

    fn parse_csv_import(input: &str) -> IResult<&str, Statement> {
        let (input, _) = parse_keyword("import")(input)?;
        let (input, _) = parse_keyword("csv")(input)?;
        let (input, _) = parse_keyword("from")(input)?;
        let (input, file_path) = parse_string(input)?;
        let (input, _) = parse_keyword("into")(input)?;
        let (input, table_name) = parse_id(input)?;
        let (input, _) = parse_keyword("with")(input)?;
        let (input, _) = parse_keyword("(")(input)?;
        let (input, column_mapping) =
            separated_list1(tag(","), Statement::parse_csv_column_mapping)(input)?;
        let (input, _) = parse_keyword(")")(input)?;
        let (input, with_truncate) = opt(parse_keyword("truncate"))(input)?;

        Ok((
            input,
            Statement::CsvImport(CsvImport {
                column_mapping: column_mapping.into_iter().collect(),
                file_path,
                table_name,
                with_truncate: with_truncate.is_some()
            }),
        ))
    }

    fn parse_insert(input: &str) -> IResult<&str, Statement> {
        let (input, _) = parse_keyword("insert")(input)?;
        let (input, _) = parse_keyword("into")(input)?;
        let (input, table_name) = parse_id(input)?;
        let (input, _) = recognize(char('('))(input)?;
        let (input, column_refs) = separated_list1(tag(","), parse_id)(input)?;
        let (input, _) = recognize(char(')'))(input)?;
        let (input, _) = parse_keyword("values")(input)?;
        let (input, _) = recognize(char('('))(input)?;
        let (input, column_values) = separated_list1(tag(","), InsertValue::parse)(input)?;
        let (input, _) = recognize(char(')'))(input)?;

        Ok((
            input,
            Statement::Insert(Insert {
                column_refs,
                column_values,
                table_name,
            }),
        ))
    }

    fn parse_show_tables(input: &str) -> IResult<&str, Statement> {
        let (input, _) = parse_keyword("show")(input)?;
        value(Statement::ShowTables {}, parse_keyword("tables"))(input)
    }

    pub fn parse(input: &str) -> IResult<&str, Statement> {
        alt((
            Statement::parse_create_table,
            Statement::parse_select,
            Statement::parse_insert,
            Statement::parse_show_tables,
            Statement::parse_csv_import
        ))(input)
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub column_type: ColumnType,
}

impl ColumnSpec {
    fn parse(input: &str) -> IResult<&str, ColumnSpec> {
        map(
            tuple((parse_id, ColumnType::parse)),
            |(name, column_type)| ColumnSpec { name, column_type },
        )(input)
    }
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum ColumnType {
    Varchar { max_length: u32 },
    Number,
    Boolean,
}

impl ColumnType {
    fn parse_varchar(input: &str) -> IResult<&str, ColumnType> {
        let (input, _) = parse_keyword("varchar")(input)?;
        let (input, _) = parse_keyword("(")(input)?;
        let (input, max_length) = preceded(multispace0, terminated(u32, multispace0))(input)?;
        let (input, _) = parse_keyword(")")(input)?;
        Ok((input, ColumnType::Varchar { max_length }))
    }

    fn parse(input: &str) -> IResult<&str, ColumnType> {
        alt((
            ColumnType::parse_varchar,
            value(ColumnType::Number, parse_keyword("number")),
            value(ColumnType::Boolean, parse_keyword("boolean")),
        ))(input)
    }
}

fn parse_keyword<'a>(expected_keyword: &'a str) -> impl Fn(&'a str) -> IResult<&'a str, &'a str> {
    move |input| {
        recognize(preceded(
            multispace0,
            terminated(tag_no_case(expected_keyword), multispace0),
        ))(input)
    }
}


fn parse_id(input: &str) -> IResult<&str, String> {
    map(
        tuple((
            preceded(multispace0, alpha1),
            terminated(alphanumeric0, multispace0),
        )),
        |(start, rest)| format!("{}{}", start, rest),
    )(input)
}

#[cfg(test)]
mod tests {
    use std::vec;

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
        assert_eq!("", remaining);
        assert_eq!("foobar", matched);
    }

    #[test]
    fn test_create_table() {
        let (remaining, matched) =
            Statement::parse("CREATE TABLE person(name varchar(128), age number, male boolean)")
                .unwrap();
        assert_eq!("", remaining);
        assert_eq!(
            Statement::CreateTable(CreateTable {
                table_name: "person".to_string(),
                column_specs: vec![
                    ColumnSpec {
                        name: "name".to_string(),
                        column_type: ColumnType::Varchar { max_length: 128 }
                    },
                    ColumnSpec {
                        name: "age".to_string(),
                        column_type: ColumnType::Number
                    },
                    ColumnSpec {
                        name: "male".to_string(),
                        column_type: ColumnType::Boolean
                    },
                ]
            }),
            matched
        );

        let (remaining, matched) = Statement::parse("   CREATE     TABLE person(  name   varchar ( 255 )\n,   age  number,    male   boolean)\n").unwrap();
        assert_eq!("\n", remaining);
        assert_eq!(
            Statement::CreateTable(CreateTable {
                table_name: "person".to_string(),
                column_specs: vec![
                    ColumnSpec {
                        name: "name".to_string(),
                        column_type: ColumnType::Varchar { max_length: 255 }
                    },
                    ColumnSpec {
                        name: "age".to_string(),
                        column_type: ColumnType::Number
                    },
                    ColumnSpec {
                        name: "male".to_string(),
                        column_type: ColumnType::Boolean
                    },
                ]
            }),
            matched
        );
    }

    #[test]
    fn test_select() {
        let (remaining, matched) = Statement::parse("select * from person").unwrap();
        assert_eq!("", remaining);
        assert_eq!(
            Statement::Select(Select {
                column_refs: vec![SelectColumnReference::Wildcard],
                table_name: "person".to_string()
            }),
            matched
        );

        let (remaining, matched) = Statement::parse("select name, age from person").unwrap();
        assert_eq!("", remaining);
        assert_eq!(
            Statement::Select(Select {
                column_refs: vec![
                    SelectColumnReference::Named {
                        column_name: "name".to_string()
                    },
                    SelectColumnReference::Named {
                        column_name: "age".to_string()
                    }
                ],
                table_name: "person".to_string()
            }),
            matched
        );
    }

    #[test]
    fn test_insert() {
        let (remaining, matched) =
            Statement::parse("insert into person(name, age, male) values (\"Martin\", 35, true)")
                .unwrap();
        assert_eq!("", remaining);
        assert_eq!(
            Statement::Insert(Insert {
                column_refs: vec!["name".to_string(), "age".to_string(), "male".to_string()],
                column_values: vec![
                    InsertValue::Varchar {
                        value: "Martin".to_string()
                    },
                    InsertValue::Number { value: 35 },
                    InsertValue::Boolean { value: true }
                ],
                table_name: "person".to_string()
            }),
            matched
        );
        let (remaining, matched) =
            Statement::parse("insert into person( name   ,     age   ,       male   ) values (   \"Martin \",   35  ,  true )")
                .unwrap();
        assert_eq!("", remaining);
        assert_eq!(
            Statement::Insert(Insert {
                column_refs: vec!["name".to_string(), "age".to_string(), "male".to_string()],
                column_values: vec![
                    InsertValue::Varchar {
                        value: "Martin ".to_string()
                    },
                    InsertValue::Number { value: 35 },
                    InsertValue::Boolean { value: true }
                ],
                table_name: "person".to_string()
            }),
            matched
        );
    }

    #[test]
    fn test_csv_import() {
        let (remaining, matched) =
            Statement::parse("import csv from \"/home/martinc/spotify.csv\" into music with (title=Title, artist=Artist, rank=Rank, date=Date, region=Region)")
                .unwrap();
        assert_eq!("", remaining);
        assert_eq!(
            Statement::CsvImport(CsvImport {
                table_name: "music".to_string(),
                column_mapping: HashMap::from_iter([
                    ("title".to_string(), "Title".to_string()),
                    ("artist".to_string(), "Artist".to_string()),
                    ("rank".to_string(), "Rank".to_string()),
                    ("date".to_string(), "Date".to_string()),
                    ("region".to_string(), "Region".to_string())
                ]),
                file_path: "/home/martinc/spotify.csv".to_string(),
                with_truncate: false
            }),
            matched
        );
    }


    #[test]
    fn test_csv_import_truncate() {
        let (remaining, matched) =
            Statement::parse("import csv from \"/home/martinc/spotify.csv\" into music with (title=Title, artist=Artist, rank=Rank, date=Date, region=Region) truncate")
                .unwrap();
        assert_eq!("", remaining);
        assert_eq!(
            Statement::CsvImport(CsvImport {
                table_name: "music".to_string(),
                column_mapping: HashMap::from_iter([
                    ("title".to_string(), "Title".to_string()),
                    ("artist".to_string(), "Artist".to_string()),
                    ("rank".to_string(), "Rank".to_string()),
                    ("date".to_string(), "Date".to_string()),
                    ("region".to_string(), "Region".to_string())
                ]),
                file_path: "/home/martinc/spotify.csv".to_string(),
                with_truncate: true
            }),
            matched
        );
    }
}
