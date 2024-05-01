use std::collections::{HashMap, HashSet};

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum Value {
    Varchar { value: String },
    Number { value: u64 },
    Boolean { value: bool },
}

pub struct Table {
    column_specs: Vec<ColumnSpec>,
    pages: Vec<Vec<u8>>,
    row_size: usize,
    rows_per_page: usize,
}

impl Table {
    const PAGE_SIZE: usize = 4096;

    pub fn new(column_specs: &Vec<ColumnSpec>) -> Table {
        let row_size: usize = column_specs
            .iter()
            .map(|c| match c.column_type {
                ColumnType::Varchar { max_len } => 8 + (max_len * 4),
                ColumnType::Number => 8,
                ColumnType::Boolean => 1,
            })
            .sum();
        let rows_per_page = Table::PAGE_SIZE / row_size;
        Table {
            column_specs: column_specs.clone(),
            pages: Vec::new(),
            row_size,
            rows_per_page,
        }
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ColumnSpec {
    column_name: String,
    column_type: ColumnType,
}

#[derive(Eq, PartialEq, Debug, Copy, Clone)]
pub enum ColumnType {
    Varchar { max_len: usize },
    Number,
    Boolean,
}

impl ColumnType {
    fn bytes_len(&self) -> usize {
        match self {
            ColumnType::Varchar { max_len } => 8 + max_len,
            ColumnType::Number => 8,
            ColumnType::Boolean => 1,
        }
    }
}

#[derive(Eq, PartialEq, Debug)]
pub struct Row {
    values: Vec<(Value, usize)>,
}

#[derive(Eq, PartialEq, Debug)]
pub enum RowBuildError {
    ColumnNameMismatch {
        actual: HashSet<String>,
        expected: HashSet<String>,
    },
    ValueTypeMismatch {
        column_name: String,
        expected: ColumnType,
        actual: ColumnType,
    },
}

impl Row {
    fn build(
        column_values: &HashMap<String, Value>,
        column_specs: &Vec<ColumnSpec>,
    ) -> Result<Row, RowBuildError> {
        let expected: HashSet<String> =
            column_specs.iter().map(|c| c.column_name.clone()).collect();
        let actual: HashSet<String> = column_values.keys().cloned().collect();

        if actual == expected {
            let mut res = Vec::new();
            for cs in column_specs {
                let value = column_values.get(&cs.column_name).unwrap();
                let value_type = match value {
                    Value::Varchar { value } => ColumnType::Varchar {
                        max_len: value.len(),
                    },
                    Value::Number { value: _ } => ColumnType::Number,
                    Value::Boolean { value: _ } => ColumnType::Boolean,
                };

                let type_matches = match (&cs.column_type, value_type) {
                    (
                        ColumnType::Varchar { max_len: max },
                        ColumnType::Varchar { max_len: actual },
                    ) => actual <= *max,
                    (t1, t2) => *t1 == t2,
                };

                if type_matches {
                    res.push((value.clone(), cs.column_type.bytes_len()));
                } else {
                    return Err(RowBuildError::ValueTypeMismatch {
                        column_name: cs.column_name.clone(),
                        expected: cs.column_type,
                        actual: value_type,
                    });
                }
            }
            Ok(Row { values: res })
        } else {
            Err(RowBuildError::ColumnNameMismatch { actual, expected })
        }
    }

    fn write(&self, buffer: &mut Vec<u8>, base: usize) {
        let mut offset: usize = 0;

        let mut write_byte = |b: u8| {
            buffer[base + offset] = b;
            offset += 1;
        };

        for (value, bytes_len) in self.values.iter() {
            match value {
                Value::Varchar { value } => {
                    let bytes = value.as_bytes();

                    for b in bytes.len().to_be_bytes() {
                        write_byte(b);
                    }

                    for b in bytes {
                        write_byte(*b);
                    }
                    for _ in 0..bytes_len - 8 - bytes.len() {
                        write_byte(0);
                    }
                }
                Value::Number { value } => {
                    for b in value.to_be_bytes() {
                        write_byte(b);
                    }
                }
                Value::Boolean { value } if *value => {
                    write_byte(1);
                }
                Value::Boolean { value: _ } => {
                    write_byte(0);
                }
            }
        }
    }

    fn read(&self, buffer: &Vec<u8>, column_specs: &Vec<ColumnSpec>, base: usize) -> Vec<Value> {
        let mut res = Vec::new();
        let mut offset: usize = 0;
        for cs in column_specs {
            let len = cs.column_type.bytes_len();
            let bytes = &buffer[(base + offset)..(base + offset + len)];

            let value = match cs.column_type {
                ColumnType::Varchar { max_len: _ } => {
                    let str_len_bytes: [u8; 8] = bytes[0..8].try_into().unwrap();
                    let str_len = usize::from_be_bytes(str_len_bytes);
                    let str_bytes = &bytes[8..8 + str_len];
                    Value::Varchar {
                        value: String::from_utf8(Vec::from(str_bytes)).unwrap(),
                    }
                }
                ColumnType::Number => {
                    let fixed_bytes: [u8; 8] = bytes.try_into().unwrap();
                    Value::Number {
                        value: u64::from_be_bytes(fixed_bytes),
                    }
                }
                ColumnType::Boolean => Value::Boolean {
                    value: bytes[0] == 1,
                },
            };

            res.push(value);
            offset += len;
        }

        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_build_missing_values() {
        let column_specs = vec![
            ColumnSpec {
                column_name: "foo".to_string(),
                column_type: ColumnType::Boolean,
            },
            ColumnSpec {
                column_name: "bar".to_string(),
                column_type: ColumnType::Boolean,
            },
            ColumnSpec {
                column_name: "baz".to_string(),
                column_type: ColumnType::Boolean,
            },
        ];
        let column_values = HashMap::from([("bar".to_string(), Value::Boolean { value: true })]);

        let result = Row::build(&column_values, &column_specs).err();

        let expected_error = RowBuildError::ColumnNameMismatch {
            actual: column_values.keys().cloned().collect(),
            expected: HashSet::from(["foo".to_string(), "bar".to_string(), "baz".to_string()]),
        };
        assert_eq!(Some(expected_error), result);
    }

    #[test]
    fn test_row_build_type_mismatch_bool() {
        let column_specs = vec![ColumnSpec {
            column_name: "foo".to_string(),
            column_type: ColumnType::Boolean,
        }];
        let column_values = HashMap::from([("foo".to_string(), Value::Number { value: 42 })]);

        let result = Row::build(&column_values, &column_specs).err();

        let expected_error = RowBuildError::ValueTypeMismatch {
            column_name: "foo".to_string(),
            expected: ColumnType::Boolean,
            actual: ColumnType::Number,
        };
        assert_eq!(Some(expected_error), result);
    }

    #[test]
    fn test_row_build_type_mismatch_varchar() {
        let column_specs = vec![ColumnSpec {
            column_name: "foo".to_string(),
            column_type: ColumnType::Varchar { max_len: 4 },
        }];
        let column_values = HashMap::from([(
            "foo".to_string(),
            Value::Varchar {
                value: "hello".to_string(),
            },
        )]);

        let result = Row::build(&column_values, &column_specs).err();

        let expected_error = RowBuildError::ValueTypeMismatch {
            column_name: "foo".to_string(),
            expected: ColumnType::Varchar { max_len: 4 },
            actual: ColumnType::Varchar { max_len: 5 },
        };
        assert_eq!(Some(expected_error), result);
    }

    #[test]
    fn test_row_build() {
        let column_specs = vec![
            ColumnSpec {
                column_name: "foo".to_string(),
                column_type: ColumnType::Boolean,
            },
            ColumnSpec {
                column_name: "bar".to_string(),
                column_type: ColumnType::Varchar { max_len: 5 },
            },
            ColumnSpec {
                column_name: "baz".to_string(),
                column_type: ColumnType::Number,
            },
        ];
        let column_values = HashMap::from([
            ("foo".to_string(), Value::Boolean { value: true }),
            (
                "bar".to_string(),
                Value::Varchar {
                    value: "hello".to_string(),
                },
            ),
            ("baz".to_string(), Value::Number { value: 42 }),
        ]);

        let result = Row::build(&column_values, &column_specs).ok();

        let expected = Row {
            values: vec![
                (Value::Boolean { value: true }, 1),
                (
                    Value::Varchar {
                        value: "hello".to_string(),
                    },
                    8 + 5,
                ),
                (Value::Number { value: 42 }, 8),
            ],
        };
        assert_eq!(Some(expected), result);
    }

    #[test]
    fn test_table_row_size() {
        let column_specs = vec![
            ColumnSpec {
                column_name: "foo".to_string(),
                column_type: ColumnType::Boolean,
            },
            ColumnSpec {
                column_name: "bar".to_string(),
                column_type: ColumnType::Varchar { max_len: 5 },
            },
            ColumnSpec {
                column_name: "baz".to_string(),
                column_type: ColumnType::Number,
            },
        ];

        let table = Table::new(&column_specs);

        assert_eq!(table.row_size, 1 + (8 + (5 * 4)) + 8);
    }

    #[test]
    fn test_row_roundtrip() {
        let column_specs = vec![
            ColumnSpec {
                column_name: "foo".to_string(),
                column_type: ColumnType::Boolean,
            },
            ColumnSpec {
                column_name: "bar".to_string(),
                column_type: ColumnType::Varchar { max_len: 5 },
            },
            ColumnSpec {
                column_name: "baz".to_string(),
                column_type: ColumnType::Number,
            },
        ];
        let values = vec![
            Value::Boolean { value: true },
            Value::Varchar {
                value: "foo".to_string(),
            },
            Value::Number { value: 42 },
        ];
        let column_values = column_specs
            .iter()
            .map(|c| c.column_name.clone())
            .zip(values.iter().cloned())
            .collect();

        let row = Row::build(&column_values, &column_specs).unwrap();
        let mut buffer: Vec<u8> = vec![0; Table::PAGE_SIZE];
        row.write(&mut buffer, 0);
        let result = row.read(&buffer, &column_specs, 0);

        assert_eq!(values, result);
    }
}
