//! Test-driver transport, not a database implementation. Database calls remain
//! explicitly unsupported until a Rust engine is connected to `dispatch`.

use datalevin_codec::nippy::{Value, fast_freeze, fast_thaw};
use std::io::{self, Read, Write};

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

fn keyword(name: &str) -> Value {
    Value::Keyword(name.into())
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn map(entries: Vec<(&str, Value)>) -> Value {
    Value::Map(
        entries
            .into_iter()
            .map(|(key, value)| (keyword(key), value))
            .collect(),
    )
}

fn field<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Map(entries) => entries
            .iter()
            .find(|(k, _)| *k == keyword(key))
            .map(|(_, v)| v),
        _ => None,
    }
}

fn protocol_error(message: &str) -> Value {
    map(vec![
        ("status", keyword("error")),
        (
            "error",
            map(vec![
                ("class", text("java.lang.IllegalArgumentException")),
                ("message", text(message)),
            ]),
        ),
    ])
}

fn dispatch(request: &Value) -> Value {
    let Some(Value::Keyword(operation)) = field(request, "op") else {
        return protocol_error("Request requires an unqualified keyword :op");
    };
    let Some(Value::Vector(arguments)) = field(request, "args") else {
        return protocol_error("Request requires a vector of argument descriptors");
    };
    // Echo tests the lossless transport only. It is not a database capability.
    if operation.as_ref() == "echo" {
        if let [argument] = arguments.as_slice()
            && field(argument, "kind") == Some(&keyword("value"))
            && let Some(value) = field(argument, "value")
        {
            return map(vec![
                ("status", keyword("ok")),
                (
                    "result",
                    map(vec![("kind", keyword("value")), ("value", value.clone())]),
                ),
            ]);
        }
        return protocol_error("Echo requires exactly one value argument");
    }
    map(vec![
        ("status", keyword("unsupported")),
        ("operation", keyword(operation)),
        (
            "message",
            text(&format!(
                "Rust database operation is not implemented: {operation}"
            )),
        ),
    ])
}

fn read_frame(input: &mut impl Read) -> io::Result<Option<Value>> {
    let mut prefix = [0; 4];
    if input.read(&mut prefix[..1])? == 0 {
        return Ok(None);
    }
    input.read_exact(&mut prefix[1..])?;
    let size = u32::from_be_bytes(prefix) as usize;
    if size == 0 || size > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid adapter frame size",
        ));
    }
    let mut bytes = vec![0; size];
    input.read_exact(&mut bytes)?;
    fast_thaw(&bytes)
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))
}

fn write_frame(output: &mut impl Write, value: &Value) -> io::Result<()> {
    let bytes = fast_freeze(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "adapter response is too large",
        ));
    }
    output.write_all(&(bytes.len() as u32).to_be_bytes())?;
    output.write_all(&bytes)?;
    output.flush()
}

fn main() -> io::Result<()> {
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    write_frame(
        &mut output,
        &map(vec![
            ("protocol", text("datalevin-test-adapter")),
            ("version", Value::int(1)),
            ("backend", keyword("rust")),
            ("implementation", text(env!("CARGO_PKG_NAME"))),
            ("implementation-version", text(env!("CARGO_PKG_VERSION"))),
            (
                "revision",
                option_env!("DATALEVIN_BUILD_REVISION")
                    .map(text)
                    .unwrap_or(Value::Null),
            ),
            ("capabilities", Value::Set(vec![keyword("echo")])),
            ("database-implemented?", Value::Bool(false)),
        ]),
    )?;
    while let Some(request) = read_frame(&mut input)? {
        write_frame(&mut output, &dispatch(&request))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn frames_preserve_query_forms_and_result_shapes() {
        let value = Value::Vector(vec![
            Value::List(vec![keyword("find"), Value::int(3)]),
            Value::Set(vec![Value::int(1), Value::int(2)]),
            Value::Bytes(vec![0, 255]),
        ]);
        let request = map(vec![
            ("op", keyword("echo")),
            (
                "args",
                Value::Vector(vec![map(vec![
                    ("kind", keyword("value")),
                    ("value", value.clone()),
                ])]),
            ),
        ]);
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &request).unwrap();
        let response = dispatch(&read_frame(&mut Cursor::new(bytes)).unwrap().unwrap());
        assert_eq!(
            field(field(&response, "result").unwrap(), "value"),
            Some(&value)
        );
    }

    #[test]
    fn database_calls_are_explicitly_unsupported() {
        for operation in ["empty-db", "db-with", "q", "close-db", "future-operation"] {
            let response = dispatch(&map(vec![
                ("op", keyword(operation)),
                ("args", Value::Vector(vec![])),
            ]));
            assert_eq!(field(&response, "status"), Some(&keyword("unsupported")));
            assert_eq!(field(&response, "operation"), Some(&keyword(operation)));
        }
    }

    #[test]
    fn malformed_and_truncated_frames_are_errors() {
        for bytes in [
            vec![0],
            vec![0, 0, 0, 0],
            vec![127, 255, 255, 255],
            vec![0, 0, 0, 2, 0],
        ] {
            assert!(read_frame(&mut Cursor::new(bytes)).is_err());
        }
        assert_eq!(read_frame(&mut Cursor::new(vec![])).unwrap(), None);
        assert_eq!(
            field(&dispatch(&Value::Null), "status"),
            Some(&keyword("error"))
        );
    }
}
