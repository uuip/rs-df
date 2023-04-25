use polars::prelude::*;

const HEADERS: [&str; 70] = [
    "ID1", "ID2", "T1", "IND", "PRO", "F11", "F12", "F13", "F14", "F21A", "F21B", "F22A", "F22B",
    "F23A", "F23B", "F24A", "F24B", "F25A", "F25B", "F26A", "F26B", "D11", "D12", "D21A", "D21B",
    "D21C", "D22A", "D22B", "D31", "M11", "M12", "M21A", "M21B", "M22A", "M22B", "M31A", "M31B",
    "M31C", "M32", "M43", "M51", "M52", "M53A", "M53B", "M53C", "M53D", "C11", "C12", "C13", "S21",
    "S22", "S31", "R11", "R12", "R13", "D13", "C32", "C33", "M21NEW", "M61", "M62", "M71", "M72",
    "C12NEW", "C13NEW", "C14A", "C14B", "C14C", "DJJG", "XZQH",
];

pub fn schemas() -> Schema {
    let mut schema = vec![
        Field::new("ID1", DataType::Utf8),
        Field::new("ID2", DataType::Utf8),
        Field::new("T1", DataType::Int32),
        Field::new("IND", DataType::Utf8),
        Field::new("PRO", DataType::Int32),
        Field::new("DJJG", DataType::Int32),
        Field::new("XZQH", DataType::Int32),
    ];
    for x in &HEADERS[5..] {
        let x = *x;
        let f = if (x == "D22A") | (x == "D22B") {
            Field::new(x, DataType::Float64)
        } else {
            Field::new(x, DataType::Float32)
        };
        schema.push(f);
    }

    Schema::from_iter(schema.into_iter())
}
