use polars::prelude::*;
use std::path::PathBuf;

use crate::dataset_util::schemas;

pub(crate) fn read_csv(path: &str) -> DataFrame {
    CsvReader::from_path(path)
        .unwrap()
        .has_header(false)
        .with_schema(SchemaRef::from(schemas()))
        .finish()
        .unwrap()
}

pub(crate) fn read_csv_lazy(path: &str) -> LazyFrame {
    LazyCsvReader::new(path)
        .has_header(false)
        .with_schema(SchemaRef::from(schemas()))
        .finish()
        .unwrap()
}

pub(crate) fn read_parquet(path: &str) -> DataFrame {
    let mut file = std::fs::File::open(path).unwrap();
    ParquetReader::new(&mut file).finish().unwrap()
}

pub(crate) fn read_parquet_lazy(path: &str) -> LazyFrame {
    let args = ScanArgsParquet::default();
    LazyFrame::scan_parquet(path, args).unwrap()
}

pub(crate) fn write_parquet(df: &mut DataFrame, path: &str) {
    let mut file = std::fs::File::create(path).unwrap();
    ParquetWriter::new(&mut file)
        .with_row_group_size(Some(50000))
        .finish(df)
        .unwrap();
}

pub(crate) fn write_parquet_streaming(df: LazyFrame, path: &str) {
    let path = PathBuf::from(path);
    let options = ParquetWriteOptions::default();
    df.sink_parquet(path, options).unwrap()
}
