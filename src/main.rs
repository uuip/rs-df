#![allow(dead_code, unused_variables)]

use polars::prelude::*;
use std::env;

use crate::io_util::*;

mod dataset_util;
mod io_util;

fn main() {
    env::set_var("POLARS_FMT_MAX_ROWS", "10");
    env::set_var("POLARS_FMT_MAX_COLS", "20");
    let o = GetOutput::from_type(DataType::Int32);
    let incl = Series::from_vec("", vec![0.0_f32]);
    let c = vec![1.0_f64, 2.0];
    let path = r"50w_2022.csv";
    // let df: DataFrame = read_csv(path);
    // let df = read_csv_lazy(path);
    let df = df!(
        "T1"=>&[Some(2020),Some(2020),Some(2020),Some(2020)],
        "ID1"=>&["aaa","aaa","aaa","aaa"],
        "M11"=>&[Some(1_f32), Some(0.0),None, Some(f32::INFINITY)],
        "M12"=>&[11,22,33,44],
        "M32"=>&[11,22,33,44],
        "M51"=>&[Some(11),Some(22),None,Some(44)],
        "F11"=>&[Some(0.0),Some(1.0),None,Some(100.0)],
        "R11"=>&[Some(0.0),Some(1.0),None,Some(100.0)],
        "R12"=>&[11,22,33,44],
        "R13"=>&[11,22,33,44],
        "D11"=>&[11,22,33,44],
        "D12"=>&[1,1,1,1],
    )
    .unwrap();
    println!("{:?}", df);

    let mut df = df
        .lazy()
        .with_columns([
            col("ID1").map(|x| Ok(Some(str_to_len(&x))), o).alias("xxx"),
            //添加列，值为100
            lit(100).alias("yyy"),
            //从一列赋值
            col("T1").alias("C33"),
            //替换值
            when(col("R11").eq(lit(0)))
                .then(lit(1))
                .when(col("R11").is_null())
                .then(lit(1))
                .otherwise(col("R11"))
                .alias("R11"),
            when(col("M11").lt(col("M12")))
                .then(col("M12"))
                .otherwise(col("M11"))
                .alias("M11"),
            when(
                col("M51")
                    .is_null()
                    .or(col("M11").is_infinite())
                    .or(col("M11").is_in(lit(incl))),
            )
            .then(lit(1000.0_f32))
            .otherwise(col("M11"))
            .alias("M51"),
            (col("R11") / (col("R11") + col("R12") + col("R13")))
                .abs()
                .alias("R1"),
        ])
        // 删除列
        .drop_columns(["M32"])
        // 过滤
        .filter((col("D12").eq(lit(1))).and(col("F11").is_not_null()))
        .sort_by_exprs([col("D11"), col("D12")], [false, false], false)
        .with_streaming(true)
        .collect()
        .unwrap();
    println!("{:#?}", df);

    let r11_rank = df.column("R11").unwrap().rank(
        RankOptions {
            method: RankMethod::Ordinal,
            descending: false,
        },
        None,
    );
    println!("{:?}", r11_rank);

    let s = df.column("R11").unwrap().cast(&DataType::Float64).unwrap();
    let s_cut = cut(&s, c, None, false, false).unwrap();
    println!("{:?}", s_cut);

    println!("{:?}", df.select(["R1", "R11", "xxx"]).unwrap().mean());

    let df1 = df.select(["ID1", "R1", "M11", "R11", "xxx"]).unwrap();
    println!("{:?}", df1.head(Some(5)));

    let _ = df
        .clone()
        .lazy()
        .join(
            df1.lazy(),
            [col("ID1")],
            [col("ID1")],
            JoinArgs::new(JoinType::Left),
        )
        .collect()
        .unwrap();
    write_parquet(&mut df, "data.pq");
    println!("{}", df.height());
    write_parquet_streaming(df.lazy(), "data.pq");
    let _ = read_parquet("data.pq");
    // let _ = read_parquet_lazy("data.pq");
}

fn str_to_len(str_val: &Series) -> Series {
    str_val
        .utf8()
        .unwrap()
        .into_iter()
        .map(|opt_name: Option<&str>| opt_name.map(|x| x.len() as u32))
        .collect::<UInt32Chunked>()
        .into_series()
}
