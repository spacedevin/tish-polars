//! Generate TPC-H LINEITEM and ORDERS as CSV for polars-bench (tpchgen, pure Rust).
//!
//! Usage: gen-tpch-data [--scale 0.1] [--out DIR]
//! Default scale 0.1 (~600k lineitems, ~150k orders). Use 0.01 for faster CI.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

use tpchgen::csv::{LineItemCsv, OrderCsv};
use tpchgen::generators::{LineItemGenerator, OrderGenerator};

fn main() -> Result<(), String> {
    let mut scale = 0.1_f64;
    let mut out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/polars-bench/data");

    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--scale" => {
                scale = args
                    .next()
                    .ok_or_else(|| "--scale needs a value".to_string())?
                    .parse()
                    .map_err(|e| format!("invalid --scale: {e}"))?;
            }
            "--out" => {
                out_dir = PathBuf::from(
                    args
                        .next()
                        .ok_or_else(|| "--out needs a path".to_string())?,
                );
            }
            "--help" | "-h" => {
                eprintln!(
                    "Usage: gen-tpch-data [--scale 0.1] [--out DIR]\n\
                     Writes lineitem.csv and orders.csv (TPC-H schema) for polars-bench."
                );
                return Ok(());
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }

    if scale <= 0.0 {
        return Err("scale must be positive".into());
    }

    fs::create_dir_all(&out_dir).map_err(|e| format!("create_dir_all: {e}"))?;

    let started = Instant::now();

    let orders_path = out_dir.join("orders.csv");
    let mut orders_out = BufWriter::new(File::create(&orders_path).map_err(|e| e.to_string())?);
    let order_gen = OrderGenerator::new(scale, 1, 1);
    let orders_expected = OrderGenerator::calculate_row_count(scale, 1, 1);
    writeln!(orders_out, "{}", OrderCsv::header()).map_err(|e| e.to_string())?;
    let mut orders_written = 0_i64;
    for row in order_gen.iter() {
        writeln!(orders_out, "{}", OrderCsv::new(row)).map_err(|e| e.to_string())?;
        orders_written += 1;
    }
    orders_out.flush().map_err(|e| e.to_string())?;

    let lineitem_path = out_dir.join("lineitem.csv");
    let mut li_out = BufWriter::new(File::create(&lineitem_path).map_err(|e| e.to_string())?);
    let line_gen = LineItemGenerator::new(scale, 1, 1);
    writeln!(li_out, "{}", LineItemCsv::header()).map_err(|e| e.to_string())?;
    let mut line_written = 0_i64;
    for row in line_gen.iter() {
        writeln!(li_out, "{}", LineItemCsv::new(row)).map_err(|e| e.to_string())?;
        line_written += 1;
    }
    li_out.flush().map_err(|e| e.to_string())?;

    let elapsed = started.elapsed();
    eprintln!(
        "gen-tpch-data: scale={scale} orders={orders_written} (expected ~{orders_expected}) lineitem={line_written} in {:.2}s\n  {}\n  {}",
        elapsed.as_secs_f64(),
        orders_path.display(),
        lineitem_path.display()
    );

    Ok(())
}
