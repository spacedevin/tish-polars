//! Polars bindings for Tish.
//!
//! Exposes Polars DataFrame and operations to Tish scripts.

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use polars::io::SerReader;
use polars::io::SerWriter;
use polars::prelude::*;
use polars::sql::SQLContext;
use tishlang_core::{NativeFn, TishOpaque, Value as CoreValue};
use tishlang_eval::{PropMap, TishNativeModule, Value as EvalValue};

/// Wrapper around Polars DataFrame for Tish (`Arc` avoids cloning the frame on each method dispatch).
pub struct TishDataFrame {
    pub inner: Arc<DataFrame>,
}

impl TishDataFrame {
    pub fn new(df: DataFrame) -> Self {
        Self {
            inner: Arc::new(df),
        }
    }

    fn select(&self, args: &[CoreValue]) -> CoreValue {
        let cols = match args.first() {
            Some(CoreValue::Array(arr)) => arr
                .borrow()
                .iter()
                .filter_map(|v| match v {
                    CoreValue::String(s) => Some(s.to_string()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            _ => return CoreValue::Null,
        };
        match self.inner.as_ref().select(cols.as_slice()) {
            Ok(df) => CoreValue::Opaque(Arc::new(TishDataFrame::new(df))),
            Err(_) => CoreValue::Null,
        }
    }

    fn to_json(&self, _args: &[CoreValue]) -> CoreValue {
        let mut buf = Vec::new();
        let mut df = self.inner.as_ref().clone();
        match JsonWriter::new(&mut buf)
            .with_json_format(JsonFormat::Json)
            .finish(&mut df)
        {
            Ok(()) => match String::from_utf8(buf) {
                Ok(s) => CoreValue::String(s.into()),
                Err(_) => CoreValue::Null,
            },
            Err(_) => CoreValue::Null,
        }
    }

    fn shape(&self, _args: &[CoreValue]) -> CoreValue {
        let (rows, cols) = self.inner.shape();
        let arr = vec![
            CoreValue::Number(rows as f64),
            CoreValue::Number(cols as f64),
        ];
        CoreValue::Array(Rc::new(RefCell::new(arr)))
    }

    fn head(&self, args: &[CoreValue]) -> CoreValue {
        let n = args
            .first()
            .and_then(|v| match v {
                CoreValue::Number(n) => {
                    let u = *n as usize;
                    if u == 0 && *n != 0.0 {
                        None
                    } else {
                        Some(u)
                    }
                }
                _ => None,
            })
            .unwrap_or(10);
        let df = self.inner.head(Some(n));
        CoreValue::Opaque(Arc::new(TishDataFrame::new(df)))
    }

    fn tail(&self, args: &[CoreValue]) -> CoreValue {
        let n = args
            .first()
            .and_then(|v| match v {
                CoreValue::Number(n) => {
                    let u = *n as usize;
                    if u == 0 && *n != 0.0 {
                        None
                    } else {
                        Some(u)
                    }
                }
                _ => None,
            })
            .unwrap_or(10);
        let df = self.inner.tail(Some(n));
        CoreValue::Opaque(Arc::new(TishDataFrame::new(df)))
    }
}

impl TishOpaque for TishDataFrame {
    fn type_name(&self) -> &'static str {
        "DataFrame"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_method(&self, name: &str) -> Option<NativeFn> {
        let inner = Arc::clone(&self.inner);
        match name {
            "select" => Some(Rc::new(move |args: &[CoreValue]| {
                TishDataFrame { inner: inner.clone() }.select(args)
            })),
            "toJson" | "to_json" => Some(Rc::new(move |args: &[CoreValue]| {
                TishDataFrame { inner: inner.clone() }.to_json(args)
            })),
            "shape" => Some(Rc::new(move |args: &[CoreValue]| {
                TishDataFrame { inner: inner.clone() }.shape(args)
            })),
            "head" => Some(Rc::new(move |args: &[CoreValue]| {
                TishDataFrame { inner: inner.clone() }.head(args)
            })),
            "tail" => Some(Rc::new(move |args: &[CoreValue]| {
                TishDataFrame { inner: inner.clone() }.tail(args)
            })),
            _ => None,
        }
    }
}

fn arc_dataframe_from_eval(v: &EvalValue) -> Result<Arc<DataFrame>, String> {
    match v {
        EvalValue::Opaque(o) => o
            .as_any()
            .downcast_ref::<TishDataFrame>()
            .map(|t| Arc::clone(&t.inner))
            .ok_or_else(|| "expected DataFrame opaque".to_string()),
        _ => Err("expected DataFrame".to_string()),
    }
}

fn arc_dataframe_from_core(v: &CoreValue) -> Result<Arc<DataFrame>, String> {
    match v {
        CoreValue::Opaque(o) => o
            .as_any()
            .downcast_ref::<TishDataFrame>()
            .map(|t| Arc::clone(&t.inner))
            .ok_or_else(|| "expected DataFrame opaque".to_string()),
        _ => Err("expected DataFrame".to_string()),
    }
}

/// Runtime function for Polars.read_csv (compiled output).
pub fn polars_read_csv_runtime(args: &[CoreValue]) -> CoreValue {
    use std::path::PathBuf;
    let path = args.first().map(|v| v.to_display_string()).unwrap_or_default();
    let path_buf: PathBuf = path.into();
    match CsvReadOptions::default().try_into_reader_with_file_path(Some(path_buf)) {
        Ok(reader) => match reader.finish() {
            Ok(df) => CoreValue::Opaque(Arc::new(TishDataFrame::new(df))),
            Err(_) => CoreValue::Null,
        },
        Err(_) => CoreValue::Null,
    }
}

/// Read CSV from in-memory string (for compile-time embedded data).
pub fn polars_read_csv_from_string_runtime(csv_content: &str) -> CoreValue {
    use std::io::Cursor;
    match CsvReader::new(Cursor::new(csv_content.as_bytes())).finish() {
        Ok(df) => CoreValue::Opaque(Arc::new(TishDataFrame::new(df))),
        Err(_) => CoreValue::Null,
    }
}

fn polars_read_parquet_runtime(args: &[CoreValue]) -> CoreValue {
    let path = args.first().map(|v| v.to_display_string()).unwrap_or_default();
    let path_buf: PathBuf = path.into();
    match File::open(&path_buf) {
        Ok(f) => match ParquetReader::new(f).finish() {
            Ok(df) => CoreValue::Opaque(Arc::new(TishDataFrame::new(df))),
            Err(_) => CoreValue::Null,
        },
        Err(_) => CoreValue::Null,
    }
}

fn polars_write_parquet_runtime(args: &[CoreValue]) -> CoreValue {
    let df = match args.first() {
        Some(v) => match arc_dataframe_from_core(v) {
            Ok(a) => a,
            Err(_) => return CoreValue::Null,
        },
        None => return CoreValue::Null,
    };
    let path = args.get(1).map(|v| v.to_display_string()).unwrap_or_default();
    if path.is_empty() {
        return CoreValue::Null;
    }
    let path_buf: PathBuf = path.into();
    let mut file = match File::create(&path_buf) {
        Ok(f) => f,
        Err(_) => return CoreValue::Null,
    };
    let mut write_df = (*df).clone();
    match ParquetWriter::new(&mut file).finish(&mut write_df) {
        Ok(_) => CoreValue::Null,
        Err(_) => CoreValue::Null,
    }
}

/// `Polars.sql(df, query)` — registers the frame as table `t`.
fn polars_sql_runtime(args: &[CoreValue]) -> CoreValue {
    let df = match args.first() {
        Some(v) => match arc_dataframe_from_core(v) {
            Ok(a) => a,
            Err(_) => return CoreValue::Null,
        },
        None => return CoreValue::Null,
    };
    let query = args.get(1).map(|v| v.to_display_string()).unwrap_or_default();
    if query.is_empty() {
        return CoreValue::Null;
    }
    let mut ctx = SQLContext::new();
    ctx.register("t", df.as_ref().clone().lazy());
    match ctx.execute(query.as_str()) {
        Ok(lf) => match lf.collect() {
            Ok(out) => CoreValue::Opaque(Arc::new(TishDataFrame::new(out))),
            Err(_) => CoreValue::Null,
        },
        Err(_) => CoreValue::Null,
    }
}

/// `Polars.sql_join(left, right, query)` — tables `l` and `r`.
fn polars_sql_join_runtime(args: &[CoreValue]) -> CoreValue {
    let left = match args.first() {
        Some(v) => match arc_dataframe_from_core(v) {
            Ok(a) => a,
            Err(_) => return CoreValue::Null,
        },
        None => return CoreValue::Null,
    };
    let right = match args.get(1) {
        Some(v) => match arc_dataframe_from_core(v) {
            Ok(a) => a,
            Err(_) => return CoreValue::Null,
        },
        None => return CoreValue::Null,
    };
    let query = args.get(2).map(|v| v.to_display_string()).unwrap_or_default();
    if query.is_empty() {
        return CoreValue::Null;
    }
    let mut ctx = SQLContext::new();
    ctx.register("l", left.as_ref().clone().lazy());
    ctx.register("r", right.as_ref().clone().lazy());
    match ctx.execute(query.as_str()) {
        Ok(lf) => match lf.collect() {
            Ok(out) => CoreValue::Opaque(Arc::new(TishDataFrame::new(out))),
            Err(_) => CoreValue::Null,
        },
        Err(_) => CoreValue::Null,
    }
}

/// Polars object for compiled Tish output.
pub fn polars_object() -> CoreValue {
    tishlang_core::tish_module! {
        "read_csv" => polars_read_csv_runtime,
        "read_parquet" => polars_read_parquet_runtime,
        "write_parquet" => polars_write_parquet_runtime,
        "sql" => polars_sql_runtime,
        "sql_join" => polars_sql_join_runtime,
    }
}

pub fn polars_read_csv(args: &[EvalValue]) -> Result<EvalValue, String> {
    use std::path::PathBuf;
    let path = args.first().map(|v| v.to_string()).unwrap_or_default();
    let path_buf: PathBuf = path.into();
    match CsvReadOptions::default().try_into_reader_with_file_path(Some(path_buf)) {
        Ok(reader) => match reader.finish() {
            Ok(df) => Ok(EvalValue::Opaque(Arc::new(TishDataFrame::new(df)))),
            Err(e) => Err(e.to_string()),
        },
        Err(e) => Err(e.to_string()),
    }
}

pub fn polars_read_parquet(args: &[EvalValue]) -> Result<EvalValue, String> {
    let path = args.first().map(|v| v.to_string()).unwrap_or_default();
    let path_buf: PathBuf = path.into();
    let f = File::open(&path_buf).map_err(|e| e.to_string())?;
    ParquetReader::new(f)
        .finish()
        .map(|df| EvalValue::Opaque(Arc::new(TishDataFrame::new(df))))
        .map_err(|e| e.to_string())
}

pub fn polars_write_parquet(args: &[EvalValue]) -> Result<EvalValue, String> {
    let df = args
        .first()
        .ok_or_else(|| "write_parquet: missing DataFrame".to_string())
        .and_then(arc_dataframe_from_eval)?;
    let path = args
        .get(1)
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "write_parquet: missing path".to_string())?;
    let path_buf: PathBuf = path.into();
    let mut file = File::create(&path_buf).map_err(|e| e.to_string())?;
    let mut write_df = (*df).clone();
    ParquetWriter::new(&mut file)
        .finish(&mut write_df)
        .map_err(|e| e.to_string())?;
    Ok(EvalValue::Null)
}

pub fn polars_sql(args: &[EvalValue]) -> Result<EvalValue, String> {
    let df = args
        .first()
        .ok_or_else(|| "sql: missing DataFrame".to_string())
        .and_then(arc_dataframe_from_eval)?;
    let query = args
        .get(1)
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "sql: missing query string".to_string())?;
    let mut ctx = SQLContext::new();
    ctx.register("t", df.as_ref().clone().lazy());
    let out = ctx
        .execute(query.as_str())
        .map_err(|e| e.to_string())?
        .collect()
        .map_err(|e| e.to_string())?;
    Ok(EvalValue::Opaque(Arc::new(TishDataFrame::new(out))))
}

pub fn polars_sql_join(args: &[EvalValue]) -> Result<EvalValue, String> {
    let left = args
        .first()
        .ok_or_else(|| "sql_join: missing left DataFrame".to_string())
        .and_then(arc_dataframe_from_eval)?;
    let right = args
        .get(1)
        .ok_or_else(|| "sql_join: missing right DataFrame".to_string())
        .and_then(arc_dataframe_from_eval)?;
    let query = args
        .get(2)
        .map(|v| v.to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "sql_join: missing query string".to_string())?;
    let mut ctx = SQLContext::new();
    ctx.register("l", left.as_ref().clone().lazy());
    ctx.register("r", right.as_ref().clone().lazy());
    let out = ctx
        .execute(query.as_str())
        .map_err(|e| e.to_string())?
        .collect()
        .map_err(|e| e.to_string())?;
    Ok(EvalValue::Opaque(Arc::new(TishDataFrame::new(out))))
}

fn polars_exports_object() -> EvalValue {
    let mut polars = PropMap::default();
    polars.insert("read_csv".into(), EvalValue::Native(polars_read_csv));
    polars.insert("read_parquet".into(), EvalValue::Native(polars_read_parquet));
    polars.insert("write_parquet".into(), EvalValue::Native(polars_write_parquet));
    polars.insert("sql".into(), EvalValue::Native(polars_sql));
    polars.insert("sql_join".into(), EvalValue::Native(polars_sql_join));
    EvalValue::Object(Rc::new(RefCell::new(polars)))
}

/// Polars native module for Tish.
pub struct PolarsModule;

impl TishNativeModule for PolarsModule {
    fn name(&self) -> &'static str {
        "Polars"
    }

    fn register(&self) -> HashMap<Arc<str>, EvalValue> {
        let mut scope = HashMap::new();
        scope.insert(Arc::from("Polars"), polars_exports_object());
        scope
    }

    fn virtual_builtin_modules(&self) -> Vec<(&'static str, EvalValue)> {
        let mut exports = PropMap::default();
        exports.insert("Polars".into(), polars_exports_object());
        vec![(
            "tish:polars",
            EvalValue::Object(Rc::new(RefCell::new(exports))),
        )]
    }
}
