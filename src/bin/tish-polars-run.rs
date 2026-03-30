//! tish-polars-run: run .tish scripts with Polars + fs + http support.

use std::env;
use std::fs;
use std::path::Path;

use tishlang_eval::Evaluator;
use tishlang_parser;
use tish_polars::PolarsModule;

fn main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    let script_path = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("./src/main.tish");

    let path = Path::new(script_path);
    let source = fs::read_to_string(path).map_err(|e| format!("Failed to read {}: {}", script_path, e))?;

    let program = tishlang_parser::parse(&source)?;
    let polars = PolarsModule;
    let mut eval = Evaluator::with_modules(&[&polars]);
    eval.set_current_dir(path.parent());
    eval.eval_program(&program)?;
    eval.run_timer_phase()?;
    Ok(())
}
