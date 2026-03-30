# tish-polars

Bindings for the mind blowing Polars and [Tish](https://github.com/tishlang/tish). Exposes Polars DataFrames and operations to Tish scripts.

**License:** [Pay It Forward (PIF)](https://payitforwardlicense.com/) — see [LICENSE](LICENSE).  
**Contributing:** See [tish CONTRIBUTING](https://github.com/tishlang/tish/blob/main/CONTRIBUTING.md) for build/test and PR guidelines.

## Usage (Rust embedder)

Use `Evaluator::with_modules` from **`tishlang_eval`** to load Polars (and call `run_timer_phase` after `eval_program` if you use HTTP timers):

```rust
use tishlang_eval::Evaluator;
use tish_polars::PolarsModule;

let mut eval = Evaluator::with_modules(&[&PolarsModule]);
eval.set_current_dir(Some(script_dir));
let result = eval.eval_program(&program)?;
#[cfg(feature = "http")]
eval.run_timer_phase()?;
```

The module registers a global **`Polars`** object and a virtual builtin **`tish:polars`** so scripts can write `import { Polars } from 'tish:polars'`.

## Tish API

**`Polars` methods (interpreter and compiled `polars_object`):**

| Method | Description |
|--------|-------------|
| `read_csv(path)` | Load CSV into a DataFrame opaque |
| `read_parquet(path)` | Load Parquet |
| `write_parquet(df, path)` | Write Parquet |
| `sql(df, query)` | Run SQL; frame is registered as table **`t`** |
| `sql_join(left, right, query)` | Two frames as **`l`** and **`r`** |

**DataFrame methods:** `select([...])`, `shape()`, `head(n?)`, `tail(n?)`, `toJson()` / `to_json()`.

```tish
import { Polars } from 'tish:polars';

let df = Polars.read_csv("data.csv");
df = df.select(["name", "age"]);
let [rows, cols] = df.shape();
let agg = Polars.sql(df, "SELECT name, SUM(age) AS s FROM t GROUP BY name");
console.log(agg.toJson());
```

Native filesystem and HTTP use separate modules, for example:

```tish
import { writeFile } from 'tish:fs';
import { process } from 'tish:process';
```

## Examples

Examples use the `tish-polars-run` binary (Polars + `tish:fs` + `tish:http` + `tish:process`):

### polars-cli

Batch job: parse CSV, compute summary, write `data/results.json`.

```bash
cd examples/polars-cli
cargo run --manifest-path ../../Cargo.toml --bin tish-polars-run -- src/main.tish
```

### polars-http

HTTP server: DataFrame as JSON on `/` and `/data`, health at `/health`.

```bash
cd examples/polars-http
cargo run --manifest-path ../../Cargo.toml --bin tish-polars-run -- src/main.tish
```

### polars-bench

TPC-H–style **PDS-H** workload: four standard query shapes (Q1, Q6, Q3, Q10-lite) on generated `lineitem` / `orders` CSV. See [examples/polars-bench/README.md](examples/polars-bench/README.md).

```bash
npm run gen-data -- --scale 0.1    # writes examples/polars-bench/data/*.csv (release)
cd examples/polars-bench
cargo run --release --manifest-path ../../Cargo.toml --bin tish-polars-run -- src/main.tish
```

### polars-sql-join

Two CSVs and `Polars.sql_join` with SQL on tables `l` / `r`.

```bash
cd examples/polars-sql-join
cargo run --manifest-path ../../Cargo.toml --bin tish-polars-run -- src/main.tish
```

See each example’s README and [tish.yaml](examples/polars-http/tish.yaml) for platform hints.

## Testing

From the **repo root** (needs [Node.js](https://nodejs.org/) for the HTTP smoke test):

```bash
npm test
```

That builds **`tish-polars-run`** and **`gen-tpch-data`** (debug profile for a quick compile check), runs each batch example **from its own directory** (so `./data/...` paths work). **`test:bench`** uses **`cargo --release`** for **`gen-tpch-data`** and **`tish-polars-run`** so benchmark numbers reflect optimized native code. **polars-http** is smoke-tested on port `18080` via [scripts/smoke-polars-http.mjs](scripts/smoke-polars-http.mjs).

Individual scripts:

| Command | What it runs |
|---------|----------------|
| `npm run build` | Build `tish-polars-run` + `gen-tpch-data` |
| `npm run gen-data` | Generate bench CSV (**`--release`**, optional `--scale`, default `0.1`) |
| `npm run test:cli` | `examples/polars-cli` |
| `npm run test:bench` | Regenerate bench CSV (default SF **0.01**), then `examples/polars-bench` (**`--release`**) |
| `npm run test:sql-join` | `examples/polars-sql-join` |
| `npm run test:http` | HTTP smoke only |

Set **`BENCH_SCALE`** (e.g. `0.1` or `1`) so **`npm test`** / **`npm run test:bench`** regenerate a larger TPC-H dataset; default is `0.01` for a fast CI-sized run. Inside an example folder you can use `npm start` / `npm test` (same env applies to `examples/polars-bench`). Override the smoke port with `SMOKE_PORT` if `18080` is taken.

## Baselines (performance sanity checks)

Timings in `polars-bench` include Polars work plus interpreter overhead. To see whether Polars itself is slow or the embedding is, compare against a plain Polars one-liner (same machine, release builds):

```bash
# After: npm run gen-data -- --scale 0.1
python -c "import polars as pl; lf=pl.scan_csv('examples/polars-bench/data/lineitem.csv'); print(lf.filter(pl.col('l_shipdate') <= pl.lit('1998-09-02')).group_by(['l_returnflag','l_linestatus']).agg(pl.col('l_quantity').sum()).collect())"
```

Use [hyperfine](https://github.com/sharkdp/hyperfine) to compare `tish-polars-run` against other CLIs on the same input; treat results as regression detectors, not benchmarks of “language speed.”

## Building

From this directory (uses `../tish/crates/` for Tish dependencies):

```bash
cd tish-polars && cargo build
```

Runner binary:

```bash
cargo build --bin tish-polars-run
```
