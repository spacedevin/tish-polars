# polars-bench

TPC-H–derived workload aligned with **PDS-H** (Polars Decision Support), the same family of queries Polars, DuckDB, and others use for dataframe benchmarks — see [Polars PDS-H results](https://pola.rs/posts/benchmarks) and the open [polars-benchmark](https://github.com/pola-rs/polars-benchmark) repo.

This is **not** an official TPC-H publication; it follows the usual rules for fair comparisons: one query per shape, no extra pre-filtering outside the query, timings per query.

## Data

CSV is produced by **`gen-tpch-data`** ([`tpchgen`](https://crates.io/crates/tpchgen), pure Rust) into `data/lineitem.csv` and `data/orders.csv` (standard TPC-H column names). Generated files are **gitignored**; regenerate after clone.

| Scale factor | ~Orders | ~Lineitems | Typical use |
|--------------|---------|------------|-------------|
| `0.01`       | 15k     | 60k        | Fast CI / `npm test` |
| `0.1` (default) | 150k | 600k       | Local regression runs |
| `1.0`        | 1.5M    | ~6M        | Heavier profiling |

```bash
# From repo root (default scale 0.1)
npm run gen-data

# Or explicitly
cargo run --release --bin gen-tpch-data -- --scale 0.1 --out examples/polars-bench/data
```

From this directory:

```bash
npm run gen-data -- --scale 0.1
```

## Queries (shapes under test)

| Label | TPC-H analogue | What it stresses |
|-------|----------------|------------------|
| **Q1** | Pricing summary | Scan + multi-aggregate `GROUP BY` on `lineitem` |
| **Q6** | Forecasting revenue change | Filter-only scan + single `SUM` (no join) |
| **Q3** | Shipping priority | `lineitem` ⋈ `orders`, aggregate, sort by revenue |
| **Q10_lite** | Returned item reporting (trimmed) | Join + date / return-flag filters + `GROUP BY` + `ORDER BY` + `LIMIT 20` |

## Run

Requires generated CSV (see above).

```bash
npm run gen-data -- --scale 0.01   # or 0.1
npm start
```

From repo root, **`npm run test:bench`** regenerates at **SF 0.01** then runs the script (used by full `npm test`). **Bench paths always use `cargo --release`** (`gen-tpch-data` and `tish-polars-run`) so timings match production-style native code.

Env overrides:

- `LINEITEM_CSV` — path to `lineitem.csv`
- `ORDERS_CSV` — path to `orders.csv`

## Output (machine-readable)

Lines look like:

```
bench_ms phase=read lineitem_ms=… orders_ms=…
bench_ms q=Q1 query_ms=… rows=…
bench_ms q=Q6 query_ms=… result_rows=1 sample_json=[{"revenue":…}]
bench_ms q=Q3 query_ms=… rows=…
bench_ms q=Q10_lite query_ms=… rows=…
```

- **phase=read** — CSV parse + load (Polars), not the Tish interpreter itself.
- **query_ms** — `Polars.sql` / `Polars.sql_join` plan + `collect`.
- Q6 prints one JSON sample row for the scalar aggregate (cheap).

## Reference timings (indicative)

On one dev machine, **`--release`** build, **SF 0.01** (~60k lineitems), approximate (varies by CPU):

| Phase / query | ~ms (order of magnitude) |
|---------------|---------------------------|
| read lineitem | 10–40 |
| read orders   | 3–12 |
| Q1            | 5–25 |
| Q6            | 3–12 |
| Q3            | 5–20 |
| Q10_lite      | 3–15 |

Use **SF 0.1** when you want regression tracking closer to real data volume. Compare against raw Polars / DuckDB on the same files — see the repo [README](../../README.md) baselines section.
