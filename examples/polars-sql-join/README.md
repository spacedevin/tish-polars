# polars-sql-join

Loads two CSVs and runs a SQL join + aggregation via `Polars.sql_join(left, right, query)`. Tables are named **`l`** (first argument) and **`r`** (second) in SQL.

## Run

From this directory:

```bash
npm test
```

Or from the tish-polars repo root:

```bash
cd examples/polars-sql-join
cargo run --manifest-path ../../Cargo.toml --bin tish-polars-run -- src/main.tish
```

## Platform

See [tish.yaml](tish.yaml) if you deploy this as a batch job.
