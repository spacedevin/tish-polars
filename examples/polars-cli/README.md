# polars-cli Example

Batch job that parses a demo CSV, computes summary stats, and stores results in `data/results.json`. Suitable for scheduled/background runs; deployable as a platform task (no port).

## Run locally

With npm (from this directory):

```bash
npm test
# or
npm start
```

From the tish-polars repo root:

```bash
cd examples/polars-cli
cargo run --manifest-path ../../Cargo.toml --bin tish-polars-run -- src/main.tish
```

Or pass a custom data path via env:

```bash
DATA_PATH=./data/sales.csv cargo run --manifest-path ../../Cargo.toml --bin tish-polars-run -- src/main.tish
```

## Output

- `data/results.json` – summary with `rows`, `cols`, and `sample` (DataFrame as JSON)

## Platform deploy

Build `tish-polars-run` and bundle the example files (`src/main.tish`, `data/sales.csv`). Use [tish.yaml](tish.yaml) for platform config. The platform can mount a volume at `./data` for persistent results.
