# tish-polars Compatibility

Declared Tish version support and feature matrix for `tish-polars`.

## Tish crate alignment

This package depends on path crates under the main Tish repo:

| Cargo dependency   | Package name     | Path (relative to tish-polars) |
|--------------------|------------------|--------------------------------|
| `tishlang_core`    | `tishlang_core`  | `../tish/crates/tish_core`     |
| `tishlang_eval`    | `tishlang_eval`  | `../tish/crates/tish_eval`     |
| `tishlang_parser`  | `tishlang_parser`| `../tish/crates/tish_parser`   |

## Interpreter features

Examples that use HTTP, files, or `process.env` need **`tishlang_eval`** built with:

| Feature   | Required by examples              |
|-----------|-----------------------------------|
| `http`    | `polars-http` (`serve`, timers)  |
| `fs`      | `polars-cli` (`writeFile`)       |
| `process` | env vars (`DATA_PATH`, `PORT`)  |

The `tish-polars-run` binary enables all three.

## Virtual builtin `tish:polars`

`import { Polars } from 'tish:polars'` is satisfied by **`TishNativeModule::virtual_builtin_modules`** on `PolarsModule`, wired through **`tishlang_eval::Evaluator::with_modules`**. The default interpreter without Polars does not provide `tish:polars`.

## Compiled output (`tish:polars` / Zectre)

The compiler resolves `tish:polars` using `package.json` (`zectre` / native module metadata) and links this crate’s **`polars_object()`** for `read_csv`, `read_parquet`, `write_parquet`, `sql`, and `sql_join`. The **`tishlang_runtime`** crate does not define a separate `polars` Cargo feature; Polars lives in **`tish-polars`**.

## Polars (Rust)

- **polars** `0.44` with `lazy`, `csv`, `json`, `parquet`, `sql`

## MSRV

Matches Tish’s minimum supported Rust version (Rust 2021 edition).
