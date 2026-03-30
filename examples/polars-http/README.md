# polars-http Example

HTTP server that serves Polars DataFrame as JSON on `/` or `/data`, with `/health` for health checks. Suitable for platform deployment with scale-to-zero.

## Run locally

**Automated smoke test** (starts the server, hits `/health`, exits) — from this directory:

```bash
npm test
```

**Interactive server** (blocks until Ctrl+C):

```bash
npm run test:manual
# or: npm start
```

From the tish-polars repo root:

```bash
cargo build --bin tish-polars-run
cd examples/polars-http
cargo run --manifest-path ../../Cargo.toml --bin tish-polars-run -- src/main.tish
```

Then open:

- http://localhost:8080/ or http://localhost:8080/data – DataFrame as JSON
- http://localhost:8080/health – 200 OK

## Platform deploy

Build `tish-polars-run` and bundle the example files. Use [tish.yaml](tish.yaml) for platform config (`port: 8080`, `protocol: http`).
