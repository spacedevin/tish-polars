#!/usr/bin/env node
/**
 * Starts polars-http on a free PORT, GET /health, then stops the child.
 * Run from tish-polars repo root: node scripts/smoke-polars-http.mjs
 */
import { spawn } from "node:child_process";
import http from "node:http";
import { fileURLToPath } from "node:url";
import path from "node:path";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const httpExampleDir = path.join(root, "examples", "polars-http");
const port = Number(process.env.SMOKE_PORT || "18080");
const timeoutMs = Number(process.env.SMOKE_TIMEOUT_MS || "15000");

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      let body = "";
      res.on("data", (c) => (body += c));
      res.on("end", () => resolve({ status: res.statusCode, body }));
    });
    req.on("error", reject);
    req.setTimeout(5000, () => {
      req.destroy();
      reject(new Error("request timeout"));
    });
  });
}

const child = spawn(
  "cargo",
  [
    "run",
    "--quiet",
    "--manifest-path",
    path.join("..", "..", "Cargo.toml"),
    "--bin",
    "tish-polars-run",
    "--",
    "src/main.tish",
  ],
  {
    cwd: httpExampleDir,
    env: { ...process.env, PORT: String(port) },
    stdio: ["ignore", "pipe", "pipe"],
  }
);

let stderr = "";
child.stderr?.on("data", (d) => {
  stderr += d.toString();
});

const deadline = Date.now() + timeoutMs;
let ok = false;

while (Date.now() < deadline) {
  try {
    const r = await httpGet(`http://127.0.0.1:${port}/health`);
    if (r.status === 200 && String(r.body).includes("OK")) {
      ok = true;
      break;
    }
  } catch {
    // server not up yet
  }
  await new Promise((r) => setTimeout(r, 200));
}

child.kill("SIGTERM");
await new Promise((r) => setTimeout(r, 500));
try {
  child.kill("SIGKILL");
} catch {
  /* ignore */
}

if (!ok) {
  console.error("smoke-polars-http: /health did not return 200 OK in time");
  if (stderr) console.error(stderr.slice(-2000));
  process.exit(1);
}

console.log("smoke-polars-http: ok");
