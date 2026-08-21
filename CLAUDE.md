# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

MaCcoyS ("Mass Centric Decoy Search" — the S is silent) is a mass-spectrometry proteomics
search engine. For each MS2 spectrum it builds a *spectrum-specific* search space (targets +
decoys fetched live from a companion database, MaCPepDB) instead of searching one static
FASTA, identifies the spectrum with an internal Comet-like scorer (`xcorrrs`), rescores PSMs
with a Local Outlier Probability (LoOP) score, and publishes results to a per-search result
directory.

Note: an older PSM-validation approach based on fitting an exponential distribution and a
goodness-of-fit test is superseded by the LoOP score — `goodness_of_fit_record.rs` still exists
in the tree (wired into the `PostProcess` CLI command) but is not part of the current scoring
pipeline; don't describe it as the active scoring method.

This is a Cargo workspace with 3 members:
- `packages/maccoys` — the core CLI binary + library: the pipeline described above.
- `packages/exchange` (`maccoys-exchange-entities`) — shared, WASM-safe (de)serializable DTOs
  used between the results web API and any HTTP client.
- `packages/frontend` (`maccoys-frontend`) — a Dioxus (Rust→WASM) web UI for submitting
  searches and browsing results. Independent build/run flow from the other two (see below).

## Build & run

- Build everything: `cargo build` (release: `cargo build -r`)
- Run the CLI: `cargo run -r --package maccoys -- <args>`, or use the compiled
  `target/release/maccoys` binary directly.
- Generate a default pipeline config: `maccoys pipeline new-config`
- Run the full pipeline in one process (good for local dev/testing):
  `maccoys -vvvvvv -l <log_file> pipeline local-run <result_dir> <config.toml> <xcorr_config.toml> <mzml_files...>`
- Dump the default `PipelineConfiguration`: `maccoys config`

No CI configuration exists in this repo (no `.github/workflows`, no other CI). There are no
documented `cargo test`/`cargo clippy`/`cargo fmt` invocations either — use standard Cargo
commands (`cargo test`, `cargo clippy --workspace`, `cargo fmt`) and judgment.

### Tests

- Unit/integration tests live inline (`#[test]`/`#[tokio::test]`) in files such as
  `packages/maccoys/src/pipeline/storage.rs`, `packages/maccoys/src/pipeline/utils.rs`, and
  `packages/maccoys/src/pipeline/tasks/scoring_task.rs`.
- Redis-backed and queue-server tests use `#[serial_test::serial]` and expect a live Redis at
  `127.0.0.1:6380` — `docker compose up` (root `docker-compose.yaml`) starts exactly that.
- Run a single test: `cargo test --package maccoys <test_name>` (start Redis first if the test
  touches `RedisPipelineQueue`/`RedisPipelineStorage`/the queue server).

### Frontend (`packages/frontend`) — separate toolchain

Not built by plain `cargo build` for the wasm target; needs:
```
rustup target add wasm32-unknown-unknown
cargo install dioxus-cli
```
(The package README also mentions installing `sass` — no longer needed with the current Dioxus
version, which handles SCSS itself. Don't list it as a prerequisite.)

Then `dx serve` (dev) or `dx build --release` (produces a static `dist/` to serve, e.g. via
NGINX/Caddy — see `docker/frontend/`). Runtime config is compiled in at build time via
`MACCOYS_CONFIG=<path> dx serve` (defaults to `frontend.config.toml` at repo root).

### Docker

`docker build -t local/maccoys:latest -f docker/maccoys/Dockerfile .` builds a minimal image
containing only the `maccoys` binary (multi-stage: `rust:1.95` builder → `debian:trixie-slim`
runtime). The container's entrypoint *is* the binary — all subcommands (`pipeline
queue-server`, `pipeline remote-entrypoint`, `pipeline index`, etc.) are passed as `docker run`
args. `docker/frontend/Dockerfile` builds the WASM frontend, served by Caddy
(`docker/frontend/Caddyfile`).

Note: the package README says `-f docker/Dockerfile`; the real path is `docker/maccoys/Dockerfile`.

## Architecture: the pipeline (`packages/maccoys/src/pipeline/`)

The pipeline is six message-passing stages. The *same* stage/task code runs whether the
deployment is single-process, multi-process on one machine, or fully distributed — only the
queue/storage backend implementation changes based on config.

```
IndexingMessage → SearchSpaceGenerationMessage → IdentificationMessage
    → ScoringMessage → PublicationMessage
(any stage's failure converts its message → ErrorMessage, routed to the error queue)
```

- **`indexing_task.rs`** — streams an mzML file (via `dihardts_omicstools`), splits out each
  MS2 spectrum + precursor(s), emits one message per spectrum to stage 2.
- **`search_space_generation_task.rs`** — builds a spectrum-specific target+decoy FASTA search
  space by querying MaCPepDB (`search_space/search_space_generator.rs`), forwards to stage 3.
- **`identification_task.rs`** — runs the actual search via the internal `xcorrrs` crate,
  producing a `PeptideSpectrumMatchCollection` (`src/peptide_spectrum_match.rs`).
- **`scoring_task.rs`** — Local Outlier Probability (LoOP) rescoring over Polars DataFrames
  (features: `xcorr`, `ions_matched_ratio`, `mass_error`), producing the `loop_score` column
  (`LOOP_COL_NAME` in `scoring_task.rs`) used to rank/validate PSMs.
- **`publication_task.rs`** — writes the final result bytes to disk under
  `<result_dir>/<uuid>/<ms_run>/<spectrum_id>/...` (see `utils.rs` for the exact path
  convention at search/ms-run/spectrum/precursor level).
- **`error_task.rs`** — sink for `ErrorMessage`s from any stage; writes `.err` files at the
  appropriate level.

All six implement the shared `Task` trait (`pipeline/tasks/task.rs`) and all message types
implement `IsMessage` (`pipeline/messages/is_message.rs`), which is what lets every stage
convert its own message into an `ErrorMessage` on failure.

### Three execution modes (same task code, different `PipelineQueue`/`PipelineStorage` impl)

Selected per stage by `TaskConfiguration.queue_url` in the runtime TOML config
(`pipeline/configuration.rs`):
- **In-memory** (`queue_url` absent) — single process, `deadqueue`-backed. Driven by
  `local_pipeline.rs` (`LocalPipeline<Q, S>`), used by `pipeline local-run`.
- **Redis** (`queue_url = "redis://..."`) — `RedisPipelineQueue`/`RedisPipelineStorage`
  (`pipeline/queue.rs`, `pipeline/storage.rs`). Used for the simple distributed setup described
  in the package README (`Procfile` + `docker compose up` + `ultraman start`).
- **HTTP queue server** (`queue_url = "http://..."`) — `HttpPipelineQueue` talks over HTTP2 to
  `queue_server.rs`, a standalone broker (`pipeline queue-server`) that spools message payloads
  to disk (not memory, since messages can be large) and periodically reschedules messages whose
  consumer timed out. `remote_pipeline_web_api.rs` (`pipeline remote-entrypoint`) is the public
  Axum HTTP API that clients POST searches to (`remote_pipeline.rs` / `pipeline remote-run` is
  the client side); it is itself a client of the queue server, not the queue holder.

Each stage can also run as its own standalone process (`pipeline index`, `pipeline
search-space-generation`, `pipeline identification`, `pipeline scoring`, `pipeline publication`,
`pipeline error`) reading a minimal `Standalone*Configuration` — this is what a real distributed
deployment runs, one stage per host (see `ansible/` below and `Procfile` for the exact commands).

### Config surface

`pipeline/configuration.rs` centralizes all runtime config (TOML, not Cargo config):
`PipelineConfiguration` (used by `local-run`) vs `RemoteEntypointConfiguration` (used by the
distributed queue-server/entrypoint/standalone-stage commands) — the latter omits
`search_parameters` since those are supplied per-search by the client instead.

**Root-level `*.toml` files are runtime pipeline configs, not build config** — several are
stale relics of an older section-naming scheme (`config.toml` uses `preparation`,
`comet_search`, `goodness_and_rescoring`, `redis_url` — none of which exist in the current
`configuration.rs`). Prefer `local.http.toml` / `maccoys.local.redis.toml` /
`search_params.toml` as current-format examples.

## Deployment topology (`ansible/`, `docker/`)

Production deployment is one Docker image (`docker/maccoys/Dockerfile`) run repeatedly with a
different `pipeline <subcommand>` per role, orchestrated by a single Ansible playbook
(`ansible/main.yml`) — not docker-compose (the root `docker-compose.yaml` only stands up a
local dev Redis on port 6380). Inventory groups map 1:1 to pipeline stages
(`ansible/hosts.template.yml`): `storage` (Redis), `queue` (the HTTP queue server, port 8123),
`entrypoint` (public API, port 8000), `indexing`, `search_space_generation`, `identification`,
`scoring`, `publishing`, `error`, plus `monitoring` (Prometheus/Grafana/Loki) and `monitored`
(node-exporter on every host). All `maccoys-*` Ansible roles are thin wrappers that
`include_role: maccoys` with a different `maccoys_cmd`; the actual deploy logic (pull/load
image, template `maccoys.toml`, restart container) lives in `ansible/maccoys/tasks/main.yml`.
