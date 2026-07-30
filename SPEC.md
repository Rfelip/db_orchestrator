# SPEC — native-DuckDB manifest execution

**Status:** implemented 2026-07-30. Derived from Ruan's task brief, which is the
signed-off intent statement; this file is the repo-resident record of it (Rule 3
lifecycle). Motivating gap analysis lives in the consumer repo:
`scripts_tabua/docs/arquitetura/lacuna-orquestrador-duckdb.md`.

## Intent

Make the orchestrator able to execute a manifest against **native DuckDB on a
remote host**, and capture the execution plan of every statement it runs.

Four deliverables, in the brief's words:

1. **(c) Persistent session for `DuckDbSshTransport`.** One remote DuckDB process
   per run instead of one per statement, so `TEMP TABLE`s and macros survive
   across statements.
2. **(d) `foreach` in the manifest schema.** Repetition declared, not generated.
3. **DuckDB configuration owned here**, settable from `.env`, overridable per run.
4. **Per-statement plan capture**, stored per run + per step, with a usable summary.

## Inputs / outputs

| Boundary | In | Out |
|---|---|---|
| `Step.from_dict` | one YAML step mapping | validated `Step` (unknown keys rejected) |
| `ManifestConfig.from_dict` | YAML manifest mapping | `ManifestConfig` with `foreach` already expanded to concrete steps |
| `DuckDbSettings.from_mapping` | a `DB_TARGET_<NAME>_*` mapping | validated `DuckDbSettings` |
| `DuckDbSession.run` | one SQL statement | `RawResult` (has a result set) \| `Completed` (does not) |
| `PlanStore.record` | step name, wall seconds, DuckDB profile JSON | file under `<root>/<run_id>/` + one `index.jsonl` line |
| `load_run` | plan root + run id | `RunSummary` |

## Invariants

- **Session state.** Two `run()` calls on the same `DuckDbSession` see the same
  DuckDB connection: a `TEMP TABLE` created by the first is visible to the second.
- **Teardown.** Leaving `open_duckdb_session(...)` closes the remote process,
  whether the block exited normally or by exception.
- **Honest returns.** A statement with no result set returns `Completed`, never an
  empty `RawResult`. `RawResult` means "there was a result set".
- **Unknown keys still fail.** Adding `foreach` does not loosen `Step` validation.
- **Cross product.** `foreach` with two axes expands to their cartesian product, in
  declaration order, last axis varying fastest.
- **Bounded spill.** `DuckDbSettings` rejects an empty `temp_directory` and an
  unbounded `max_temp_directory_size` — MR3 is shared and an unbounded spill takes
  down another user's session.
- **Backward compatibility.** Oracle and Postgres manifest execution is unchanged:
  no existing call signature changed, every new parameter defaults to the old
  behaviour.

## Non-goals

- Running the tábua pipeline. MR3 is shared; this work ships untested against it.
- Option (b) from the gap analysis — injecting a `Transport` into `Executor` so
  `--target` works in manifest mode. Not chosen, not built. See "Known gaps".
- Bind parameters over the ssh transports. Callers render before executing.

## Known gaps

- **`foreach` steps are never auto-disabled.** `YamlManager.disable_step` matches
  by name; expanded names (`load_bkt00`) do not exist in the source YAML, so the
  call is a logged no-op. This is *safe* (a hand-written manifest is never
  rewritten) but it means a `foreach` manifest is not resumable by the
  `enabled: false` mechanism. `ManifestConfig.from_dict` logs a warning when it
  expands anything.

## Success criteria

- Full suite green before and after.
- A test proves `TEMP TABLE` survives across `run()` calls against a **real**
  DuckDB (not a mock).
- A test proves teardown happens when the session body raises.
- `foreach` tests cover: single axis, cross product, unknown-key rejection.
