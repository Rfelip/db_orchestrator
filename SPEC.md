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

*(Both gaps recorded here on 2026-07-30 were closed the same day — see
"Resume" below and `requirements-dev.txt`. Kept for the record.)*

- ~~**`foreach` steps are never auto-disabled.**~~ Closed by the run ledger.
  `YamlManager.disable_step` still matches by name and still cannot see expanded
  names — that has not changed and does not need to. Resume for expanded steps
  is `--resume`, which works off the expanded plan.

- ~~**`duckdb` is a test-only dependency and is not in `requirements.txt`.**~~
  Now in `requirements-dev.txt`, which is the same statement made structurally.

  ```sh
  uv run --with pytest --with sqlalchemy --with python-dotenv --with ruamel.yaml \
         --with jinja2 --with requests --with duckdb python -m pytest tests -q
  ```

# SPEC — resume

**Status:** implemented 2026-07-30. The 345-step tábua pipeline takes ~30 min on
a shared machine; a failure at step 300 must not cost steps 1–299.

## Intent

Make a manifest run resumable, including for `foreach`-expanded steps, without
hand-editing any generated file — and be honest about the one thing resume
cannot check.

Two mechanisms, because they answer different questions:

| Mechanism | Question | Where |
|---|---|---|
| **Run ledger** | *"resume where it broke"* | `<plan_dir>/<run_id>/ledger.jsonl`, `--resume` |
| **Window selector** | *"just run this part"* | `--from` / `--until`, substring over the expanded plan |

`--from` restores the ergonomics of the pre-existing `scripts/run_pipeline.py`
in the parent repo. The ledger is what that runner never had.

## Inputs / outputs

| Boundary | In | Out |
|---|---|---|
| `select_window` | expanded step names + `start`/`until` substrings | half-open index range, or `NoStepMatchedError` |
| `decide_resume` | `StepCheck` per planned step + ledger entries by name | `ResumeDecision` (skip prefix, unverified, resume point, reason) |
| `RunLedger.record` | step name, source sha, rendered `produces`, seconds | one flushed `ledger.jsonl` line |
| `load_resume_request` | `ResumeOptions` | `ResumeRequest` with the prior ledger loaded, or `LedgerNotFoundError` |
| `Step.from_dict` | a step mapping that may carry `produces:` | `Step` with `produces` unrendered |

## Invariants

- **Prefix, not set.** The skip list is the longest *prefix* of the plan the
  ledger proves is done. The first step that fails a check is the resume point;
  everything from there runs, even if the ledger lists it. Whatever invalidated
  a step probably invalidated its successors.
- **A step is only skipped if it is unchanged.** `source_sha` covers the SQL
  file bytes *and* the step's params, so an edited query or a changed `foreach`
  value re-runs.
- **Declared outputs are checked; undeclared ones are declared unchecked.** A
  step with `produces:` whose file is gone is not skipped. A step without
  `produces:` is skipped and counted into `ResumeDecision.unverified`, which
  `format_resume_banner` prints at the point of use with the word TRUSTING. The
  gap is surfaced, never silent.
- **A declared output's directory exists before the step runs.** The parent of
  the rendered `produces:` is created (`parents=True, exist_ok=True`) ahead of
  every step, on both the SQLAlchemy and the DuckDB path. This is what lets a
  manifest be self-sufficient: `COPY … TO` creates no directory, so otherwise a
  wrapper has to carry the folder list. A step without `produces:` declares no
  output and gets none created.
- **Chaining works.** A resumed run's ledger *inherits* the prior entries
  (keeping their original `run_id`), so resuming a resumed run still knows about
  the first run's work.
- **A mistyped selector fails.** `--from`/`--until` matching nothing raises
  rather than degrading into a 345-step run. Same for an unknown `--resume` id.
- **Ledger writes never fail a run.** An `OSError` writing the ledger logs a
  warning; it costs a resume, not a result.
- **Backward compatibility.** `ledger` and `resume` default to `None` on both
  `Executor` and `run_manifest`. Oracle / Postgres manifest execution and the
  DuckDB session path are unchanged when they are not passed.

## Non-goals

- Running the tábua pipeline. MR3 is shared.
- Replacing `disable_step`. The two compose; see "Interaction" below.
- A dependency graph. Resume is positional, because manifest execution is
  sequential.
- Verifying output *contents*. `produces:` checks existence. A truncated
  parquet from a killed writer passes — the ledger is a resume aid, not a
  validator.

## Interaction with `disable_step` and with `carga_runs`

`disable_step` writes `enabled: false` into the source YAML for steps it can
find by name; the ledger records every completed step including expanded ones.
They overlap for hand-written steps and only the ledger covers expanded ones.
**Pair `--resume` with `--enable-all`** when you want the ledger to be the sole
authority — otherwise a step already disabled in the YAML never reaches the
resume logic, so a vanished output for that step cannot be detected.

`carga_runs` (parent ADR-0004) is *not* this ledger and this is not a
duplicate of it: that is one row per **published dataset** in Postgres, carrying
git SHA and an Iceberg snapshot; this is hundreds of lines per run on local
disk, alive only until the run succeeds. They meet at the run id — the id minted
here is an id a `carga_runs` row can reference.

## Success criteria

- Full suite green before and after (202 → 252).
- A test proves an interrupted run resumes at the failed step.
- A test proves a resumed run does not re-execute a completed step (output
  mtime unchanged).
- A test proves a deleted declared output forces that step *and everything after
  it* to re-run.
- A test proves that with no `produces:` anywhere, resume skips everything and
  says out loud that it verified nothing.

# SPEC — native-DuckDB success criteria (original)

- Full suite green before and after.
- A test proves `TEMP TABLE` survives across `run()` calls against a **real**
  DuckDB (not a mock).
- A test proves teardown happens when the session body raises.
- `foreach` tests cover: single axis, cross product, unknown-key rejection.
