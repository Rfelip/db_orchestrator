"""End-to-end: a `foreach` manifest executed on one persistent DuckDB.

This is the assembly test — declared fan-out, a session that carries
TEMP state between steps, and captured plans, all through `Executor`.
The transport is stubbed to launch the helper locally instead of over
ssh; everything above that line is the real thing.
"""

import sys

import pytest

from src.duckdb_session import write_helper
from src.executor import Executor
from src.plans import PlanStore
from src.transport import DuckDbSettings

pytest.importorskip("duckdb")


class LocalDuckDbTransport:
    """A `DuckDbSshTransport` that runs the helper on this machine.

    Same `session_command()` contract, no ssh — the session protocol is
    identical either way, and the ssh hop is not what carries the
    semantics under test."""

    name = "ssh+duckdb"

    def __init__(self, helper, settings):
        self.helper = helper
        self.settings = settings

    def session_command(self):
        return [sys.executable, str(self.helper), "--serve"]


@pytest.fixture
def transport(tmp_path):
    helper = tmp_path / "helper.py"
    write_helper(helper)
    return LocalDuckDbTransport(
        helper,
        DuckDbSettings(
            memory_limit="1GB",
            threads=2,
            temp_directory=str(tmp_path / "spill"),
            max_temp_directory_size="1GB",
            profile=True,
        ),
    )


def _manifest(tmp_path, out_dir):
    sql = tmp_path / "step.sql"
    sql.write_text(
        "COPY (SELECT {{ bkt }} AS bkt, {{ year }} AS year, count(*) AS n "
        "FROM range(10) t(i)) "
        "TO '{{ out }}/b{{ bkt }}_y{{ year }}.parquet' (FORMAT 'parquet')",
        encoding="utf-8",
    )
    seed = tmp_path / "seed.sql"
    seed.write_text("CREATE TEMP TABLE shared AS SELECT 1 AS v", encoding="utf-8")
    reader = tmp_path / "read.sql"
    reader.write_text(
        "COPY (SELECT v FROM shared) TO '{{ out }}/shared.parquet' (FORMAT 'parquet')",
        encoding="utf-8",
    )

    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        f"""
steps:
  - name: seed
    type: sql
    file: "{seed}"
  - name: read_shared
    type: sql
    file: "{reader}"
    params: {{ out: "{out_dir}" }}
  - name: fan
    type: sql
    file: "{sql}"
    params: {{ out: "{out_dir}" }}
    foreach:
      bkt: [0, 1]
      year: [2017, 2018]
""",
        encoding="utf-8",
    )
    return manifest


class TestManifestOnDuckDb:
    def test_foreach_manifest_runs_on_one_session(
        self, tmp_path, transport, monkeypatch
    ):
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        monkeypatch.chdir(tmp_path)
        plans = PlanStore(tmp_path / "plans", run_id="RUN")

        executor = Executor(
            manifest_path=_manifest(tmp_path, out_dir),
            db_config={},
            notifier_config={},
            force=True,
            duckdb_transport=transport,
            plan_store=plans,
        )
        executor.run()

        # 2 singles + 2x2 cross product.
        written = sorted(p.name for p in out_dir.glob("*.parquet"))
        assert written == [
            "b0_y2017.parquet",
            "b0_y2018.parquet",
            "b1_y2017.parquet",
            "b1_y2018.parquet",
            "shared.parquet",
        ]
        # `read_shared` only works because `seed`'s TEMP TABLE survived.
        assert (out_dir / "shared.parquet").exists()

    def test_plans_are_captured_for_every_step(self, tmp_path, transport, monkeypatch):
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        monkeypatch.chdir(tmp_path)
        plans = PlanStore(tmp_path / "plans", run_id="RUN")

        Executor(
            manifest_path=_manifest(tmp_path, out_dir),
            db_config={},
            notifier_config={},
            force=True,
            duckdb_transport=transport,
            plan_store=plans,
        ).run()

        summary = plans.summary()
        assert [s.step for s in summary.steps] == [
            "seed",
            "read_shared",
            "fan_bkt0_year2017",
            "fan_bkt0_year2018",
            "fan_bkt1_year2017",
            "fan_bkt1_year2018",
        ]
        assert summary.operator_totals

    def test_psql_step_is_refused_not_approximated(
        self, tmp_path, transport, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        manifest = tmp_path / "m.yaml"
        manifest.write_text(
            'steps:\n  - {name: p, type: psql, file: "x.sql"}\n', encoding="utf-8"
        )
        executor = Executor(
            manifest_path=manifest,
            db_config={},
            notifier_config={},
            force=True,
            duckdb_transport=transport,
        )
        with pytest.raises(SystemExit):
            executor.run()

    def test_failure_still_tears_the_session_down(
        self, tmp_path, transport, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        bad = tmp_path / "bad.sql"
        bad.write_text("SELECT * FROM table_that_is_not_there", encoding="utf-8")
        manifest = tmp_path / "m.yaml"
        manifest.write_text(
            f'steps:\n  - {{name: boom, type: sql, file: "{bad}"}}\n', encoding="utf-8"
        )
        executor = Executor(
            manifest_path=manifest,
            db_config={},
            notifier_config={},
            force=True,
            duckdb_transport=transport,
        )
        with pytest.raises(SystemExit):
            executor.run()

    def test_no_dsn_is_built_for_a_duckdb_run(self, tmp_path, transport):
        # The old constructor exploded on an empty db_config because it
        # always built a SQLAlchemy URL.
        manifest = tmp_path / "m.yaml"
        manifest.write_text("steps: []\n", encoding="utf-8")
        executor = Executor(
            manifest_path=manifest,
            db_config={},
            notifier_config={},
            duckdb_transport=transport,
        )
        assert executor.db_url is None
