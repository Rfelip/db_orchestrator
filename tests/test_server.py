"""Tests for the connection-REUSE batch server (`src/server.py`).

These run WITHOUT the live MR3 DB by pointing a `direct` target at a local
sqlite file (SQLAlchemy's stdlib-sqlite dialect needs no extra driver). They
exercise the real stack — `TransportPool` → `PersistentDirectTransport` →
`DatabaseManager` engine — so the "one engine, reused across fetches"
property is verified end to end, plus the JSON line framing, utf-8, and
error propagation the client relies on.

No pytest required: run with `python -m tests.test_server` (or
`python tests/test_server.py` from the repo root). Each `test_*` is a plain
function; `main()` runs them all and exits non-zero on the first failure.
"""
from __future__ import annotations

import io
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.server import (  # noqa: E402
    TransportPool, PersistentDirectTransport, serve, _handle_request,
)


def _sqlite_targets(db_path: str) -> dict:
    """A resolver returning a `direct` sqlite target (mimics load_targets)."""
    return {
        "LOCAL": {
            "transport": "direct",
            "dialect": "sqlite",
            "user": "", "password": "", "host": "", "port": "",
            "database": db_path,
        }
    }


def _make_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    # SQLAlchemy sqlite URL is sqlite:///<abs path>; _build_db_url produces
    # sqlite://:@:/<database>, which sqlite tolerates as a host-less URL only
    # if we feed an absolute path as `database`. Seed a table to read back.
    import sqlite3
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE IF NOT EXISTS t(id INTEGER, name TEXT)")
    con.execute("INSERT INTO t VALUES (1, 'açaí'), (2, 'café')")
    con.commit()
    con.close()
    return path


def _pool_for(path: str) -> TransportPool:
    return TransportPool(target_resolver=lambda name: _sqlite_targets(path)[name])


# `_build_db_url` always emits `dialect://user:pass@host:port/db` — correct for
# the real MR3 targets (which always have host+port), but SQLAlchemy's sqlite
# dialect rejects the empty host:port. For tests we patch the URL builder used
# by PersistentDirectTransport to emit a proper `sqlite:///<path>` URL. This
# touches only how the engine string is formed; the reuse/framing logic under
# test is unchanged.
def _install_sqlite_url_builder() -> None:
    import src.transport as _t

    orig = _t._build_db_url

    def patched(db_config):
        if str(db_config.get("dialect", "")).startswith("sqlite"):
            return f"sqlite:///{db_config['database']}"
        return orig(db_config)

    _t._build_db_url = patched


_install_sqlite_url_builder()


def test_build_db_url_sqlite_shape():
    # Confirm our sqlite target resolves to a URL SQLAlchemy accepts.
    from src.transport import _build_db_url
    url = _build_db_url(_sqlite_targets("/tmp/x.sqlite")["LOCAL"])
    # sqlite is forgiving about the empty authority; the path is what matters.
    assert "x.sqlite" in url, url


def test_persistent_transport_reuses_one_engine():
    path = _make_db()
    try:
        pool = _pool_for(path)
        tp = pool.get("LOCAL")
        assert isinstance(tp, PersistentDirectTransport)
        # Same instance returned on the second get → pooled, not rebuilt.
        assert pool.get("LOCAL") is tp
        # First call builds the engine; capture its identity.
        tp.execute("SELECT 1")
        eng1 = tp._manager().engine
        # Many more fetches must reuse THE SAME engine object (the whole point).
        for _ in range(20):
            r = tp.execute("SELECT id, name FROM t ORDER BY id")
            assert r.rows == [(1, "açaí"), (2, "café")], r.rows
        assert tp._manager().engine is eng1, "engine was rebuilt — no reuse!"
        pool.close()
    finally:
        os.unlink(path)


def test_serve_framing_and_reuse_over_pipes():
    path = _make_db()
    out_csv = tempfile.mkdtemp()
    try:
        sql1 = Path(tempfile.mkstemp(suffix=".sql")[1])
        sql1.write_text("SELECT id, name FROM t ORDER BY id", encoding="utf-8")
        o1 = Path(out_csv) / "a.csv"
        o2 = Path(out_csv) / "b.csv"
        reqs = "\n".join([
            json.dumps({"id": 1, "sql_file": str(sql1), "target": "LOCAL",
                        "output": str(o1)}),
            json.dumps({"id": 2, "sql_file": str(sql1), "target": "LOCAL",
                        "output": str(o2)}),
            json.dumps({"id": 3, "cmd": "shutdown"}),
        ]) + "\n"
        stdin = io.StringIO(reqs)
        stdout = io.StringIO()
        pool = _pool_for(path)
        rc = serve(stdin=stdin, stdout=stdout, pool=pool)
        assert rc == 0
        lines = [l for l in stdout.getvalue().splitlines() if l.strip()]
        # readiness handshake + 2 responses (shutdown produces none)
        assert json.loads(lines[0]) == {"ready": True}, lines[0]
        r1 = json.loads(lines[1]); r2 = json.loads(lines[2])
        assert r1 == {"id": 1, "ok": True, "rows": 2, "error": None}, r1
        assert r2["id"] == 2 and r2["ok"] is True
        # Both CSVs written, utf-8 preserved.
        body = o1.read_text(encoding="utf-8")
        assert "açaí" in body and "café" in body, body
        # Only ONE engine across both fetches → reuse held over the pipe path.
        assert pool.get("LOCAL")._manager().engine is not None
        pool.close()
    finally:
        os.unlink(path)


def test_missing_sql_file_is_error_not_crash():
    path = _make_db()
    try:
        pool = _pool_for(path)
        resp = _handle_request(
            {"id": 9, "sql_file": "/no/such/file.sql", "target": "LOCAL",
             "output": str(Path(tempfile.mkdtemp()) / "x.csv")},
            pool,
        )
        assert resp["ok"] is False
        assert resp["error"].startswith("ERROR:")
        assert "not found" in resp["error"].lower()
        pool.close()
    finally:
        os.unlink(path)


def test_bad_target_error_propagates():
    path = _make_db()
    try:
        pool = _pool_for(path)  # resolver only knows LOCAL
        resp = _handle_request(
            {"id": 1, "sql_file": str(Path(tempfile.mkstemp(suffix='.sql')[1])),
             "target": "NOPE", "output": str(Path(tempfile.mkdtemp()) / "x.csv")},
            pool,
        )
        assert resp["ok"] is False and resp["error"].startswith("ERROR:")
        pool.close()
    finally:
        os.unlink(path)


def test_malformed_json_line_yields_error_line():
    path = _make_db()
    try:
        stdin = io.StringIO('not json\n' + json.dumps({"cmd": "shutdown"}) + "\n")
        stdout = io.StringIO()
        rc = serve(stdin=stdin, stdout=stdout, pool=_pool_for(path))
        assert rc == 0
        lines = [l for l in stdout.getvalue().splitlines() if l.strip()]
        assert json.loads(lines[0]) == {"ready": True}
        bad = json.loads(lines[1])
        assert bad["ok"] is False and "bad request JSON" in bad["error"]
        assert "missing_source" not in bad, "dead field must not appear in response"
    finally:
        os.unlink(path)


def test_response_has_no_missing_source_field():
    """Defect 4: missing_source is removed from the wire protocol — client
    does classification by substring matching, not this server field."""
    path = _make_db()
    try:
        sql_file = Path(tempfile.mkstemp(suffix=".sql")[1])
        sql_file.write_text("SELECT id FROM t", encoding="utf-8")
        out = Path(tempfile.mkdtemp()) / "out.csv"
        resp = _handle_request(
            {"id": 1, "sql_file": str(sql_file), "target": "LOCAL",
             "output": str(out)},
            _pool_for(path),
        )
        assert resp["ok"] is True
        assert "missing_source" not in resp, (
            f"missing_source must not appear in success response: {resp}"
        )
        # Verify error responses also lack the field.
        err_resp = _handle_request(
            {"id": 2, "sql_file": "/no/such/file.sql", "target": "LOCAL",
             "output": str(out)},
            _pool_for(path),
        )
        assert err_resp["ok"] is False
        assert "missing_source" not in err_resp, (
            f"missing_source must not appear in error response: {err_resp}"
        )
    finally:
        os.unlink(path)


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            import traceback
            print(f"FAIL {t.__name__}: {exc}")
            traceback.print_exc()
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
