"""Real DuckDB / MetricFlow integration and snapshot lifecycle tests."""

import os
import threading
from pathlib import Path

import duckdb
import pytest
from dbt.adapters.contracts.connection import LazyHandle
from dbt.cli.main import dbtRunner

from metricflow_server.engine_manager import EngineManager


def _snapshot(path: Path, clicks: tuple[int, int] = (10, 10)) -> None:
    with duckdb.connect(str(path)) as con:
        con.execute(
            "create table facts (date date, clicks integer, impressions integer)"
        )
        con.executemany(
            "insert into facts values (?, ?, ?)",
            [("2026-01-05", clicks[0], 100), ("2026-01-06", clicks[1], 900)],
        )


@pytest.fixture
def duckdb_service(tmp_path, monkeypatch):
    db_path = tmp_path / "snapshot.duckdb"
    _snapshot(db_path)
    profiles = tmp_path / "profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        f"test_duckdb:\n  target: dev\n  outputs:\n    dev:\n"
        f"      type: duckdb\n      path: '{db_path}'\n"
        "      schema: main\n      threads: 1\n"
        "      config_options:\n        access_mode: READ_ONLY\n"
    )
    project = tmp_path / "project"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        "name: test_duckdb\nversion: '1.0.0'\nconfig-version: 2\n"
        "profile: test_duckdb\nmodel-paths: [models]\n"
    )
    (project / "models" / "facts.sql").write_text("select * from main.facts")
    (project / "models" / "metricflow_time_spine.sql").write_text(
        "select cast(date_day as date) as date_day from generate_series("
        "date '2020-01-01', date '2030-01-01', interval '1 day') as days(date_day)"
    )
    (project / "models" / "time_spine.yml").write_text(
        "models:\n- name: metricflow_time_spine\n"
        "  time_spine:\n    standard_granularity_column: date_day\n"
        "  columns:\n  - name: date_day\n    granularity: day\n"
    )
    (project / "models" / "semantic.yml").write_text(
        "semantic_models:\n"
        "- name: test_facts\n  model: ref('facts')\n"
        "  defaults:\n    agg_time_dimension: date\n"
        "  dimensions:\n  - name: date\n    type: time\n"
        "    type_params:\n      time_granularity: day\n"
        "  measures:\n  - name: fact_clicks\n    agg: sum\n    expr: clicks\n"
        "  - name: fact_impressions\n    agg: sum\n    expr: impressions\n"
        "  primary_entity: test_facts\n"
        "metrics:\n"
        "- name: clicks\n  label: clicks\n  type: simple\n"
        "  type_params:\n    measure: fact_clicks\n"
        "- name: impressions\n  label: impressions\n  type: simple\n"
        "  type_params:\n    measure: fact_impressions\n"
        "- name: ctr\n  label: ctr\n  type: ratio\n"
        "  type_params:\n    numerator: clicks\n    denominator: impressions\n"
    )
    result = dbtRunner().invoke(
        ["parse", "--project-dir", str(project), "--profiles-dir", str(profiles)]
    )
    assert result.success, result.exception
    manifest = (project / "target" / "semantic_manifest.json").read_text()
    from metricflow_server import engine_manager as manager_module

    monkeypatch.setattr(manager_module.settings, "dbt_profile_name", "test_duckdb")
    manager = EngineManager()
    manager.init_adapter(profiles)
    manager.load_manifest(manifest)
    yield manager, db_path, manifest
    manager.close()


def _query(manager, metric):
    from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

    return manager.engine.query(MetricFlowQueryRequest.create(metric_names=[metric]))


def test_duckdb_query_simple_metric(duckdb_service):
    manager, _, _ = duckdb_service
    result = _query(manager, "clicks")
    assert result.result_df.rows[0][0] == 20


def test_duckdb_ratio_metric(duckdb_service):
    manager, _, _ = duckdb_service
    result = _query(manager, "ctr")
    assert result.result_df.rows[0][0] == pytest.approx(0.02)


def test_duckdb_snapshot_is_read_only(duckdb_service):
    manager, _, _ = duckdb_service
    adapter = manager._sql_client._adapter
    with adapter.connection_named("read_only_check"):
        _, table = adapter.execute("select current_setting('access_mode')", fetch=True)
        assert table.rows[0][0] == "read_only"
        with pytest.raises(Exception, match="read-only"):
            adapter.execute("create table forbidden_write (id int)", fetch=False)


def test_duckdb_rejects_writable_profile(duckdb_service, tmp_path):
    manager, db_path, _ = duckdb_service
    profiles = tmp_path / "writable_profiles"
    profiles.mkdir()
    (profiles / "profiles.yml").write_text(
        "test_duckdb:\n  target: dev\n  outputs:\n    dev:\n"
        f"      type: duckdb\n      path: '{db_path}'\n"
        "      schema: main\n      threads: 1\n"
    )
    other = EngineManager()
    with pytest.raises(ValueError, match="access_mode: READ_ONLY"):
        other.init_adapter(profiles)
    assert _query(manager, "clicks").result_df.rows[0][0] == 20


def test_refresh_reopens_connection(duckdb_service, tmp_path):
    manager, db_path, manifest = duckdb_service
    assert _query(manager, "clicks").result_df.rows[0][0] == 20
    replacement = tmp_path / "replacement.duckdb"
    _snapshot(replacement, (30, 40))
    os.replace(replacement, db_path)
    manager.load_manifest(manifest)
    assert _query(manager, "clicks").result_df.rows[0][0] == 70


def test_concurrent_query_during_refresh(duckdb_service, tmp_path, monkeypatch):
    manager, db_path, manifest = duckdb_service
    old_engine = manager.engine
    old_client = manager._sql_client
    entered = threading.Event()
    release = threading.Event()
    original_execute = old_client._adapter.execute

    def paused_execute(*args, **kwargs):
        connection = old_client._adapter.connections.get_thread_connection()
        if isinstance(connection.handle, LazyHandle):
            connection.handle.resolve(connection)
        assert connection.state == "open"
        entered.set()
        assert release.wait(timeout=10)
        return original_execute(*args, **kwargs)

    monkeypatch.setattr(old_client._adapter, "execute", paused_execute)
    result = {}

    def worker():
        try:
            result["old"] = _query(manager, "clicks").result_df.rows[0][0]
        except Exception as exc:  # noqa: BLE001 - report worker failures on the test thread
            result["error"] = exc

    thread = threading.Thread(target=worker)
    thread.start()
    assert entered.wait(timeout=10), result.get("error")
    replacement = tmp_path / "replacement.duckdb"
    _snapshot(replacement, (30, 40))
    os.replace(replacement, db_path)
    manager.load_manifest(manifest)
    release.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert "error" not in result, result.get("error")
    assert result["old"] == 20
    assert manager.engine is not old_engine
    assert _query(manager, "clicks").result_df.rows[0][0] == 70


def test_refresh_close_failure_is_unavailable_and_recovers(
    duckdb_service, tmp_path, monkeypatch
):
    from metricflow_server import engine_manager as manager_module

    manager, db_path, manifest = duckdb_service
    old_engine = manager.engine
    _query(manager, "clicks")
    replacement = tmp_path / "replacement.duckdb"
    _snapshot(replacement, (30, 40))
    os.replace(replacement, db_path)
    original = manager_module.reopen_environment

    def fail_once(client):
        monkeypatch.setattr(manager_module, "reopen_environment", original)
        raise OSError("injected close failure")

    monkeypatch.setattr(manager_module, "reopen_environment", fail_once)
    with pytest.raises(RuntimeError, match="DuckDB snapshot refresh failed") as error:
        manager.load_manifest(manifest)
    assert str(error.value.__cause__) == "injected close failure"
    assert manager.engine is None
    assert not manager.is_ready
    with pytest.raises(RuntimeError, match="DuckDB snapshot refresh failed"):
        old_engine.query(_request("clicks"))
    manager.load_manifest(manifest)
    assert _query(manager, "clicks").result_df.rows[0][0] == 70


def _request(metric):
    from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

    return MetricFlowQueryRequest.create(metric_names=[metric])


def test_shutdown_closes_environment_and_blocks_refresh(duckdb_service):
    manager, _, manifest = duckdb_service
    _query(manager, "clicks")
    environment = manager._sql_client._adapter.ConnectionManager._ENV
    assert environment.conn is not None
    manager.close()
    assert manager.engine is None
    assert manager._sql_client is None
    assert environment.conn is None
    with pytest.raises(RuntimeError, match="closed"):
        manager.load_manifest(manifest)


def test_async_refresh_close_failure_recovers(duckdb_service, tmp_path, monkeypatch):
    from metricflow_server import engine_manager as manager_module

    manager, db_path, manifest = duckdb_service
    old_client = manager._sql_client
    entered = threading.Event()
    release = threading.Event()
    original_execute = old_client._adapter.execute

    def paused_execute(*args, **kwargs):
        connection = old_client._adapter.connections.get_thread_connection()
        if isinstance(connection.handle, LazyHandle):
            connection.handle.resolve(connection)
        entered.set()
        assert release.wait(timeout=10)
        return original_execute(*args, **kwargs)

    monkeypatch.setattr(old_client._adapter, "execute", paused_execute)
    result = {}

    def worker():
        try:
            result["old"] = _query(manager, "clicks").result_df.rows[0][0]
        except Exception as exc:  # noqa: BLE001 - report worker failures
            result["error"] = exc

    thread = threading.Thread(target=worker)
    thread.start()
    assert entered.wait(timeout=10), result.get("error")
    replacement = tmp_path / "replacement.duckdb"
    _snapshot(replacement, (30, 40))
    os.replace(replacement, db_path)
    original_reopen = manager_module.reopen_environment

    def fail_once(client):
        monkeypatch.setattr(manager_module, "reopen_environment", original_reopen)
        raise OSError("async close failure")

    monkeypatch.setattr(manager_module, "reopen_environment", fail_once)
    manager.load_manifest(manifest)
    pending_engine = manager.engine
    release.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert result == {"old": 20}
    assert manager.engine is None
    assert not manager.is_ready
    with pytest.raises(RuntimeError, match="refresh failed"):
        pending_engine.query(_request("clicks"))
    manager.load_manifest(manifest)
    assert _query(manager, "clicks").result_df.rows[0][0] == 70


def test_shutdown_drains_query_before_closing_environment(duckdb_service, monkeypatch):
    manager, _, _ = duckdb_service
    old_client = manager._sql_client
    environment = old_client._adapter.ConnectionManager._ENV
    entered = threading.Event()
    release = threading.Event()
    original_execute = old_client._adapter.execute

    def paused_execute(*args, **kwargs):
        connection = old_client._adapter.connections.get_thread_connection()
        if isinstance(connection.handle, LazyHandle):
            connection.handle.resolve(connection)
        entered.set()
        assert release.wait(timeout=10)
        return original_execute(*args, **kwargs)

    monkeypatch.setattr(old_client._adapter, "execute", paused_execute)
    result = {}

    def worker():
        try:
            result["old"] = _query(manager, "clicks").result_df.rows[0][0]
        except Exception as exc:  # noqa: BLE001 - report worker failures
            result["error"] = exc

    thread = threading.Thread(target=worker)
    thread.start()
    assert entered.wait(timeout=10), result.get("error")
    manager.close()
    assert environment.conn is not None
    release.set()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert result == {"old": 20}
    assert environment.conn is None


def test_shutdown_serializes_with_refresh(duckdb_service, monkeypatch):
    from metricflow_server import engine_manager as manager_module

    manager, _, manifest = duckdb_service
    entered = threading.Event()
    release = threading.Event()
    closed = threading.Event()
    original_parse = manager_module.parse_manifest_from_dbt_generated_manifest

    def paused_parse(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=10)
        return original_parse(*args, **kwargs)

    monkeypatch.setattr(
        manager_module, "parse_manifest_from_dbt_generated_manifest", paused_parse
    )
    refresh = threading.Thread(target=manager.load_manifest, args=(manifest,))
    refresh.start()
    assert entered.wait(timeout=10)

    def shutdown():
        manager.close()
        closed.set()

    closer = threading.Thread(target=shutdown)
    closer.start()
    assert not closed.wait(timeout=0.1)
    release.set()
    refresh.join(timeout=10)
    closer.join(timeout=10)
    assert not refresh.is_alive() and not closer.is_alive()
    assert closed.is_set()
    assert manager.engine is None
    assert manager._sql_client is None
    with pytest.raises(RuntimeError, match="closed"):
        manager.load_manifest(manifest)
