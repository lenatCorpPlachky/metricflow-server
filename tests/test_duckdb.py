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
        "      schema: main\n      threads: 1\n      read_only: true\n"
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
