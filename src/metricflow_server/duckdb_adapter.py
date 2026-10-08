"""Connection lifecycle for atomically replaced DuckDB snapshots."""

from __future__ import annotations

import threading
from collections.abc import Callable

from dbt.mp_context import get_mp_context
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import (
    AdapterBackedSqlClient,
)


def refreshed_client(client: AdapterBackedSqlClient) -> AdapterBackedSqlClient:
    """Create an independent dbt adapter pool pointing at the same profile path."""
    adapter = client._adapter
    return AdapterBackedSqlClient(type(adapter)(adapter.config, get_mp_context()))


def reopen_environment(client: AdapterBackedSqlClient) -> None:
    """Drop dbt-duckdb's process-global connection to the replaced inode."""
    manager = client._adapter.ConnectionManager
    with manager._LOCK:
        environment = manager._ENV
        if environment is not None:
            environment.close()
            manager._ENV = None


class ManagedEngine:
    """Keep a retired client's connections alive until its active queries finish."""

    def __init__(self, engine, client: AdapterBackedSqlClient, ready=None):
        self._engine = engine
        self._client = client
        self._lock = threading.Lock()
        self._active = 0
        self._retired = False
        self._closed = False
        self._successor = None
        self._on_drained: Callable[[Exception | None], None] = lambda _: None
        self._failure: Exception | None = None
        self._ready = ready or threading.Event()
        if ready is None:
            self._ready.set()

    def __getattr__(self, name):
        return getattr(self._engine, name)

    def release(self) -> None:
        self._ready.set()
        self._finish_if_drained()

    def fail(self, error: Exception) -> None:
        self._failure = error
        self._ready.set()
        self._finish_if_drained()

    def _finish_if_drained(self) -> None:
        with self._lock:
            should_close = (
                self._retired
                and self._active == 0
                and self._ready.is_set()
                and not self._closed
            )
            if should_close:
                self._closed = True
                on_drained = self._on_drained
        if should_close:
            error = None
            try:
                self._client.close()
            except Exception as exc:  # noqa: BLE001 - callback records retirement failure
                error = exc
            on_drained(error)

    def query(self, request):
        self._ready.wait()
        with self._lock:
            if self._failure is not None:
                raise RuntimeError("DuckDB snapshot refresh failed") from self._failure
            successor = self._successor if self._retired else None
            if successor is None and self._retired:
                raise RuntimeError("MetricFlow engine is closed")
            if successor is None:
                self._active += 1
        if successor is not None:
            return successor.query(request)
        try:
            return self._engine.query(request)
        finally:
            with self._lock:
                self._active -= 1
            self._finish_if_drained()

    def retire(self, successor=None, on_drained=lambda _: None):
        with self._lock:
            self._retired = True
            self._successor = successor
            self._on_drained = on_drained
        self._finish_if_drained()
