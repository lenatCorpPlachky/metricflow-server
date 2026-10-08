from __future__ import annotations

import logging
import shutil
import tempfile
import threading
from pathlib import Path

from dbt.adapters.factory import get_adapter_by_type
from dbt.cli.main import dbtRunner
from dbt.config.runtime import load_profile, load_project
from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import (
    AdapterBackedSqlClient,
)
from metricflow.engine.metricflow_engine import MetricFlowEngine
from metricflow_semantics.model.dbt_manifest_parser import (
    parse_manifest_from_dbt_generated_manifest,
)
from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

from metricflow_server.config import settings
from metricflow_server.duckdb_adapter import (
    ManagedEngine,
    refreshed_client,
    reopen_environment,
)

logger = logging.getLogger(__name__)


class EngineManager:
    def __init__(self) -> None:
        self._engine = None
        self._sql_client = None
        self._adapter_type = None
        self._lock = threading.Lock()
        self._refresh_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Adapter bootstrap
    # ------------------------------------------------------------------
    def init_adapter(self, profiles_dir: Path) -> None:
        tmpdir = tempfile.mkdtemp(prefix="mfserver_")
        try:
            dbt_project = (
                f"name: metricflow_server_stub\n"
                f"version: '1.0.0'\n"
                f"profile: {settings.dbt_profile_name}\n"
            )
            (Path(tmpdir) / "dbt_project.yml").write_text(dbt_project)

            logger.info("Running dbt debug to register adapter …")
            logger.info("  project-dir: %s", tmpdir)
            logger.info("  profiles-dir: %s", profiles_dir)
            result = dbtRunner().invoke(
                [
                    "debug",
                    "--quiet",
                    "--project-dir",
                    tmpdir,
                    "--profiles-dir",
                    str(profiles_dir),
                ]
            )
            # dbt debug can fail on non-critical checks (e.g. git not installed).
            # We only hard-fail if there's an exception or the connection test failed.
            if result.exception:
                raise RuntimeError(f"dbt debug raised an exception: {result.exception}")
            if not result.success:
                logger.warning(
                    "dbt debug reported failures (possibly non-critical), continuing…"
                )

            profile = load_profile(project_root=tmpdir, cli_vars={})
            load_project(tmpdir, version_check=False, profile=profile)
            adapter = get_adapter_by_type(profile.credentials.type)
            self._sql_client = AdapterBackedSqlClient(adapter)
            self._adapter_type = profile.credentials.type
            logger.info("Adapter initialised (type=%s)", profile.credentials.type)
        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)

    # ------------------------------------------------------------------
    # Manifest hot-reload
    # ------------------------------------------------------------------
    def load_manifest(self, manifest_json: str) -> None:
        with self._refresh_lock:
            self._load_manifest(manifest_json)

    def _load_manifest(self, manifest_json: str) -> None:
        if self._sql_client is None:
            raise RuntimeError("Adapter not initialised – call init_adapter first")
        if isinstance(self._engine, ManagedEngine):
            self._engine._ready.wait()

        logger.info("Parsing semantic manifest …")
        semantic_manifest = parse_manifest_from_dbt_generated_manifest(
            manifest_json_string=manifest_json
        )
        lookup = SemanticManifestLookup(semantic_manifest)
        client = (
            refreshed_client(self._sql_client)
            if self._adapter_type == "duckdb" and self._engine is not None
            else self._sql_client
        )
        engine = MetricFlowEngine(
            semantic_manifest_lookup=lookup,
            sql_client=client,
        )
        ready = threading.Event() if isinstance(self._engine, ManagedEngine) else None
        with self._lock:
            old_engine = self._engine
            self._engine = (
                ManagedEngine(engine, client, ready)
                if self._adapter_type == "duckdb"
                else engine
            )
            self._sql_client = client
        if isinstance(old_engine, ManagedEngine):

            def release_new_engine():
                try:
                    reopen_environment(old_engine._client)
                finally:
                    ready.set()

            old_engine.retire(self._engine, release_new_engine)
        logger.info("MetricFlowEngine reloaded successfully")

    def close(self) -> None:
        with self._lock:
            old_engine = self._engine
            self._engine = None
        if isinstance(old_engine, ManagedEngine):
            old_engine.retire()
        elif self._sql_client is not None:
            self._sql_client.close()

    # ------------------------------------------------------------------
    # Access
    # ------------------------------------------------------------------
    @property
    def engine(self):
        with self._lock:
            return self._engine

    @property
    def is_ready(self) -> bool:
        with self._lock:
            return self._engine is not None


engine_manager = EngineManager()
