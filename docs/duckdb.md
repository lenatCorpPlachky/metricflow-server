# DuckDB adapter research

This fork starts at upstream commit `401c5b6`. Its original lock resolves
`dbt-metricflow==0.8.2` and MetricFlow `<0.209`, whereas Marketing Factory's
validated semantic catalog uses `dbt-metricflow==0.15.0`,
`metricflow==0.213.0`, `dbt-core==1.12.5`, and `dbt-duckdb==1.11.0`.
The server must use that compatible, Apache-2.0 MetricFlow line to consume
the same dbt semantic manifest as the factory.

## Connection and SQL contract

At startup, `EngineManager.init_adapter` runs `dbt debug` with the configured
profile, calls `load_profile` and `load_project`, obtains the registered dbt
adapter via `get_adapter_by_type(profile.credentials.type)`, and wraps it in
`dbt_metricflow.cli.dbt_connectors.adapter_backed_client.AdapterBackedSqlClient`.
`load_manifest` parses the JSON with
`parse_manifest_from_dbt_generated_manifest(manifest_json_string=...)`, builds
`SemanticManifestLookup`, and passes the wrapped client to
`MetricFlowEngine(semantic_manifest_lookup=..., sql_client=...)`.

Inspection of the installed, pinned 0.15.0 client shows that
`SupportedAdapterTypes` contains `duckdb` mapped to `SqlEngine.DUCKDB` and
the DuckDB SQL plan renderer. The client accepts a dbt `BaseAdapter`, opens
`adapter.connection_named(...)` for each query, executes SQL through
`adapter.execute`, and returns `MetricFlowDataTable`. It also implements
`execute`, `dry_run`, and `close`. Thus a custom MetricFlow `SqlClient` or SQL
dialect is unnecessary for normal DuckDB queries. The fork needs the
`dbt-duckdb` extra, a profile with `type: duckdb` and `read_only: true`, and
connection lifecycle handling on snapshot replacement.

dbt-duckdb 1.11.0 keeps a process-global `DuckDBConnectionManager._ENV`.
Creating a new adapter by itself does not reopen the replaced path. The
implementation waits for active queries on the retired client to drain, closes
that environment, then releases queries on the new adapter. An in-flight
query may finish with the old snapshot; its successor sees the replacement.
The reset currently uses dbt-duckdb's private `_ENV` and `_LOCK` attributes,
which must be rechecked if the pinned adapter version changes.

The pinned MetricFlow 0.213.0 engine constructor accepts
`SemanticManifestLookup` and `SqlClient` as above. Its query request factory is
`MetricFlowQueryRequest.create(...)`; the fork's
`create_with_random_request_id(...)` call from the older API is absent and
must be updated. The parser still accepts `manifest_json_string` and returns
a semantic manifest. This API mismatch is a concrete compatibility change
required in addition to adapter registration.
The dimension enum also moved from `dbt_semantic_interfaces.type_enums` to
`metricflow_semantic_interfaces.type_enums`; the old import fails in a clean
installation of the pinned stack.

## Snapshot lifecycle

DuckDB connects to a database file identified by the profile path. After the
publisher atomically replaces that file with `os.replace`, an existing open
connection may still refer to the previous inode. Refresh must make future
queries use a fresh dbt adapter/client connection, while already running
queries finish against their original connection. The tests exercise this
with real DuckDB snapshots and concurrent requests.
