"""Optional MCP query paths against the pinned MetricFlow request API."""

import json
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("mcp")

from metricflow_server.mcp_server import get_dimension_values, query_metrics


@pytest.fixture
def engine():
    result = MagicMock()
    result.sql = "select project, clicks from facts"
    result.result_df.column_names = ["project", "clicks"]
    result.result_df.rows = [("alpha", 20)]
    engine = MagicMock()
    engine.query.return_value = result
    with patch("metricflow_server.mcp_server.engine_manager._engine", engine):
        yield engine


def test_mcp_get_dimension_values(engine):
    values = json.loads(get_dimension_values(["clicks"], "project", limit=5))
    assert values == ["alpha"]
    request = engine.query.call_args.args[0]
    assert request.metric_names == ["clicks"]
    assert request.group_by_names == ["project"]
    assert request.limit == 5


def test_mcp_query_metrics(engine):
    result = json.loads(query_metrics(["clicks"], group_by=["project"], limit=5))
    assert result == {
        "sql": "select project, clicks from facts",
        "rows": [{"project": "alpha", "clicks": 20}],
    }
    request = engine.query.call_args.args[0]
    assert request.metric_names == ["clicks"]
    assert request.group_by_names == ["project"]
    assert request.limit == 5
