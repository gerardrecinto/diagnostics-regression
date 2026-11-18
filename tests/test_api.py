import os

import pytest
from fastapi.testclient import TestClient

from diaglab.api import create_app


@pytest.fixture()
def client(built):
    return TestClient(create_app(os.path.join(built["dir"], "out")))


def test_builds_exclude_suppressed(client):
    assert client.get("/v1/builds").json() == ["27A1", "27A2"]


def test_regressions_endpoint(client):
    r = client.get("/v1/regressions", params={"baseline": "27A1", "candidate": "27A2"})
    assert r.status_code == 200
    assert {(x["app"], x["metric"]) for x in r.json()} == {("Photos", "launch_ms"), ("Maps", "battery_pct_per_hr")}


def test_suppressed_build_is_404(client):
    r = client.get("/v1/regressions", params={"baseline": "27A1", "candidate": "27A3"})
    assert r.status_code == 404


def test_bad_input_is_422(client):
    assert client.get("/v1/summary", params={"build": "../etc/passwd"}).status_code == 422
    assert client.get("/v1/summary", params={"limit": 9999}).status_code == 422
    assert client.get("/v1/regressions", params={"baseline": "a", "candidate": "b", "alpha": 0}).status_code == 422


def test_summary_filters(client):
    rows = client.get("/v1/summary", params={"build": "27A2", "app": "Photos"}).json()
    assert len(rows) == 3 and all(x["app"] == "Photos" for x in rows)


def test_metrics_exposed(client):
    client.get("/healthz")
    text = client.get("/metrics").text
    assert 'diaglab_http_requests_total{route="/healthz",status="200"} 1.0' in text
    assert 'diaglab_pipeline_rows{stage="silver_rows"}' in text
    assert "diaglab_summary_age_seconds" in text


def test_503_before_pipeline_runs(tmp_path):
    assert TestClient(create_app(str(tmp_path))).get("/v1/builds").status_code == 503


def test_dashboard_and_alerts_use_real_metric_names(client):
    import json
    import re
    root = os.path.join(os.path.dirname(__file__), "..", "deploy")
    with open(os.path.join(root, "grafana", "dashboard.json")) as f:
        dash = json.load(f)
    with open(os.path.join(root, "prometheus", "alerts.yml")) as f:
        alerts = f.read()
    exprs = " ".join(t["expr"] for p in dash["panels"] for t in p["targets"]) + " " + alerts
    client.get("/healthz")
    exposed = client.get("/metrics").text
    for name in set(re.findall(r"diaglab_[a-z_]+", exprs)):
        assert name in exposed, f"{name} is queried but never exposed"
