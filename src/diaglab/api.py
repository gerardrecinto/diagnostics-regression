"""Read-only REST API over the pipeline output, with Prometheus metrics."""
import json
import os
import time

from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from . import regress

NAME = r"^[A-Za-z0-9_.-]{1,40}$"


def create_app(data_dir=None):
    data_dir = data_dir or os.environ.get("DATA_DIR", "data/out")
    app = FastAPI(title="diaglab", version="0.1.0")
    reg = CollectorRegistry()
    reqs = Counter("diaglab_http_requests_total", "Requests", ["route", "status"], registry=reg)
    lat = Histogram("diaglab_http_request_seconds", "Latency", ["route"], registry=reg,
                    buckets=(.005, .01, .025, .05, .1, .25, .5, 1, 2.5))
    rows = Gauge("diaglab_pipeline_rows", "Rows by stage", ["stage"], registry=reg)
    age = Gauge("diaglab_summary_age_seconds", "Seconds since summary.json was written", registry=reg)

    def load(name):
        try:
            with open(os.path.join(data_dir, name)) as f:
                return json.load(f)
        except FileNotFoundError:
            raise HTTPException(503, f"{name} not built yet, run the pipeline")

    @app.middleware("http")
    async def measure(request: Request, call_next):
        t0 = time.perf_counter()
        resp = await call_next(request)
        route = request.scope.get("route")
        label = route.path if route else "unmatched"
        reqs.labels(label, str(resp.status_code)).inc()
        lat.labels(label).observe(time.perf_counter() - t0)
        return resp

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    @app.get("/v1/builds")
    def builds():
        return sorted({r["os_build"] for r in load("summary.json")})

    @app.get("/v1/summary")
    def summary(build: str | None = Query(None, pattern=NAME), app_name: str | None = Query(None, alias="app", pattern=NAME),
                metric: str | None = Query(None, pattern=NAME), limit: int = Query(100, ge=1, le=500)):
        out = [r for r in load("summary.json")
               if (not build or r["os_build"] == build) and (not app_name or r["app"] == app_name)
               and (not metric or r["metric"] == metric)]
        return out[:limit]

    @app.get("/v1/regressions")
    def regressions(baseline: str = Query(..., pattern=NAME), candidate: str = Query(..., pattern=NAME),
                    min_effect: float = Query(0.05, ge=0.0, le=5.0), alpha: float = Query(0.01, gt=0.0, le=0.5)):
        known = {r["os_build"] for r in load("summary.json")}
        if baseline not in known or candidate not in known:
            raise HTTPException(404, "build not in summary (it may have too few devices to report)")
        found = regress.detect(load("summary.json"), baseline, candidate, min_effect, alpha)
        return [{**r.__dict__, "severity": r.severity} for r in found]

    @app.get("/v1/health")
    def health():
        return load("health.json")

    @app.get("/metrics")
    def metrics():
        try:
            h = load("health.json")
            for k in ("raw_rows", "silver_rows", "rejected_rows", "unaccounted_rows",
                      "batch_duplicates_dropped"):
                if k in h:
                    rows.labels(k).set(h[k])
            age.set(time.time() - os.path.getmtime(os.path.join(data_dir, "summary.json")))
        except (HTTPException, FileNotFoundError):
            pass
        return Response(generate_latest(reg), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()
