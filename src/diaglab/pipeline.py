"""Pipeline steps as plain functions. The CLI and the Airflow DAG call the same ones."""
import argparse
import json
import os
import shutil

from . import bugs, gen, regress, spark_jobs

BASELINE, CANDIDATE = "27A1", "27A2"


def _salt():
    return os.environ.get("DIAGLAB_SALT", "demo-salt-change-me")


def _p(data_dir, *parts):
    return os.path.join(data_dir, *parts)


def step_generate(data_dir, seed=7):
    shutil.rmtree(data_dir, ignore_errors=True)
    return gen.generate(_p(data_dir, "raw"), seed=seed)


def step_ingest(data_dir):
    spark = spark_jobs.spark_session()
    counts = spark_jobs.ingest(spark, _p(data_dir, "raw"), _p(data_dir, "out"), _salt())
    _merge_health(data_dir, counts)
    return counts


def step_aggregate(data_dir):
    spark = spark_jobs.spark_session()
    h = spark_jobs.aggregate(spark, _p(data_dir, "out"))
    _merge_health(data_dir, h)
    return h


def step_detect(data_dir, baseline=BASELINE, candidate=CANDIDATE):
    with open(_p(data_dir, "out", "summary.json")) as f:
        summary = json.load(f)
    regs = regress.detect(summary, baseline, candidate)
    with open(_p(data_dir, "out", "regressions.json"), "w") as f:
        json.dump([{**r.__dict__, "severity": r.severity, "fingerprint": bugs.fingerprint(r)}
                   for r in regs], f, indent=1)
    return regs


def step_file_bugs(data_dir, regs, sink=None):
    sink = sink or bugs.PrintSink()
    return bugs.file_bugs(regs, _p(data_dir, "out", "filed.json"), sink)


def _merge_health(data_dir, extra):
    path = _p(data_dir, "out", "health.json")
    cur = {}
    if os.path.exists(path):
        with open(path) as f:
            cur = json.load(f)
    cur.update(extra)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(cur, f, indent=1)


def main(argv=None):
    ap = argparse.ArgumentParser(prog="diaglab")
    ap.add_argument("command", choices=["demo"])
    ap.add_argument("--dir", default="data")
    ap.add_argument("--repo", help="owner/name; with GITHUB_TOKEN set, files real issues")
    a = ap.parse_args(argv)
    print("generate", step_generate(a.dir))
    print("ingest", step_ingest(a.dir))
    print("aggregate", step_aggregate(a.dir))
    regs = step_detect(a.dir)
    print(f"regressions {BASELINE} -> {CANDIDATE}: {len(regs)}")
    sink = None
    if a.repo and os.environ.get("GITHUB_TOKEN"):
        sink = bugs.GitHubSink(a.repo, os.environ["GITHUB_TOKEN"])
    filed, skipped = step_file_bugs(a.dir, regs, sink)
    print(f"bugs filed {filed}, already filed {skipped}")
    print(f"serve:  DATA_DIR={a.dir}/out uvicorn diaglab.api:app --port 8000")


if __name__ == "__main__":
    main()
