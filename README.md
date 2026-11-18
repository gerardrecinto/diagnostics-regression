# diagnostics-regression-lab

Devices send battery and performance numbers. This lab ingests them with Spark, strips anything private, compares one OS build to the last, and files a bug when a metric got worse. A REST API and Prometheus metrics sit on top, and an Airflow DAG runs it daily. Synthetic data, one laptop, personal project.

[![CI](https://github.com/gerardrecinto/diagnostics-regression-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/gerardrecinto/diagnostics-regression-lab/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/github/license/gerardrecinto/diagnostics-regression-lab)](LICENSE)

**[Live page: move the thresholds and watch the bugs appear](https://gerardrecinto.github.io/diagnostics-regression-lab/)**

## Run it

Python 3.10 or newer and Java 17 or newer. About a minute.

```bash
git clone https://github.com/gerardrecinto/diagnostics-regression-lab
cd diagnostics-regression-lab
python3 -m venv .venv && . .venv/bin/activate
pip install -e ".[dev]"

python -m diaglab demo
python -m pytest -q
```

The demo prints what it did. This is a real run:

```
generate {'unique': 43488, 'duplicates': 881, 'late_duplicates': 82, 'malformed': 472, 'rows': 44923}
ingest {'raw_rows': 44923, 'silver_rows': 44451, 'rejected_rows': 472, 'unaccounted_rows': 0}
aggregate {'batch_duplicates_dropped': 963, 'groups_reported': 24, 'groups_suppressed_k': 12, 'k_min_devices': 20}
regressions 27A1 -> 27A2: 2
[high] Maps battery_pct_per_hr up 26.1% in 27A2
[high] Photos launch_ms up 20.0% in 27A2
bugs filed 2, already filed 0
```

Serve the API and ask it the same question:

```bash
DATA_DIR=data/out uvicorn diaglab.api:app --port 8000
curl "localhost:8000/v1/regressions?baseline=27A1&candidate=27A2"
curl localhost:8000/metrics
```

To file real GitHub issues instead of printing them: `GITHUB_TOKEN=... python -m diaglab demo --repo you/your-repo`. A token with issue write access on a repo you own. Each regression is filed once, rerunning does not duplicate it.

## What was planted, and what came back

The generator plants Photos launch time +18%, Maps battery drain +25%, and Safari hang time +2% in build 27A2. Build 27A3 has 8 devices. The files also hold duplicates, malformed rows and an `email` field.

| Check | Result |
| :--- | :--- |
| Regressions flagged | Photos +20.0%, Maps +26.1%. Safari +2% is not flagged. Measured on 1,800 samples per group, so values sit near the planted ones. |
| Every row accounted for | stored plus rejected equals read, `unaccounted_rows` is 0 |
| Duplicates | 963 removed at aggregate time, exactly the planted count |
| Email in storage or API output | none, tested |
| Build 27A3 (8 devices) | hidden, under the 20 device minimum |

## Parts

| Part | File | Checked how |
| :--- | :--- | :--- |
| Spark Structured Streaming ingest, reject table with reasons | `spark_jobs.py` | row accounting test, reject reason test |
| Privacy: field allowlist, salted device token, k of 20 | `privacy.py` | no email or device id in Parquet, summary or API |
| Aggregates: exact dedupe, p50, p95, mean, std | `spark_jobs.py` | dedupe test on hand-made data |
| Regression detection: Welch test, effect floor, Bonferroni | `regress.py` | planted and control groups, improvements, tiny but significant change |
| Bug filing, once per fingerprint | `bugs.py` | rerun files nothing; GitHub sink tested against a local fake server |
| REST API with validated inputs | `api.py` | 200, 404, 422, 503 paths |
| Metrics, five alert rules, Grafana dashboard | `deploy/` | `promtool test rules`; dashboard queries checked against real metric names |
| Daily DAG: ingest, aggregate, detect and file | `dags/` | loads in Airflow, task order checked |
| Pages demo uses the same detection math in JavaScript | `docs/index.html` | test runs both and compares |

## Bug found while building it

I first used Spark's `dropDuplicatesWithinWatermark` to dedupe in the stream. A six-row test lost three rows, and the full run came up one rejected row short. Events older than the watermark were dropped without a trace, valid or not. I removed it, ingest now keeps every row, and exact dedupe happens at aggregate time. `test_late_unique_events_are_kept` pins it. Also found by `promtool test rules`: my reject-rate alert divided two series with different labels and never fired until I added `ignoring(stage)`.

## Trade-offs

All design-only; nothing here ran in production.

| Chose | Over | Costs | Flip when |
| :--- | :--- | :--- | :--- |
| Exact dedupe at aggregate time | Watermark dedupe in the stream | A shuffle over the day's data | The source is Kafka with a transactional sink, or ids sit in a keyed store with a TTL |
| Welch test on summary stats | Test on raw events | The mean is skewed for latency data; weak under a few hundred samples | Small groups: Mann-Whitney or a bootstrap |
| Bonferroni | Benjamini-Hochberg | Misses weak real changes | Hundreds of groups per build |
| 5% effect floor plus a test | Significance alone | Slow creep under 5% per build goes unseen | Add a trend check over several builds |
| Salted hash token | Raw device id | Rotating the salt breaks joins across periods | Not needed here; keep the salt out of the repo in real use |
| Fingerprint state file | Search issues by label | The file must persist between runs | The DAG runs on several workers |

## Not covered

- Scale. 45 thousand rows in about 40 seconds on one machine says nothing about billions of devices.
- Kafka, Flink, HDFS, Scala. Kafka operations are in [kafka-ops-lab](https://github.com/gerardrecinto/kafka-ops-lab). Everything here is Python and PySpark.
- Airflow was never run as a live scheduler. The DAG is loaded and order-checked in CI.
- The Grafana dashboard was never loaded into a running Grafana. Its queries are checked against the metric names the API exposes.
- The detection is a statistical test, not a model. No LLM in this repo.

MIT license.
