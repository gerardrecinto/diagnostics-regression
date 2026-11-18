import glob
import json
import os
import pathlib

from diaglab import bugs, gen, pipeline, regress, spark_jobs
from diaglab.privacy import K_MIN_DEVICES


def _summary(built):
    with open(os.path.join(built["dir"], "out", "summary.json")) as f:
        return json.load(f)


def test_generator_is_deterministic(tmp_path):
    a = gen.generate(str(tmp_path / "a"), seed=3, devices_per_build={"B": 5})
    b = gen.generate(str(tmp_path / "b"), seed=3, devices_per_build={"B": 5})
    assert a == b
    fa = sorted(glob.glob(str(tmp_path / "a" / "*.jsonl")))
    fb = sorted(glob.glob(str(tmp_path / "b" / "*.jsonl")))
    assert [pathlib.Path(x).read_text() for x in fa] == [pathlib.Path(x).read_text() for x in fb]


def test_row_accounting_adds_up(built):
    c, p = built["counts"], built["planted"]
    assert c["raw_rows"] == p["rows"]
    assert c["rejected_rows"] == p["malformed"]
    assert c["unaccounted_rows"] == 0
    assert c["silver_rows"] == p["unique"] + p["duplicates"] + p["late_duplicates"]
    assert built["health"]["batch_duplicates_dropped"] == p["duplicates"] + p["late_duplicates"]


def test_rejects_carry_reasons(built):
    sp = spark_jobs.spark_session()
    got = {r["reject_reason"] for r in
           sp.read.parquet(os.path.join(built["dir"], "out", "rejects")).collect()}
    assert got == {"bad_value", "out_of_range", "unknown_metric"}


def test_no_pii_reaches_storage(built):
    sp = spark_jobs.spark_session()
    df = sp.read.parquet(os.path.join(built["dir"], "out", "silver"))
    assert "email" not in df.columns and "device" not in df.columns
    token = df.first()["device_token"]
    assert len(token) == 16 and "dev" not in token
    with open(os.path.join(built["dir"], "out", "summary.json")) as f:
        blob = f.read()
    assert "@" not in blob and "device_token" not in blob


def test_small_cohort_is_suppressed(built):
    summary = _summary(built)
    assert "27A3" not in {r["os_build"] for r in summary}      # 8 devices < K
    assert all(r["devices"] >= K_MIN_DEVICES for r in summary)
    assert built["health"]["groups_suppressed_k"] == 12          # 4 apps x 3 metrics


def test_batch_dedupe_is_exact(tmp_path):
    sp = spark_jobs.spark_session()
    out = str(tmp_path)
    rows = [(f"e{i}", f"t{i}", "B", "A", "launch_ms", 100.0 + i) for i in range(30)]
    rows += [rows[0], rows[1]]                                    # two exact duplicates
    sp.createDataFrame(rows, "event_id string, device_token string, os_build string, "
                             "app string, metric string, value double") \
        .write.partitionBy("os_build").parquet(os.path.join(out, "silver"))
    assert spark_jobs.aggregate(sp, out)["batch_duplicates_dropped"] == 2


def test_detects_planted_and_nothing_else(built):
    regs = pipeline.step_detect(built["dir"])
    by = {(r.app, r.metric): r for r in regs}
    assert set(by) == {("Photos", "launch_ms"), ("Maps", "battery_pct_per_hr")}
    assert 0.15 < by[("Photos", "launch_ms")].rel_change < 0.25
    assert 0.20 < by[("Maps", "battery_pct_per_hr")].rel_change < 0.30


def test_same_build_is_clean(built):
    assert regress.detect(_summary(built), "27A1", "27A1") == []


def _row(build, p50, n=100_000):
    return {"os_build": build, "app": "A", "metric": "launch_ms", "n": n,
            "devices": 1000, "mean": p50, "std": 10.0, "p50": p50, "p95": p50 * 2}


def test_effect_floor_blocks_tiny_but_significant_change():
    summary = [_row("old", 100.0), _row("new", 102.0)]      # +2%, hugely significant at n=100k
    assert regress.detect(summary, "old", "new", min_effect=0.05) == []
    assert len(regress.detect(summary, "old", "new", min_effect=0.01)) == 1


def test_improvement_is_not_a_regression():
    assert regress.detect([_row("old", 100.0), _row("new", 80.0)], "old", "new") == []


def test_bugs_filed_once(built, tmp_path):
    regs = pipeline.step_detect(built["dir"])
    state = str(tmp_path / "filed.json")
    sink = bugs.PrintSink()
    assert bugs.file_bugs(regs, state, sink) == (2, 0)
    assert bugs.file_bugs(regs, state, sink) == (0, 2)
    assert len(sink.sent) == 2
    for issue in sink.sent:
        assert "@" not in issue["body"] and "-dev-" not in issue["body"]


def test_late_unique_events_are_kept(tmp_path):
    """Regression test: streaming dropDuplicatesWithinWatermark silently dropped
    events older than the watermark, valid or not. Ingest must account for every row."""
    raw = tmp_path / "raw"
    raw.mkdir()

    def ev(i, ts, metric="launch_ms"):
        return json.dumps({"event_id": i, "device": "d", "os_build": "b", "app": "a",
                           "metric": metric, "value": "100", "ts": ts})
    (raw / "a.jsonl").write_text(ev("e1", "2026-11-16T12:00:00Z") + "\n")
    (raw / "b.jsonl").write_text(ev("e2", "2026-11-16T12:30:00Z") + "\n")
    (raw / "c.jsonl").write_text(ev("e3", "2026-11-16T01:00:00Z") + "\n"
                                 + ev("e4", "2026-11-16T01:05:00Z", "nope") + "\n")
    (raw / "d.jsonl").write_text(ev("e5", "2026-11-16T01:10:00Z") + "\n"
                                 + ev("e6", "2026-11-16T13:00:00Z") + "\n")
    sp = spark_jobs.spark_session()
    out = str(tmp_path / "out")
    counts = spark_jobs.ingest(sp, str(raw), out, "x", max_files=1)
    assert counts == {"raw_rows": 6, "silver_rows": 5, "rejected_rows": 1, "unaccounted_rows": 0}
