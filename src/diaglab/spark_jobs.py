"""Spark jobs: streaming ingest to a cleaned table, then batch aggregates.

ingest:    JSON files -> validate -> privacy scrub -> Parquet partitioned by os_build.
           Rejected rows go to their own table. Nothing is dropped silently.
aggregate: exact dedupe by event_id -> per (build, app, metric) stats ->
           suppress groups below K devices -> summary.json
"""
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from .privacy import ALLOWED_FIELDS, K_MIN_DEVICES, RANGES

# value is read as a string so a bad value becomes a counted reject, not a null.
RAW_SCHEMA = StructType([StructField(n, StringType()) for n in ALLOWED_FIELDS])


def spark_session(name="diaglab"):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    return (SparkSession.builder.master("local[2]").appName(name)
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def _range_ok():
    cond = F.lit(False)
    for metric, (lo, hi) in RANGES.items():
        cond = cond | ((F.col("metric") == metric) & F.col("value_d").between(lo, hi))
    return cond


def ingest(spark, in_dir, out_dir, salt, max_files=4):
    """Run the streaming ingest to completion (availableNow) and return counts."""
    silver = os.path.join(out_dir, "silver")
    rejects = os.path.join(out_dir, "rejects")
    ckpt = os.path.join(out_dir, "_checkpoint")

    raw = spark.readStream.schema(RAW_SCHEMA).option("maxFilesPerTrigger", max_files).json(in_dir)
    # Allowlist: select only the known columns, so anything else (email) is dropped here.
    t = (raw.select(*ALLOWED_FIELDS)
         .withColumn("value_d", F.col("value").cast(DoubleType()))
         .withColumn("ts_t", F.to_timestamp("ts")))
    # Reject reasons are evaluated in order; the first that matches wins.
    reason = (F.when(F.col("event_id").isNull() | F.col("device").isNull()
                     | F.col("os_build").isNull() | F.col("app").isNull(), "missing_field")
              .when(F.col("ts_t").isNull(), "bad_timestamp")
              .when(~F.col("metric").isin(*RANGES.keys()), "unknown_metric")
              .when(F.col("value_d").isNull() | F.isnan("value_d"), "bad_value")
              .when(~_range_ok(), "out_of_range"))
    t = t.withColumn("reject_reason", reason)

    def write_batch(df, batch_id):
        df.persist()
        good = (df.filter(F.col("reject_reason").isNull())
                .select("event_id",
                        F.substring(F.sha2(F.concat_ws("|", F.lit(salt), "device"), 256), 1, 16)
                        .alias("device_token"),
                        "os_build", "app", "metric",
                        F.col("value_d").alias("value"), F.col("ts_t").alias("ts")))
        bad = df.filter(F.col("reject_reason").isNotNull()).select("event_id", "metric", "reject_reason")
        good.write.mode("append").partitionBy("os_build").parquet(silver)
        bad.write.mode("append").parquet(rejects)
        df.unpersist()

    q = (t.writeStream.foreachBatch(write_batch)
         .option("checkpointLocation", ckpt).trigger(availableNow=True).start())
    q.awaitTermination()

    raw_rows = spark.read.schema(RAW_SCHEMA).json(in_dir).count()
    good_rows = spark.read.parquet(silver).count() if os.path.exists(silver) else 0
    bad_rows = spark.read.parquet(rejects).count() if os.path.exists(rejects) else 0
    return {"raw_rows": raw_rows, "silver_rows": good_rows, "rejected_rows": bad_rows,
            "unaccounted_rows": raw_rows - good_rows - bad_rows}


def aggregate(spark, out_dir, k=K_MIN_DEVICES):
    """Exact dedupe, per-group stats, k-anonymity. Writes summary.json and health.json."""
    silver = spark.read.parquet(os.path.join(out_dir, "silver"))
    before = silver.count()
    exact = silver.dropDuplicates(["event_id"])
    after = exact.count()
    g = (exact.groupBy("os_build", "app", "metric")
         .agg(F.count("*").alias("n"),
              F.countDistinct("device_token").alias("devices"),
              F.avg("value").alias("mean"),
              F.stddev_samp("value").alias("std"),
              F.percentile_approx("value", F.array(F.lit(0.5), F.lit(0.95)), 10000).alias("pct")))
    rows = (g.select("os_build", "app", "metric", "n", "devices", "mean", "std",
                     F.col("pct")[0].alias("p50"), F.col("pct")[1].alias("p95"))
            .orderBy("os_build", "app", "metric").collect())
    kept, suppressed = [], 0
    for r in rows:
        if r["devices"] < k:
            suppressed += 1
            continue
        kept.append({"os_build": r["os_build"], "app": r["app"], "metric": r["metric"],
                     "n": r["n"], "devices": r["devices"], "mean": round(r["mean"], 4),
                     "std": round(r["std"], 4), "p50": round(r["p50"], 4), "p95": round(r["p95"], 4)})
    with open(os.path.join(out_dir, "summary.json"), "w") as f:
        json.dump(kept, f, indent=1)
    health = {"batch_duplicates_dropped": before - after, "groups_reported": len(kept),
              "groups_suppressed_k": suppressed, "k_min_devices": k}
    return health
