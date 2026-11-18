"""Privacy rules shared by the ingest and aggregate jobs.

Only allowlisted fields survive ingest. The device id is replaced by a salted
hash. Groups with fewer than K distinct devices are never reported.
"""

ALLOWED_FIELDS = ("event_id", "device", "os_build", "app", "metric", "value", "ts")

# metric -> (min, max) accepted value. Anything outside is rejected as invalid.
RANGES = {
    "launch_ms": (1.0, 60_000.0),
    "battery_pct_per_hr": (0.0, 100.0),
    "hang_ms": (0.0, 120_000.0),
}

# A (build, app, metric) group is reported only when at least this many
# distinct devices contributed to it.
K_MIN_DEVICES = 20
