"""Synthetic diagnostics generator. Deterministic for a given seed.

Build 27A2 carries two planted regressions (Photos launch time, Maps battery
drain) and one change too small to flag (Safari hang time). Build 27A3 has too
few devices to report. The files also contain duplicates, malformed rows and a
field that must never reach storage.
"""
import json
import math
import os
import random
from datetime import datetime, timedelta, timezone

APPS = ("Mail", "Maps", "Photos", "Safari")
BASE = {
    "launch_ms": {"Mail": 420.0, "Maps": 650.0, "Photos": 800.0, "Safari": 380.0},
    "battery_pct_per_hr": {"Mail": 1.1, "Maps": 3.4, "Photos": 1.8, "Safari": 1.5},
    "hang_ms": {"Mail": 90.0, "Maps": 140.0, "Photos": 120.0, "Safari": 100.0},
}
SIGMA = {"launch_ms": 0.35, "battery_pct_per_hr": 0.30, "hang_ms": 0.50}

# (build, app, metric) -> multiplier on the median
PLANTED = {
    ("27A2", "Photos", "launch_ms"): 1.18,
    ("27A2", "Maps", "battery_pct_per_hr"): 1.25,
    ("27A2", "Safari", "hang_ms"): 1.02,
}
DAY = datetime(2026, 11, 16, tzinfo=timezone.utc)


def _value(rng, build, app, metric):
    median = BASE[metric][app] * PLANTED.get((build, app, metric), 1.0)
    return round(median * math.exp(rng.gauss(0.0, SIGMA[metric])), 3)


def generate(out_dir, seed=7, devices_per_build=None, per_combo=3,
             dup_rate=0.02, late_dup_rate=0.004, bad_rate=0.01):
    """Write hourly JSONL files into out_dir. Returns a dict of what was planted."""
    rng = random.Random(seed)
    devices_per_build = devices_per_build or {"27A1": 600, "27A2": 600, "27A3": 8}
    os.makedirs(out_dir, exist_ok=True)
    hours = [[] for _ in range(24)]
    n_unique = 0
    for build, n_dev in devices_per_build.items():
        for d in range(n_dev):
            device = f"{build}-dev-{d:04d}"
            for app in APPS:
                for metric in BASE:
                    for _ in range(per_combo):
                        ts = DAY + timedelta(seconds=rng.randrange(86_400))
                        ev = {
                            "event_id": f"e{n_unique:07d}",
                            "device": device,
                            "os_build": build,
                            "app": app,
                            "metric": metric,
                            "value": _value(rng, build, app, metric),
                            "ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "email": f"user{d}@example.com",  # must never be stored
                        }
                        n_unique += 1
                        hours[ts.hour].append(ev)
    n_dup = n_late = n_bad = 0
    for h in range(24):
        for ev in list(hours[h]):
            if rng.random() < dup_rate:
                target = h if rng.random() < 0.5 else min(h + 1, 23)
                hours[target].append(dict(ev))
                n_dup += 1
            if h < 12 and rng.random() < late_dup_rate:
                # a device retry hours later
                hours[h + rng.randint(6, 11)].append(dict(ev))
                n_late += 1
            if rng.random() < bad_rate:
                bad = dict(ev)
                bad["event_id"] = f"bad{n_bad:06d}"
                kind = n_bad % 3
                if kind == 0:
                    bad["value"] = "NaN-ish"
                elif kind == 1:
                    bad["value"] = -5.0
                else:
                    bad["metric"] = "unknown_metric"
                hours[h].append(bad)
                n_bad += 1
    for h, rows in enumerate(hours):
        rng.shuffle(rows)
        with open(os.path.join(out_dir, f"events-{h:02d}.jsonl"), "w") as f:
            f.writelines(json.dumps(r) + "\n" for r in rows)
    return {"unique": n_unique, "duplicates": n_dup, "late_duplicates": n_late,
            "malformed": n_bad, "rows": n_unique + n_dup + n_late + n_bad}
