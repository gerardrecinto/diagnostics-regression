"""Regression detection on per-group summary stats (n, mean, std, p50).

A group is flagged when the candidate build is worse (all metrics here are
higher-is-worse) by at least min_effect on the median, and a Welch test on the
mean passes a Bonferroni-corrected alpha. Needs only the aggregates, so it
scales with the number of groups, not the number of events.
"""
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class Regression:
    app: str
    metric: str
    baseline: str
    candidate: str
    p50_base: float
    p50_cand: float
    rel_change: float
    p_value: float
    n_base: int
    n_cand: int

    @property
    def severity(self):
        return "high" if self.rel_change >= 0.20 else "medium"


def _p_two_sided(z):
    return math.erfc(abs(z) / math.sqrt(2.0))


def detect(summary, baseline, candidate, min_effect=0.05, alpha=0.01):
    base = {(r["app"], r["metric"]): r for r in summary if r["os_build"] == baseline}
    cand = {(r["app"], r["metric"]): r for r in summary if r["os_build"] == candidate}
    keys = sorted(set(base) & set(cand))
    if not keys:
        return []
    corrected = alpha / len(keys)
    out = []
    for key in keys:
        b, c = base[key], cand[key]
        if b["p50"] <= 0 or b["n"] < 2 or c["n"] < 2:
            continue
        se = math.sqrt(b["std"] ** 2 / b["n"] + c["std"] ** 2 / c["n"])
        if se == 0:
            continue
        z = (c["mean"] - b["mean"]) / se
        p = _p_two_sided(z)
        rel = (c["p50"] - b["p50"]) / b["p50"]
        if rel >= min_effect and z > 0 and p < corrected:
            out.append(Regression(key[0], key[1], baseline, candidate,
                                  b["p50"], c["p50"], round(rel, 4), p, b["n"], c["n"]))
    return sorted(out, key=lambda r: -r.rel_change)
