"""Turn regressions into bug reports, once each.

A fingerprint of (metric, app, candidate build) is kept in a state file, so a
nightly rerun never files the same regression twice. The default sink prints
the report. GitHubSink posts it to the Issues API.
"""
import hashlib
import json
import os
import urllib.request


def fingerprint(reg):
    return hashlib.sha1(f"{reg.metric}|{reg.app}|{reg.candidate}".encode()).hexdigest()[:12]


def build_issue(reg):
    pct = round(reg.rel_change * 100, 1)
    title = f"[{reg.severity}] {reg.app} {reg.metric} up {pct}% in {reg.candidate}"
    body = (
        f"Regression detected by diaglab (fingerprint `{fingerprint(reg)}`).\n\n"
        f"- App: {reg.app}\n- Metric: {reg.metric} (higher is worse)\n"
        f"- Baseline {reg.baseline}: median {reg.p50_base} over {reg.n_base} samples\n"
        f"- Candidate {reg.candidate}: median {reg.p50_cand} over {reg.n_cand} samples\n"
        f"- Change: +{pct}% (Welch test p = {reg.p_value:.2e})\n\n"
        "Aggregates only. No device identifiers are included."
    )
    return {"title": title, "body": body, "labels": ["regression", reg.severity, reg.app.lower()]}


class PrintSink:
    def __init__(self):
        self.sent = []

    def create(self, issue):
        self.sent.append(issue)
        print(issue["title"])


class GitHubSink:
    def __init__(self, repo, token, api="https://api.github.com"):
        self.url = f"{api}/repos/{repo}/issues"
        self.token = token

    def create(self, issue):
        req = urllib.request.Request(
            self.url, data=json.dumps(issue).encode(), method="POST",
            headers={"Authorization": f"Bearer {self.token}", "Accept": "application/vnd.github+json",
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.load(r).get("html_url")


def file_bugs(regressions, state_path, sink):
    """Create one issue per new regression. Returns (filed, skipped)."""
    seen = set()
    if os.path.exists(state_path):
        with open(state_path) as f:
            seen = set(json.load(f))
    filed = skipped = 0
    for reg in regressions:
        fp = fingerprint(reg)
        if fp in seen:
            skipped += 1
            continue
        sink.create(build_issue(reg))
        seen.add(fp)
        filed += 1
        with open(state_path, "w") as f:  # persist after each success, so a crash does not refile
            json.dump(sorted(seen), f)
    return filed, skipped
