"""The Pages demo re-implements detect() in JavaScript. Keep it honest."""
import json
import os
import shutil
import subprocess

import pytest

from diaglab import regress

ROOT = os.path.join(os.path.dirname(__file__), "..")


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_js_detect_matches_python():
    page = os.path.join(ROOT, "docs", "index.html")
    js = json.loads(subprocess.run(["node", os.path.join(ROOT, "tests", "page_parity.js"), page],
                                   check=True, capture_output=True, text=True).stdout)
    with open(page) as f:
        html = f.read()
    data = json.loads(html.split("const DATA=")[1].split(";\nconst BASE")[0])
    for key, flagged in js.items():
        eff, alp = map(float, key.split("|"))
        py = sorted(f"{r.app}|{r.metric}" for r in regress.detect(data, "27A1", "27A2", eff, alp))
        assert flagged == py, key
