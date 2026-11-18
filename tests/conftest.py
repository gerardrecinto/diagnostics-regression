import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture(scope="session")
def built(tmp_path_factory):
    """Run generate, ingest, aggregate once and share the result."""
    from diaglab import pipeline
    d = str(tmp_path_factory.mktemp("data"))
    planted = pipeline.step_generate(d)
    counts = pipeline.step_ingest(d)
    health = pipeline.step_aggregate(d)
    return {"dir": d, "planted": planted, "counts": counts, "health": health}
