import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from diaglab import bugs
from diaglab.regress import Regression


def test_github_sink_posts_issue():
    seen = {}

    class H(BaseHTTPRequestHandler):
        def do_POST(self):
            seen["path"] = self.path
            seen["auth"] = self.headers["Authorization"]
            seen["body"] = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            self.send_response(201)
            self.end_headers()
            self.wfile.write(b'{"html_url": "http://example.test/1"}')

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    reg = Regression("Photos", "launch_ms", "27A1", "27A2", 800.0, 944.0, 0.18, 1e-9, 1800, 1800)
    sink = bugs.GitHubSink("o/r", "tok", api=f"http://127.0.0.1:{srv.server_port}")
    url = sink.create(bugs.build_issue(reg))
    srv.shutdown()
    assert url == "http://example.test/1"
    assert seen["path"] == "/repos/o/r/issues"
    assert seen["auth"] == "Bearer tok"
    assert seen["body"]["labels"] == ["regression", "medium", "photos"]
    assert "up 18.0%" in seen["body"]["title"]
