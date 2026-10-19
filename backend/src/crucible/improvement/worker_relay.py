"""Trusted fixed-origin, fixed-route relay; never an HTTP forward proxy."""

import http.client
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

ROUTES = frozenset({"/internal/improvement/model", "/internal/improvement/command"})


class Relay(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, format: str, *args: object) -> None:
        pass

    def do_POST(self) -> None:
        if self.path not in ROUTES or self.headers.get("Transfer-Encoding"):
            self.send_error(403)
            return
        connection = None
        try:
            length = int(self.headers.get("Content-Length", "-1"))
            if not 0 < length <= 8 * 1024 * 1024:
                self.send_error(413)
                return
            origin = urlsplit(os.environ["IMPROVEMENT_BROKER_ORIGIN"])
            if (
                origin.scheme != "http"
                or origin.hostname != "host.docker.internal"
                or not origin.port
                or origin.path
                or origin.query
                or origin.fragment
                or origin.username
            ):
                raise ValueError("invalid fixed broker origin")
            self.connection.settimeout(30)
            body = self.rfile.read(length)
            if len(body) != length:
                raise ValueError("incomplete body")
            connection = http.client.HTTPConnection(
                origin.hostname, origin.port, timeout=600
            )
            connection.request(
                "POST",
                self.path,
                body=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": self.headers.get("Authorization", ""),
                },
            )
            response = connection.getresponse()
            self.send_response(response.status)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            total = 0
            while chunk := response.read1(8192):
                total += len(chunk)
                if total > 32 * 1024 * 1024:
                    raise ValueError("relay response cap")
                self.wfile.write(chunk)
                self.wfile.flush()
        except (OSError, ValueError, http.client.HTTPException):
            self.close_connection = True
        finally:
            if connection:
                connection.close()

    def do_GET(self) -> None:
        self.send_error(403)

    def do_CONNECT(self) -> None:
        self.send_error(403)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Relay).serve_forever()
