"""Protected two-network relay: one fixed upstream route, no forward proxy."""

import http.client
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


class Relay(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def log_message(self, format: str, *args: object) -> None:
        pass  # Credentials and request bodies never enter Docker logs.

    def do_POST(self) -> None:
        if self.path != "/internal/repair-model/responses" or self.headers.get(
            "Transfer-Encoding"
        ):
            self.send_error(403)
            return
        try:
            length = int(self.headers.get("Content-Length", "-1"))
            if not 0 < length <= 8 * 1024 * 1024:
                self.send_error(413)
                return
            upstream = urlsplit(os.environ["REPAIR_GATEWAY_UPSTREAM"])
            if (
                not upstream.hostname
                or upstream.scheme != "http"
                or upstream.path != self.path
                or upstream.username
                or upstream.password
                or upstream.query
                or upstream.fragment
            ):
                raise ValueError("Invalid protected upstream")
            connection = http.client.HTTPConnection(
                upstream.hostname, upstream.port or 80, timeout=600
            )
            try:
                self.connection.settimeout(30)
                body = self.rfile.read(length)
                if len(body) != length:
                    raise ValueError("Incomplete request")
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
                self.send_header(
                    "Content-Type",
                    response.getheader("Content-Type", "application/x-ndjson"),
                )
                self.end_headers()
                while chunk := response.read1(8192):
                    self.wfile.write(chunk)
                    self.wfile.flush()
            finally:
                connection.close()
        except (ValueError, OSError, http.client.HTTPException):
            self.close_connection = True

    def do_CONNECT(self) -> None:
        self.send_error(403)

    def do_GET(self) -> None:
        self.send_error(403)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Relay).serve_forever()
