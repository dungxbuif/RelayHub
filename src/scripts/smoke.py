"""Smoke a built RelayHub binary using a temporary loopback listener."""
import json
import os
import re
import select
import subprocess
import sys
import time
import urllib.error
import urllib.request

binary = sys.argv[1]
env = dict(os.environ, RELAYHUB_ADDR="127.0.0.1:0")
process = subprocess.Popen([binary], env=env, stderr=subprocess.PIPE, text=True)
try:
    deadline = time.monotonic() + 10
    address = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError("server exited before listening")
        ready, _, _ = select.select([process.stderr], [], [], 0.2)
        if ready:
            line = process.stderr.readline()
            match = re.search(r"address=(127\.0\.0\.1:\d+)", line)
            if match:
                address = match.group(1)
                break
    assert address, "startup timed out"
    for path, expected in [("/healthz", 200), ("/readyz", 503),
                           ("/api/v1/jobs", 501), ("/api/v1/admin/projects", 404)]:
        try:
            response = urllib.request.urlopen("http://" + address + path, timeout=3)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = json.load(response)
            assert response.status == expected, (path, response.status)
            assert response.headers["X-Request-ID"], path
            print(path, expected, body)
    process.terminate()
    assert process.wait(timeout=12) == 0, "unclean shutdown"
    print("PASS: HTTP smoke and graceful SIGTERM")
finally:
    if process.poll() is None:
        process.kill()
        process.wait()
