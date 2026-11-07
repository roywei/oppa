#!/usr/bin/env python3
"""Run the bundled Mac connector, compiling apple.swift once per source version.

The compiled helper lives next to this file, named by the source digest, so a
changed source compiles once and an unchanged one never recompiles. The request
passes through unchanged; the helper speaks connector protocol v1. If the helper
cannot be built, the answer still has the shape the request's method expects.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
FIX = "Install the Xcode command line tools (xcode-select --install), then try again"


def fail(method, problem):
    if method == "health":
        print(json.dumps({"ready": False, "problem": problem, "fix": FIX}))
    else:
        print(json.dumps({"status": "error", "error": problem, "fix": FIX}))
    sys.exit(0)


def helper(method):
    with open(os.path.join(HERE, "apple.swift"), "rb") as handle:
        source = handle.read()
    digest = hashlib.sha256(source).hexdigest()[:16]
    folder = os.path.join(HERE, "bin")
    binary = os.path.join(folder, "apple-" + digest)
    if os.path.isfile(binary):
        return binary
    os.makedirs(folder, mode=0o700, exist_ok=True)
    # A compile stopped at the connector deadline leaves its work folder behind.
    for name in os.listdir(folder):
        path = os.path.join(folder, name)
        if name.startswith("tmp") and os.path.isdir(path) and time.time() - os.path.getmtime(path) > 600:
            shutil.rmtree(path, ignore_errors=True)
    # Compile exactly the bytes that were hashed, even if a reinstall races.
    with tempfile.TemporaryDirectory(dir=folder) as work:
        copy = os.path.join(work, "apple.swift")
        with open(copy, "wb") as handle:
            handle.write(source)
        built = os.path.join(work, "apple")
        try:
            compiled = subprocess.run(["/usr/bin/xcrun", "swiftc", "-O", copy, "-o", built],
                                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=300)
        except (OSError, subprocess.TimeoutExpired):
            fail(method, "the Mac connector could not be compiled")
        if compiled.returncode != 0:
            fail(method, "the Mac connector could not be compiled")
        os.replace(built, binary)
    for name in os.listdir(folder):
        if name.startswith("apple-") and name != os.path.basename(binary):
            try:
                os.remove(os.path.join(folder, name))
            except OSError:
                pass
    return binary


if __name__ == "__main__":
    request = sys.stdin.buffer.read(1024 * 1024)
    try:
        method = json.loads(request).get("method")
    except (ValueError, AttributeError):
        method = None
    answer = subprocess.run([helper(method)], input=request, stdout=subprocess.PIPE)
    sys.stdout.buffer.write(answer.stdout)
    sys.exit(answer.returncode)
