#!/usr/bin/env python3
"""Public-tree size/privacy check. Heuristics supplement review, not prove secrecy."""
import pathlib
import re
import subprocess
import sys

root = pathlib.Path(__file__).resolve().parents[1]
paths = subprocess.check_output(
    ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=root
).decode().split("\0")
patterns = [
    ("home directory", re.compile(r"/(?:Users|home)/[A-Za-z][A-Za-z0-9_.-]+/")),
    ("private IPv4", re.compile(r"\b(?:192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+)\b")),
    ("secret key", re.compile(r"(?:sk-[A-Za-z0-9_-]{24,}|-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----)")),
    ("email", re.compile(r"\b[A-Za-z0-9._%+-]+@(?!(?:example\.(?:com|org|test)|localhost)\b)[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
]
failures = []
lines = 0
for name in sorted(set(filter(None, paths))):
    path = root / name
    if not path.is_file():
        continue
    if path.is_symlink():
        failures.append(f"{name}: symlink requires explicit review")
        continue
    if path.suffix in {".db", ".sqlite", ".pem", ".key"} or name.startswith(("private/", "runtime/")):
        failures.append(f"{name}: private artifact")
    try:
        text = path.read_text()
    except UnicodeDecodeError:
        failures.append(f"{name}: binary artifact requires explicit review")
        continue
    if name.startswith("src/") and path.suffix == ".rs":
        lines += len(text.splitlines())
    if name == "scripts/audit.py":
        continue
    # On macOS a spawn's pipes leak into any process another thread starts at
    # that moment; src/process.rs serializes spawns, so core starts none itself.
    if name.startswith("src/") and not name.startswith(("src/process.rs", "src/bin/")):
        for number, line in enumerate(text.splitlines(), 1):
            started = re.search(r"\.(?:spawn|output)\(\)|(?<!self)(?<!service)\.status\(\)", line)
            if started and "process::spawning(||" not in line:
                failures.append(f"{name}:{number}: starts a process outside src/process.rs")
    for label, pattern in patterns:
        if pattern.search(text):
            failures.append(f"{name}: potential {label} (value withheld)")
# Nested modules (src/broker/*.rs) count like top-level files.
sources = sorted((root / "src").rglob("*.rs"))
# FTS tables and their internal shadow tables are excluded from the ordinary budget.
schema = "\n".join(p.read_text() for p in sources)
tables = set(re.findall(r"CREATE TABLE(?: IF NOT EXISTS)?\s+([a-z_]+)", schema, re.I))
# Size is tracked, never capped: the audit reports it
# and names the largest code to question; only privacy findings fail it. The
# dashboard is front end, counted apart from core and edges.
FRONT_END = ("resources/adapters/web.",)
listed = [n for n in set(filter(None, paths)) if (root / n).is_file()]
front = sum(len((root / n).read_text().splitlines()) for n in listed if n.startswith(FRONT_END))
edge = sum(len((root / n).read_text().splitlines()) for n in listed
           if n.startswith("resources/") and not n.startswith(FRONT_END))
tests = sum(len((root / n).read_text().splitlines()) for n in listed if n.startswith("tests/"))
largest = sorted(((len(p.read_text().splitlines()), p.relative_to(root / "src").as_posix()) for p in sources),
                 reverse=True)[:5]
print(f"Core: {lines} Rust lines; edges: {edge} lines; front end: {front} lines (counted apart); "
      f"tests: {tests} lines; ordinary schema: {len(tables)} tables.")
print("Largest core files, to question what can be removed or simplified: "
      + ", ".join(f"{name} ({count})" for count, name in largest) + ".")
for failure in failures:
    print(failure)
sys.exit(bool(failures))
