#!/usr/bin/env python3
"""What changed in the owner's other agents' memory since OPPA last took it in.

Usage: memory_changes.py --state FILE --source GLOB [--source GLOB ...] [--mark DIGEST]

The owner uses Claude Code and Codex every day, and each keeps a memory of its
own; OPPA's memory is the source of truth. The
memory sync's turn runs this and gets, as data, each source file that is new or
changed since the last mark, with its text while the answer stays small, and
the files removed since. After the turn has put what matters into OPPA memory
it runs the printed mark command, which records what it was offered; a turn
that never marks sees the same changes again next time, which is safe. A turn
is offered at most BATCH files, so a first sync of many takes several turns.
"""

import argparse
import glob
import hashlib
import json
import os
import sys
import time

HOME = os.path.expanduser("~")
# The turn reads its event body up to 16,000 characters; leave room for the rest.
TEXT_BUDGET = 6000
# At most this many changed files a turn, newest first, so the list fits too;
# the rest come in the next turns.
BATCH = 25
MAX_FILE = 256 * 1024


def scan(patterns):
    files = {}
    for pattern in patterns:
        for path in glob.glob(os.path.expanduser(pattern), recursive=True):
            if os.path.isfile(path) and not os.path.islink(path) and os.path.getsize(path) <= MAX_FILE:
                with open(path, "rb") as handle:
                    files[path] = hashlib.sha256(handle.read()).hexdigest()
    return files


def digest(offered, removed):
    return hashlib.sha256(json.dumps([offered, removed]).encode()).hexdigest()[:16]


def plan(files, seen):
    """The newest changes, at most BATCH of them, and every removal: what one turn takes in."""
    changed = sorted((path for path in files if seen.get(path) != files[path]), key=lambda p: -os.path.getmtime(p))
    offered = [[path, files[path]] for path in changed[:BATCH]]
    removed = sorted(path for path in seen if path not in files)
    return offered, removed, len(changed) - len(offered)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", required=True, help="where the last mark is kept")
    parser.add_argument("--source", action="append", required=True, help="a file or glob to take in")
    parser.add_argument("--mark", help="record what a turn was offered, once its changes are in OPPA memory")
    options = parser.parse_args()
    files = scan(options.source)
    try:
        with open(options.state) as handle:
            seen = json.load(handle)["files"]
    except (OSError, ValueError, KeyError):
        seen = {}
    offered, removed, remaining = plan(files, seen)
    current = digest(offered, removed)
    if options.mark is not None:
        if options.mark != current:
            sys.exit("the sources changed since that digest was printed; run the sync again")
        seen.update(dict(offered))
        for path in removed:
            seen.pop(path, None)
        temporary = options.state + ".tmp"
        with open(temporary, "w") as handle:
            # When and how much is left, so the host knows an import has ended.
            json.dump({"files": seen, "marked_at_ms": int(time.time() * 1000), "remaining": remaining}, handle)
        os.replace(temporary, options.state)
        print("marked %d changed and %d removed; %d more to take in" % (len(offered), len(removed), remaining))
        return
    changed, budget = [], TEXT_BUDGET
    for path, _ in offered:
        entry = {"path": path.replace(HOME, "~", 1), "status": "changed" if path in seen else "new",
                 "bytes": os.path.getsize(path)}
        with open(path, encoding="utf-8", errors="replace") as handle:
            text = handle.read()
        if len(text) <= budget:
            entry["text"], budget = text, budget - len(text)
        changed.append(entry)
    mark = [sys.executable, os.path.abspath(__file__), "--state", options.state]
    for source in options.source:
        mark += ["--source", source]
    print(json.dumps({"first_sync": not seen, "changed": changed,
                      "removed": [path.replace(HOME, "~", 1) for path in removed],
                      "unchanged": len(files) - len(offered) - remaining, "not_yet_offered": remaining,
                      "mark": " ".join(json.dumps(part) if " " in part else part for part in mark + ["--mark", current])},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
