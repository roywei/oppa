"""Small, bounded native-CLI bridge. No credentials or native logs are emitted.

These adapters run as the signed-in operator, not as an isolated worker. Native
CLIs may persist their own private sessions. Rust owns the deadline/process group;
children must inherit it and must never detach. This module starts no models by
itself, and never interprets the user's prompt.
"""

import json
import re
import os
import selectors
import subprocess
import sys
import uuid

MAX_REQUEST = 2 * 1024 * 1024
# A Read returns its file in one event: an image as Claude resized it (a 12 MB
# noise image came back as a 1.3 MB line, CLI 2.1.288), a PDF whole, as base64,
# once in the message and again in its tool_use_result. A line holds the largest
# file a turn may open, 20 MiB, twice as base64 (53.3 MiB), and a run's output
# each of the eight it may open, read once.
MAX_NATIVE_LINE = 56 * 1024 * 1024
MAX_NATIVE_OUTPUT = 8 * MAX_NATIVE_LINE
MAX_STDERR = 64 * 1024
OPPA_TOOLS = ("history.search", "history.read", "history.conversations", "history.turn", "memory.search", "memory.read",
                 "memory.commit", "memory.forget", "memory.import", "tasks.delegate", "tasks.list", "tasks.read", "tasks.stop",
                 "schedules.create", "schedules.list", "schedules.cancel", "wakes.set", "wakes.list", "wakes.cancel",
                 "messages.send", "messages.ask_owner", "people.list", "people.propose", "people.answer", "people.group")
OPPA_EFFECTS = frozenset(("memory.commit", "memory.forget", "memory.import", "tasks.delegate", "tasks.stop", "schedules.create", "schedules.cancel",
                          "wakes.set", "wakes.cancel", "messages.send", "messages.ask_owner", "people.propose", "people.answer",
                          "people.group"))


class AdapterError(Exception):
    def __init__(self, category, diagnostic):
        self.category = category
        self.diagnostic = diagnostic


def emit(event_type, **fields):
    line = json.dumps(dict(type=event_type, **fields), ensure_ascii=False)
    if len(line.encode("utf-8")) >= 256 * 1024:
        raise AdapterError("protocol", "normalized event exceeds size limit")
    print(line, flush=True)


def read_request():
    data = sys.stdin.buffer.read(MAX_REQUEST + 1)
    if len(data) > MAX_REQUEST:
        raise AdapterError("protocol", "request exceeds size limit")
    try:
        value = json.loads(data)
        required = {"protocol_version", "conversation", "prompt", "session"}
        optional = {"mcp", "working_directory", "instructions", "access", "readable_dir", "model",
                    "effort", "browser"}
        if not isinstance(value, dict) or not required <= set(value) or set(value) - required - optional:
            raise ValueError()
        if type(value["protocol_version"]) is not int or value["protocol_version"] != 1:
            raise ValueError()
        if not isinstance(value["conversation"], str) or not value["conversation"]:
            raise ValueError()
        if not isinstance(value["prompt"], str):
            raise ValueError()
        instructions = value.setdefault("instructions", None)
        if instructions is not None and (not isinstance(instructions, str) or "\0" in instructions
                                         or len(instructions.encode("utf-8")) > 64 * 1024):
            raise ValueError()
        if value["session"] is not None:
            value["session"] = str(uuid.UUID(value["session"]))
        directory = value.setdefault("working_directory", None)
        if directory is not None:
            if not isinstance(directory, str) or "\0" in directory or not os.path.isabs(directory) or not os.path.isdir(directory):
                raise ValueError()
        mcp = value.setdefault("mcp", None)
        if mcp is not None:
            if not isinstance(mcp, dict) or not {"command", "args"} <= set(mcp) <= {
                    "command", "args", "read_tools", "write_tools"}:
                raise ValueError()
            reads = mcp.setdefault("read_tools", [])
            writes = mcp.setdefault("write_tools", [])
            tools = reads + writes if isinstance(reads, list) and isinstance(writes, list) else None
            if tools is None or len(tools) > 256 or len(set(map(str, tools))) != len(tools):
                raise ValueError()
            if any(not isinstance(t, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,64}\.[a-z0-9_]{1,64}", t)
                   or t in OPPA_TOOLS for t in tools):
                raise ValueError()
            if not isinstance(mcp["command"], str) or not mcp["command"].strip() or "\0" in mcp["command"]:
                raise ValueError()
            if not isinstance(mcp["args"], list) or len(mcp["args"]) > 128:
                raise ValueError()
            if any(not isinstance(arg, str) or "\0" in arg for arg in mcp["args"]):
                raise ValueError()
        readable = value.setdefault("readable_dir", None)
        if readable is not None and (not isinstance(readable, str) or not readable.startswith("/") or "\0" in readable):
            raise ValueError()
        if value.setdefault("access", "read") not in ("read", "connectors", "computer"):
            raise ValueError()
        if value["access"] != "read" and mcp is None:
            raise ValueError()  # Full access needs the broker's permission tool.
        # A read run may carry connector writes: the ones the broker offers an
        # adult (its may), each journaled by the broker like the owner's.
        model = value.setdefault("model", None)
        if model is not None and (not isinstance(model, str) or not re.fullmatch(r"[A-Za-z0-9._:\[\]/-]{1,128}", model)):
            raise ValueError()
        effort = value.setdefault("effort", None)
        if effort is not None and effort not in ("low", "medium", "high", "xhigh", "max"):
            raise ValueError()
        if value.setdefault("browser", False) not in (True, False):
            raise ValueError()
        return value
    except (ValueError, TypeError, AttributeError):
        raise AdapterError("protocol", "invalid version 1 request") from None


def native_environment(excluded):
    # Retain normal OS login/keychain access; do not copy or read auth stores.
    return {key: value for key, value in os.environ.items() if key not in excluded}


def oppa_activity(request, server, tool):
    """Classify an observed broker call, never a prompt or an inferred action."""
    mcp = request.get("mcp")
    if mcp is not None and server == "oppa" and tool in OPPA_TOOLS:
        return "effect" if tool in OPPA_EFFECTS else "read"
    if mcp is not None and server == "oppa" and tool in mcp.get("read_tools", []):
        return "read"
    if mcp is not None and server == "oppa" and tool in mcp.get("write_tools", []):
        return "effect"
    return "unmediated"


def run_native(command, prompt, on_event, environment):
    """Drain all pipes concurrently with bounded buffers; no shell expansion."""
    try:
        process = subprocess.Popen(command, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   env=environment, bufsize=0)
    except OSError:
        raise AdapterError("unavailable", "native CLI could not be started") from None
    selector = selectors.DefaultSelector()
    pending = bytearray()
    errors = bytearray()
    total = 0
    data = prompt.encode("utf-8")
    written = 0
    try:
        for stream, name in [(process.stdout, "stdout"), (process.stderr, "stderr")]:
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        if data:
            os.set_blocking(process.stdin.fileno(), False)
            selector.register(process.stdin, selectors.EVENT_WRITE, "stdin")
        else:
            process.stdin.close()
        while selector.get_map():
            for key, _ in selector.select():
                if key.data == "stdin":
                    try:
                        written += os.write(key.fd, data[written:written + 65536])
                    except BlockingIOError:
                        continue
                    except BrokenPipeError:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    if written == len(data):
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                    continue
                try:
                    chunk = os.read(key.fd, 65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                if key.data == "stderr":
                    errors.extend(chunk)
                    if len(errors) > MAX_STDERR:
                        raise AdapterError("protocol", "native diagnostic limit exceeded")
                    continue
                total += len(chunk)
                if total > MAX_NATIVE_OUTPUT:
                    raise AdapterError("protocol", "native output limit exceeded")
                pending.extend(chunk)
                # Only a new chunk can end a line, so a long line is searched once.
                while b"\n" in chunk and b"\n" in pending:
                    line, _, pending = pending.partition(b"\n")
                    if len(line) > MAX_NATIVE_LINE:
                        raise AdapterError("protocol", "native event limit exceeded")
                    try:
                        event = json.loads(line)
                    except (ValueError, UnicodeError):
                        raise AdapterError("protocol", "invalid native event") from None
                    if not isinstance(event, dict):
                        raise AdapterError("protocol", "invalid native event")
                    on_event(event)
                if len(pending) > MAX_NATIVE_LINE:
                    raise AdapterError("protocol", "native event limit exceeded")
        status = process.wait()
        if pending or (status == 0 and written != len(data)):
            raise AdapterError("protocol", "incomplete native stream")
        return status, errors.decode("utf-8", errors="replace")
    finally:
        selector.close()
        if process.poll() is None:
            process.kill()
        process.wait()
        for stream in (process.stdin, process.stdout, process.stderr):
            stream.close()


def entrypoint(adapter):
    try:
        adapter(read_request())
    except AdapterError as error:
        emit("failure", category=error.category, diagnostic=error.diagnostic)
    except Exception:
        # Never expose exception repr: native output can include private context.
        emit("failure", category="protocol", diagnostic="adapter encountered an unexpected error")
    emit("completed")
