#!/usr/bin/env python3
"""Claude subscription adapter.

An owner run at connectors or computer access is the harness as the owner set
it up: their accounts, MCP servers, plugins, skills, settings and memory, Claude
in Chrome when the browser is on, and a high-effort subagent for hard work. The
host answers each permission prompt through the broker's permission tool.
A read run (a heartbeat, a result presentation) keeps all of that out but may
read the owner's Gmail, Calendar and Drive.
Stream reference: https://code.claude.com/docs/en/headless
Safe mode is used without a broker. Bare mode is deliberately absent: it uses
API-key auth, not subscription login.
"""

import json
import os
import re
import subprocess

from common import AdapterError, OPPA_TOOLS, emit, entrypoint, oppa_activity, native_environment, run_native

PERMISSION_TOOL = "mcp__oppa__permission_decide"
# Restricted mode drops the code-running tools unless --tools names them.
# What an owner run at connectors access keeps of the built-in tools: the web
# and subagents, without a shell or files.
CONNECTOR_TOOLS = "WebSearch,WebFetch,Task"
# A subagent the model may hand coding and hard problems to, at high effort.
DEEP_WORKER = {"deep-worker": {
    "description": "Hand it coding, debugging, research and other hard multi-step problems. It thinks at high "
                   "effort with the same tools and reports back.",
    "prompt": "You are a careful senior engineer and researcher working for the assistant that called you. Do "
              "the task completely, verify the result by running it where you can, and report exactly what you "
              "did, what you found and what is left.",
    "effort": "high"}}
# A read run (a heartbeat, a result presentation) may read the owner's Google
# accounts signed in to Claude, and only read them.
ACCOUNT_SERVER_PREFIX = "claude.ai "
ACCOUNT_READS = {
    "claude.ai Gmail": ("search_threads", "get_thread", "get_message", "list_labels", "list_drafts", "get_draft"),
    "claude.ai Google Calendar": ("list_calendars", "list_events", "search_events", "get_event", "suggest_time"),
    "claude.ai Google Drive": ("search_files", "list_recent_files", "get_file_metadata", "read_file_content",
                               "download_file_content"),
}


def account_tool(server, operation):
    return "mcp__" + re.sub(r"[^A-Za-z0-9_-]", "_", server) + "__" + operation


ACCOUNT_READ_TOOLS = {account_tool(server, operation): server
                      for server, operations in ACCOUNT_READS.items() for operation in operations}
# Their write tools, denied by name as well: explicit deny rules outrank any
# allow the owner's managed settings may hold.
ACCOUNT_WRITES = {
    "claude.ai Gmail": ("apply_sensitive_message_label", "apply_sensitive_thread_label", "create_draft",
                        "create_label", "delete_draft", "delete_label", "forward", "label_message",
                        "label_thread", "mark_message_spam", "mark_thread_spam", "reply", "send_message",
                        "trash_message", "trash_thread", "unlabel_message", "unlabel_thread",
                        "unmark_message_spam", "unmark_thread_spam", "untrash_message", "untrash_thread",
                        "update_draft", "update_label", "update_message_labels"),
    "claude.ai Google Calendar": ("create_event", "delete_event", "respond_to_event", "update_event"),
    "claude.ai Google Drive": ("copy_file", "create_file", "get_file_permissions", "share_file", "trash_file",
                               "update_file"),
}
ACCOUNT_DENIALS = ["mcp__claude_ai_Claude_Docs__*"] + sorted(
    account_tool(server, operation) for server, operations in ACCOUNT_WRITES.items() for operation in operations)


def event_tag(value):
    """Expose bounded protocol enum tags, never native messages or payloads."""
    if value is None:
        return "absent"
    if isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value):
        return value
    return "invalid"


def failure_category(text, session=None):
    text = text.lower()
    if session and ("no conversation found with session id: " + session.lower() in text
                    or "session " + session.lower() + " not found. it may have been archived or expired." in text):
        return "session_invalid"
    if any(term in text for term in ("rate_limit", "rate limit", "usage limit", "quota", "hit your limit")):
        return "quota"
    if any(term in text for term in ("authentication", "not logged in", "unauthorized", "login", "sign in")):
        return "auth"
    if any(term in text for term in ("overloaded", "unavailable", "connection refused")):
        return "unavailable"
    return "failed"


def full_access(request):
    """Connectors or computer access; the request validator requires a broker."""
    return request["access"] != "read"


def command_for(request):
    full = full_access(request)
    computer = request["access"] == "computer"
    readable = request.get("readable_dir")
    command = ["claude", "-p", "--output-format", "stream-json", "--verbose",
               "--effort", request.get("effort") or "medium"]
    if request.get("model"):
        command.extend(["--model", request["model"]])
    mcp = request.get("mcp")
    if full:
        # The owner's own run: the harness as the owner set it up, with every
        # account, MCP server, plugin, skill, setting and memory they gave it.
        # The host answers each permission prompt.
        config = {"mcpServers": {"oppa": {"type": "stdio", "command": mcp["command"], "args": mcp["args"]}}}
        broker_tools = list(OPPA_TOOLS) + mcp["read_tools"] + mcp["write_tools"]
        allowed = ["mcp__oppa__" + name.replace(".", "_") for name in broker_tools] + (["Read"] if readable else [])
        command.extend(["--chrome" if request.get("browser") else "--no-chrome",
                        "--mcp-config", json.dumps(config),
                        "--allowedTools", ",".join(allowed),
                        "--permission-prompt-tool", PERMISSION_TOOL,
                        # Explicit, so an owner setting of bypassPermissions
                        # cannot skip the host's permission tool.
                        "--permission-mode", "default",
                        "--agents", json.dumps(DEEP_WORKER)])
        if computer:
            command.extend(["--add-dir", "/"])
        else:
            command.extend(["--tools", CONNECTOR_TOOLS + (",Read" if readable else "")])
            if readable:
                command.extend(["--add-dir", readable])
    else:
        # A read run (a heartbeat, a result presentation) keeps the owner's
        # accounts, MCP servers, settings and memory out: only the broker, the
        # accounts' reads, and Read for this turn's attached files, which
        # restricted mode confines to their folder.
        command.extend(["--no-chrome", "--tools", "Read" if readable else "", "--permission-mode", "dontAsk"])
        if mcp is None:
            # Only the broker loads; otherwise restricted mode still loads the
            # account's claude.ai connectors, whose read tools are allowed below.
            command.extend(["--strict-mcp-config", "--safe-mode"])
        else:
            settings = {"disableAllHooks": True, "autoMemoryEnabled": False,
                        "claudeMdExcludes": ["**"],
                        "pluginConfigs": {"agents-md@builtin": {"options": {"instructionFiles": "managed-only"}}}}
            config = {"mcpServers": {"oppa": {"type": "stdio", "command": mcp["command"], "args": mcp["args"]}}}
            broker_tools = list(OPPA_TOOLS) + mcp["read_tools"] + mcp["write_tools"]
            allowed = ["mcp__oppa__" + name.replace(".", "_") for name in broker_tools] + sorted(ACCOUNT_READ_TOOLS)
            if readable:
                allowed.append("Read")
            command.extend(["--restricted", "--disable-slash-commands", "--settings", json.dumps(settings),
                            "--mcp-config", json.dumps(config), "--allowedTools", ",".join(allowed),
                            "--disallowedTools", ",".join(ACCOUNT_DENIALS)])
            if readable:
                command.extend(["--add-dir", readable])
    if request.get("instructions"):
        # The system prompt is supplied on every call and never enters the
        # resumed transcript, so standing instructions do not accumulate.
        command.extend(["--append-system-prompt", request["instructions"]])
    if request["session"]:
        command.extend(["--resume", request["session"]])
    return command


def require_subscription(environment):
    # Read only the CLI's public status response; never open credential stores.
    # The status is not printed, persisted, or included in model context.
    try:
        process = subprocess.Popen(["claude", "--safe-mode", "auth", "status", "--json"],
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=environment)
    except OSError:
        raise AdapterError("unavailable", "Claude CLI could not be started") from None
    try:
        data = process.stdout.read(65537)
        if len(data) > 65536:
            raise AdapterError("auth", "Claude sign-in status could not be verified")
        status = process.wait()
        value = json.loads(data)
        if status or not value.get("loggedIn") or value.get("authMethod") != "claude.ai":
            raise AdapterError("auth", "Claude subscription sign-in is required")
    except (ValueError, AttributeError):
        raise AdapterError("auth", "Claude sign-in status could not be verified") from None
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        process.stdout.close()


def adapter(request):
    state = {"session": None, "result": None, "started": False, "reads": {}, "failed_reads": {}}
    full = full_access(request)
    mcp = request.get("mcp") or {}
    broker = {"mcp__oppa__" + tool.replace(".", "_"): tool
              for tool in list(OPPA_TOOLS) + mcp.get("read_tools", []) + mcp.get("write_tools", [])}

    def on_event(event):
        kind = event.get("type")
        if kind == "system" and event.get("subtype") == "init":
            if request.get("mcp") is not None:
                servers = event.get("mcp_servers", [])
                if not any(server.get("name") == "oppa" and server.get("status") == "connected" for server in servers):
                    raise AdapterError("unavailable", "oppa MCP server did not connect")
                # The owner's run has every server they configured; a read run
                # has the broker and the account's claude.ai connectors, of
                # which dontAsk lets only the allowed read tools run.
                if not full and any(server.get("name") != "oppa"
                                    and not str(server.get("name", "")).startswith(ACCOUNT_SERVER_PREFIX)
                                    for server in servers):
                    raise AdapterError("protocol", "unexpected MCP server in a read run")
            state["session"] = event["session_id"]
            emit("session", handle=state["session"])
        elif kind == "assistant":
            state["started"] = True
            for item in event.get("message", {}).get("content", []):
                if item.get("type") in ("tool_use", "server_tool_use"):
                    name = item.get("name", "")
                    if name == "Read" and request.get("readable_dir"):
                        # Graded when its result arrives: only a Read that succeeded counts.
                        state["reads"][item.get("id")] = str((item.get("input") or {}).get("file_path", ""))
                    elif name in broker:
                        emit("activity", kind=oppa_activity(request, "oppa", broker[name]))
                    elif name in ACCOUNT_READ_TOOLS and not full:
                        emit("activity", kind="read")
                    else:
                        # The owner's own tools, accounts and browser act
                        # without passing through the broker; a read run's CLI
                        # refuses them, but what was attempted is not known.
                        emit("activity", kind="unmediated")
        elif kind == "user":
            # Native MCP result envelopes may echo tool output. Never turn that
            # content into an answer or emit it as diagnostics; prior assistant
            # tool-use events establish activity, the broker journal outcomes.
            for item in event.get("message", {}).get("content", []):
                if item.get("type") == "tool_result":
                    path = state["reads"].pop(item.get("tool_use_id"), None)
                    if path is not None:
                        # The name alone, never the path or what the file held,
                        # cut to 255 bytes on a character boundary.
                        name = os.path.basename(path).encode(errors="ignore")[:255].decode(errors="ignore")
                        if item.get("is_error") is not True:
                            # A file read outside the attachments folder means the
                            # confinement failed, so it counts as unmediated.
                            folder = os.path.realpath(request["readable_dir"]) + os.sep
                            inside = os.path.realpath(path).startswith(folder)
                            emit("activity", kind="read" if inside else "unmediated")
                            if name:
                                emit("file_read", name=name, opened=True)
                        elif name:
                            # Refused or failed: the result's permission denials tell which.
                            state["failed_reads"][item.get("tool_use_id")] = name
                        continue
        elif kind == "result":
            if state["result"] is not None:
                raise AdapterError("protocol", "duplicate Claude result")
            # Only a Read the harness's permissions denied was refused; one that
            # failed otherwise, such as a missing file, read nothing either way.
            denied = {d.get("tool_use_id") for d in event.get("permission_denials") or [] if isinstance(d, dict)}
            for key, name in state["failed_reads"].items():
                if key in denied:
                    emit("file_read", name=name, opened=False)
            state["result"] = event
        elif kind == "system":
            # Progress and count metadata (status, api_retry, thinking_tokens,
            # commands_changed, ...). Tool calls and answers arrive only as
            # assistant and result events, so a subtype a newer CLI adds is
            # ignored: 2.1.286's commands_changed used to fail live turns.
            pass
        elif kind == "rate_limit_event":
            pass  # Advisory status; a final result establishes success/failure.
        elif kind == "tool_progress":
            # Elapsed time for a call an assistant event already announced,
            # sent while a long tool runs; newer CLIs emit it and every
            # delegated task with a long Bash call failed on it (2026-10-11).
            pass
        else:
            raise AdapterError("protocol", "unrecognized Claude event (type="
                + event_tag(kind) + ", subtype=" + event_tag(event.get("subtype")) + ")")

    environment = native_environment({"ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN",
        "ANTHROPIC_BASE_URL", "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX",
        "CLAUDE_CODE_USE_FOUNDRY"})
    if request.get("mcp") is not None:
        # Without this the CLI sometimes starts the turn before the account's
        # connectors have loaded (4 of 6 runs loaded with 2.1.283; 12 of 12 with it).
        environment["MCP_CONNECTION_NONBLOCKING"] = "false"
    require_subscription(environment)
    if request.get("readable_dir"):
        # Read may also open files under the CLI's own temporary folder, which every run
        # in the same working directory would share. Inside this run's folder of attached
        # files it is the run's alone, and goes with that folder however the run ends.
        environment["CLAUDE_CODE_TMPDIR"] = os.path.join(request["readable_dir"], ".tmp")
        os.makedirs(environment["CLAUDE_CODE_TMPDIR"], mode=0o700, exist_ok=True)
    status, diagnostic = run_native(command_for(request), request["prompt"], on_event, environment)
    # However the run ended, a call with no result may have done what that call does,
    # so what the run did is not fully known.
    if state["reads"]:
        raise AdapterError("protocol", "a tool did not return a result")
    result = state["result"]
    if result is not None and (result.get("is_error") or result.get("subtype") != "success"):
        category = failure_category(str(result.get("errors", "")) + str(result.get("result", "")),
                                    request["session"] if not state["started"] else None)
        raise AdapterError(category, "Claude rejected or could not complete the run")
    if status != 0:
        raise AdapterError(failure_category(diagnostic, request["session"] if not state["started"] else None),
                           "Claude could not complete the run")
    if result is None or not isinstance(result.get("result"), str):
        raise AdapterError("protocol", "Claude did not produce a completed answer")
    emit("answer", text=result["result"])


if __name__ == "__main__":
    entrypoint(adapter)
