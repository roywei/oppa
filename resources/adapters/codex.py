#!/usr/bin/env python3
"""Codex subscription adapter; validated against CLI 0.160.1.

Protocol reference: https://learn.chatgpt.com/docs/non-interactive-mode
Config reference: https://learn.chatgpt.com/docs/config-file/config-reference
An owner run at connectors or computer access is Codex as the owner set it up:
their config, rules, AGENTS.md, apps, plugins, hooks, memories, subagents, web
search and, when the browser is on, browser use. Connectors access keeps the
shell and computer use off; computer access drops the sandbox and approvals.
A read run ignores the owner's config and has the broker alone. Exec mode has
no host permission handler, so the access setting is the whole policy; every
native call counts as unmediated except a web search, which can change nothing.
"""

import json

from common import AdapterError, OPPA_TOOLS, emit, entrypoint, oppa_activity, native_environment, run_native


def failure_category(text, session=None):
    text = text.lower()
    if session and any(marker + session.lower() in text for marker in (
            "no rollout found for thread id ", "thread not found: ", "session not found: ")):
        return "session_invalid"
    if any(term in text for term in ("usage limit", "rate limit", "quota", "credits exhausted")):
        return "quota"
    if any(term in text for term in ("not logged in", "authentication", "unauthorized", "login required", "sign in")):
        return "auth"
    if any(term in text for term in ("overloaded", "unavailable", "connection refused")):
        return "unavailable"
    return "failed"


# A read run leaves memory, delegation and hooks to OPPA and has no native tools.
HOST_OWNED = ("plugins", "hooks", "memories", "multi_agent")
NATIVE = ("shell_tool", "unified_exec", "code_mode", "computer_use", "browser_use",
          "image_generation", "view_image")
# What an owner run at connectors access keeps off: the shell and the desktop.
COMPUTER = ("shell_tool", "unified_exec", "code_mode", "computer_use")
BROWSER = ("browser_use", "browser_use_external")


def command_for(request):
    access = request["access"]
    command = ["codex", "exec", "--json", "--skip-git-repo-check"]
    settings = ['forced_login_method="chatgpt"',
                'model_reasoning_effort=' + json.dumps(request.get("effort") or "medium")]
    if request.get("model"):
        command.extend(["-m", request["model"]])
    if access == "read":
        command.extend(["--ignore-user-config", "--ignore-rules"])
        settings += ['project_doc_max_bytes=0', 'features.skip_host_skill_discovery=true',
                     'approval_policy="never"', 'sandbox_mode="read-only"', 'web_search="disabled"']
        disabled = HOST_OWNED + NATIVE + ("apps",)
    elif access == "connectors":
        settings += ['approval_policy="never"', 'sandbox_mode="read-only"']
        disabled = COMPUTER + (() if request.get("browser") else BROWSER)
    else:
        command.append("--dangerously-bypass-approvals-and-sandbox")
        disabled = () if request.get("browser") else BROWSER
    for setting in settings:
        command.extend(["-c", setting])
    for feature in disabled:
        command.extend(["--disable", feature])
    mcp = request.get("mcp")
    if mcp is not None:
        settings = {"command": mcp["command"], "args": mcp["args"], "required": True,
                    "enabled_tools": list(OPPA_TOOLS) + mcp["read_tools"] + mcp["write_tools"],
                    "default_tools_approval_mode": "approve"}
        # Only these broker tools are preapproved. The host broker still checks
        # its per-run grant and revision before every write. Native tools retain
        # approval_policy=never and the read-only sandbox.
        # JSON string/array literals are valid TOML literals here. Each setting
        # stays one argv element, including quotes, Unicode and grant paths.
        for name, value in settings.items():
            command.extend(["-c", "mcp_servers.oppa." + name + "=" + json.dumps(value, ensure_ascii=False)])
    if request.get("instructions") and not request["session"]:
        # Developer instructions become part of a new thread's rollout; a resumed
        # thread already has them. The host starts a fresh session when they change.
        command.extend(["-c", "developer_instructions=" + json.dumps(request["instructions"], ensure_ascii=False)])
    if request["session"]:
        command.extend(["resume", request["session"]])
    return command + ["-"]


def adapter(request):
    state = {"answer": None, "completed": False, "error": None, "failed": False, "started": False}

    def on_event(event):
        kind = event.get("type")
        if kind == "thread.started":
            emit("session", handle=event["thread_id"])
        elif kind == "turn.started":
            state["started"] = True
        elif kind == "turn.completed":
            state["completed"] = True
        elif kind in ("turn.failed", "error"):
            state["error"] = str(event.get("error", event.get("message", "")))
            state["failed"] |= kind == "turn.failed"
        elif kind in ("item.started", "item.updated", "item.completed"):
            item = event.get("item", {})
            item_kind = item.get("type")
            if item_kind == "agent_message" and kind == "item.completed":
                state["answer"] = item["text"]
            elif item_kind == "web_search":
                emit("activity", kind="read")
            elif item_kind == "file_change":
                emit("activity", kind="unmediated")
            elif item_kind == "mcp_tool_call":
                emit("activity", kind=oppa_activity(request, item.get("server"), item.get("tool")))
            elif item_kind in ("command_execution", "tool_call", "computer_use", "browser_use"):
                # These native actions have not passed through our broker.
                emit("activity", kind="unmediated")
            elif item_kind == "error":
                # Native startup warnings are diagnostic items, not tool calls.
                state["error"] = str(item.get("message", ""))
            elif item_kind not in ("agent_message", "reasoning", "todo_list", "plan"):
                if request["access"] == "read":
                    raise AdapterError("protocol", "unrecognized Codex item")
                # The owner's Codex may bring tools this adapter has never seen,
                # such as a subagent or an app; they did not pass the broker.
                emit("activity", kind="unmediated")
        elif kind != "turn.started":
            raise AdapterError("protocol", "unrecognized Codex event")

    environment = native_environment({"OPENAI_API_KEY", "CODEX_API_KEY", "OPENAI_BASE_URL"})
    status, diagnostic = run_native(command_for(request), request["prompt"], on_event, environment)
    if status != 0 or state["failed"]:
        category = failure_category((state["error"] or "") + "\n" + diagnostic,
                                    request["session"] if not state["started"] else None)
        raise AdapterError(category, "Codex rejected or could not complete the run")
    if not state["completed"] or not isinstance(state["answer"], str):
        raise AdapterError("protocol", "Codex did not produce a completed answer")
    emit("answer", text=state["answer"])


if __name__ == "__main__":
    entrypoint(adapter)
