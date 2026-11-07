#![cfg(unix)]

use oppa::harness::{
    run, run_controlled, Access, Activity, AgentManifest, CleanupStatus, FailureCategory,
    HarnessRequest, McpServer, StopReason,
};
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::Path};
use tempfile::TempDir;

fn request() -> HarnessRequest {
    HarnessRequest {
        access: Default::default(),
        readable_dir: None,
        session_log: None,
        protocol_version: 1,
        conversation: "synthetic-conversation".into(),
        prompt: "A literal $(touch NEVER) and two lines:\n你好".into(),
        session: None,
        instructions: None,
        mcp: None,
        working_directory: None,
        model: None,
        effort: None,
        browser: false,
    }
}

fn fixture(code: &str) -> (TempDir, AgentManifest) {
    let directory = tempfile::tempdir().unwrap();
    let script = directory.path().join("adapter.py");
    fs::write(&script, code).unwrap();
    let manifest = AgentManifest {
        id: "synthetic".into(),
        family: "fixture".into(),
        command: vec!["python3".into(), script.display().to_string()],
        timeout_seconds: 5,
        task_timeout_seconds: 5,
        model: None,
        effort: None,
    };
    (directory, manifest)
}

fn emit_fixture(events: serde_json::Value) -> String {
    format!("import json,sys\njson.load(sys.stdin)\nfor e in json.loads({:?}): print(json.dumps(e),flush=True)\n", events.to_string())
}

#[tokio::test]
async fn prose_and_session_cross_boundary_without_shell_interpretation() {
    let (_directory, manifest) = fixture(
        "import json,sys\nr=json.load(sys.stdin)\nprint(json.dumps({'type':'session','handle':'native-session'}))\nprint(json.dumps({'type':'answer','text':r['prompt']}))\nprint(json.dumps({'type':'completed'}))\n",
    );
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(outcome.answer.as_deref(), Some(request().prompt.as_str()));
    assert_eq!(outcome.session.as_deref(), Some("native-session"));
    assert!(outcome.failure.is_none());
    assert_eq!(outcome.activity, Activity::None);
}

#[tokio::test]
async fn quota_rejection_preserves_complete_monotonic_activity_evidence() {
    for (activity, expected) in [
        ("none", Activity::None),
        ("read", Activity::Read),
        ("effect", Activity::Effect),
        ("unmediated", Activity::Unmediated),
    ] {
        let (_directory, manifest) = fixture(&emit_fixture(serde_json::json!([
            {"type":"activity","kind":activity},
            {"type":"activity","kind":"none"},
            {"type":"failure","category":"quota","diagnostic":"synthetic quota"},
            {"type":"completed"}
        ])));
        let outcome = run(&manifest, &request()).await.unwrap();
        assert_eq!(outcome.activity, expected);
        assert!(outcome.telemetry_complete);
        assert_eq!(outcome.failure.unwrap().category, FailureCategory::Quota);
        assert!(outcome.answer.is_none());
    }
}

#[tokio::test]
async fn incomplete_or_contradictory_stream_preserves_observed_answer_and_reports_fault() {
    for events in [
        serde_json::json!([{"type":"failure","category":"quota","diagnostic":"synthetic"}]),
        serde_json::json!([{"type":"completed"}]),
        serde_json::json!([{"type":"answer","text":"first"},{"type":"completed"},{"type":"activity","kind":"effect"}]),
        serde_json::json!([{"type":"answer","text":"first"},{"type":"answer","text":"second"},{"type":"completed"}]),
    ] {
        let observed_answer = events
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["type"] == "answer")
            .map(|event| event["text"].as_str().unwrap().to_owned());
        let (_directory, manifest) = fixture(&emit_fixture(events));
        let outcome = run(&manifest, &request()).await.unwrap();
        assert_eq!(
            outcome.failure.as_ref().unwrap().category,
            FailureCategory::Protocol
        );
        assert_eq!(outcome.activity, Activity::None);
        assert!(!outcome.telemetry_complete);
        assert_eq!(outcome.answer, observed_answer);
    }
}

#[tokio::test]
async fn explicit_empty_answer_is_silence() {
    let (_directory, manifest) = fixture(&emit_fixture(serde_json::json!([
        {"type":"answer","text":""},{"type":"completed"}
    ])));
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(outcome.answer.as_deref(), Some(""));
    assert!(outcome.failure.is_none());
}

#[tokio::test]
async fn oversized_or_private_diagnostics_are_bounded_and_not_echoed() {
    for code in [
        "import sys\nsys.stdin.read()\nprint('x'*(300*1024))\n",
        "import sys\nsys.stdin.read()\nsys.stderr.write('SYNTHETIC_PRIVATE_DIAGNOSTIC'*4000)\n",
        "import sys\nsys.stdin.read()\nprint('SYNTHETIC_PRIVATE_DIAGNOSTIC')\n",
    ] {
        let (_directory, manifest) = fixture(code);
        let outcome = run(&manifest, &request()).await.unwrap();
        assert_eq!(
            outcome.failure.as_ref().unwrap().category,
            FailureCategory::Protocol
        );
        assert!(!serde_json::to_string(&outcome)
            .unwrap()
            .contains("SYNTHETIC_PRIVATE_DIAGNOSTIC"));
    }
}

#[tokio::test]
async fn timeout_kills_and_reaps_the_adapter_and_its_child() {
    let (directory, mut manifest) = fixture(
        "import json,sys,subprocess,time,os\njson.load(sys.stdin)\nopen(sys.argv[1],'w').write(str(os.getpid()))\nsubprocess.Popen([sys.executable,'-c',\"import time,pathlib,sys;time.sleep(2);pathlib.Path(sys.argv[1]).write_text('unexpected')\",sys.argv[2]])\ntime.sleep(30)\n",
    );
    let pid_path = directory.path().join("pid");
    let marker = directory.path().join("effect");
    manifest
        .command
        .extend([pid_path.display().to_string(), marker.display().to_string()]);
    manifest.timeout_seconds = 1;
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(
        outcome.failure.as_ref().unwrap().category,
        FailureCategory::Timeout
    );
    assert_eq!(outcome.activity, Activity::None);
    assert!(!outcome.telemetry_complete);
    let pid: i32 = fs::read_to_string(pid_path).unwrap().parse().unwrap();
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "adapter was not reaped");
    tokio::time::sleep(std::time::Duration::from_millis(1300)).await;
    assert!(!marker.exists(), "descendant survived timeout");
}

#[tokio::test]
async fn missing_executable_is_an_explicit_pre_effect_failure() {
    let (_directory, mut manifest) = fixture("");
    manifest.command = vec!["/nonexistent/synthetic-adapter".into()];
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(
        outcome.failure.as_ref().unwrap().category,
        FailureCategory::Unavailable
    );
    assert!(outcome.telemetry_complete);
    assert_eq!(outcome.activity, Activity::None);
    assert!(outcome.answer.is_none());
    let mut invalid = request();
    invalid.protocol_version = 9;
    assert!(run(&manifest, &invalid).await.is_err());
}

/// The length of the largest file a turn may open, 20 MiB, as base64.
const PDF: usize = 4 * (20 * 1024 * 1024usize).div_ceil(3);

fn native_fixture(provider: &str, events: serde_json::Value) -> (TempDir, AgentManifest) {
    let directory = tempfile::tempdir().unwrap();
    let native = directory.path().join(provider);
    // The events stay out of the script's source, where Python 3.9 misreads a
    // long line of non-ASCII text. "@pdf@" stands for the largest file a turn
    // may open, 20 MiB, as base64; an "exit" event ends the CLI with its code
    // and stderr.
    fs::write(directory.path().join("events.json"), events.to_string()).unwrap();
    let script = "#!/usr/bin/env python3\nimport json,sys,os\nassert 'OPENAI_API_KEY' not in os.environ\nassert 'ANTHROPIC_API_KEY' not in os.environ\nif any('claude_ai' in a for a in sys.argv): assert os.environ.get('MCP_CONNECTION_NONBLOCKING') == 'false'\nif '--version' in sys.argv:\n print('2.1.284 (Claude Code)')\nelif 'auth' in sys.argv:\n print(json.dumps({'loggedIn':True,'authMethod':'claude.ai'}))\nelse:\n sys.stdin.read()\n open(os.path.join(os.path.dirname(__file__),'environment.json'),'w').write(json.dumps(sorted(os.environ)))\n t = os.environ.get('CLAUDE_CODE_TMPDIR')\n open(os.path.join(os.path.dirname(__file__),'temporary.txt'),'a').write(json.dumps([t, bool(t) and os.path.isdir(t)])+'\\n')\n for e in json.load(open(os.path.join(os.path.dirname(__file__),'events.json'))):\n  if e.get('type') == 'exit': sys.stderr.write(e['stderr']); sys.exit(e['code'])\n  line = json.dumps(e)\n  if '\"@pdf@\"' in line: line = line.replace('\"@pdf@\"','\"'+'A'*@PDF@+'\"')\n  print(line,flush=True)\n"
        .replace("@PDF@", &PDF.to_string());
    fs::write(&native, script).unwrap();
    fs::set_permissions(&native, fs::Permissions::from_mode(0o700)).unwrap();
    let wrapper = directory.path().join("wrapper.py");
    let adapter = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("resources/adapters")
        .join(format!("{provider}.py"));
    fs::write(&wrapper,
        "import os,sys,runpy\nos.environ['PATH']=sys.argv[1]+os.pathsep+os.environ['PATH']\nos.environ.pop('OPENAI_API_KEY',None)\nos.environ.pop('ANTHROPIC_API_KEY',None)\nsys.path.insert(0,os.path.dirname(sys.argv[2]))\nrunpy.run_path(sys.argv[2],run_name='__main__')\n").unwrap();
    let manifest = AgentManifest {
        id: provider.into(),
        family: "synthetic".into(),
        timeout_seconds: 5,
        task_timeout_seconds: 5,
        model: None,
        effort: None,
        command: vec![
            "python3".into(),
            wrapper.display().to_string(),
            directory.path().display().to_string(),
            adapter.display().to_string(),
        ],
    };
    (directory, manifest)
}

#[tokio::test]
async fn codex_native_translation_preserves_session_final_text_and_quota_boundary() {
    let session = "12345678-1234-1234-1234-123456789abc";
    let (_directory, manifest) = native_fixture(
        "codex",
        serde_json::json!([
        {"type":"thread.started","thread_id":session},
        {"type":"item.completed","item":{"type":"error","message":"synthetic startup warning"}},
        {"type":"turn.started"},
            {"type":"item.completed","item":{"type":"agent_message","text":"Synthetic final prose."}},
            {"type":"turn.completed"}
        ]),
    );
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(outcome.answer.as_deref(), Some("Synthetic final prose."));
    assert_eq!(outcome.session.as_deref(), Some(session));
    assert!(outcome.failure.is_none());
    assert_eq!(outcome.activity, Activity::None);
    let (_directory, manifest) = native_fixture(
        "codex",
        serde_json::json!([
            {"type":"item.started","item":{"type":"command_execution"}},
            {"type":"turn.failed","error":{"message":"synthetic usage limit"}}
        ]),
    );
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(
        outcome.failure.as_ref().unwrap().category,
        FailureCategory::Quota
    );
    assert_eq!(outcome.activity, Activity::Unmediated);
    assert!(outcome.telemetry_complete);
}

#[tokio::test]
async fn claude_native_translation_requires_final_result_and_tracks_tools() {
    let session = "12345678-1234-1234-1234-123456789abc";
    let (_directory, manifest) = native_fixture(
        "claude",
        serde_json::json!([
        {"type":"system","subtype":"init","session_id":session},
        {"type":"system","subtype":"thinking_tokens","thinking_tokens":27},
        {"type":"assistant","message":{"content":[{"type":"text","text":"Synthetic prose."}]}},
            {"type":"result","subtype":"success","is_error":false,"result":"Synthetic prose."}
        ]),
    );
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(outcome.answer.as_deref(), Some("Synthetic prose."));
    assert_eq!(outcome.session.as_deref(), Some(session));
    assert!(outcome.failure.is_none());
    let (_directory, manifest) = native_fixture(
        "claude",
        serde_json::json!([
            {"type":"assistant","message":{"content":[{"type":"tool_use","name":"unknown"}]}},
            {"type":"result","subtype":"error_during_execution","is_error":true,"errors":["synthetic rate_limit"]}
        ]),
    );
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(
        outcome.failure.as_ref().unwrap().category,
        FailureCategory::Quota
    );
    assert_eq!(outcome.activity, Activity::Unmediated);
    assert!(outcome.telemetry_complete);
}

#[tokio::test]
async fn unknown_native_metadata_preserves_activity_and_withholds_payload() {
    // Incident 2026-09-30: Claude Code 2.1.286 began emitting
    // system/commands_changed and every turn that saw it failed. A system
    // subtype carries no tool call or answer, so a new one is ignored.
    let (_directory, manifest) = native_fixture(
        "claude",
        serde_json::json!([
            {"type":"system","subtype":"init","session_id":"12345678-1234-1234-1234-123456789abc"},
            {"type":"system","subtype":"future_protocol_event","diagnostic":"SYNTHETIC_PRIVATE_PAYLOAD"},
            // Incident 2026-10-11: a newer CLI reports a long tool call's
            // elapsed time as a top-level tool_progress event.
            {"type":"tool_progress","tool_use_id":"toolu_synthetic","tool_name":"Bash","elapsed_time_seconds":30},
            {"type":"result","subtype":"success","is_error":false,"result":"Synthetic prose."}
        ]),
    );
    let outcome = run(&manifest, &request()).await.unwrap();
    assert!(outcome.failure.is_none(), "{:?}", outcome.failure);
    assert_eq!(outcome.answer.as_deref(), Some("Synthetic prose."));
    // An unknown event type could carry a call or an answer, so it still fails
    // closed, named by bounded tags and without its payload.
    let (_directory, manifest) = native_fixture(
        "claude",
        serde_json::json!([
            {"type":"future_event","subtype":"call","diagnostic":"SYNTHETIC_PRIVATE_PAYLOAD"}
        ]),
    );
    let outcome = run(&manifest, &request()).await.unwrap();
    let failure = outcome.failure.as_ref().unwrap();
    assert_eq!(failure.category, FailureCategory::Protocol);
    assert!(failure
        .diagnostic
        .contains("type=future_event, subtype=call"));
    assert!(!failure.diagnostic.contains("SYNTHETIC_PRIVATE_PAYLOAD"));
    assert_eq!(outcome.activity, Activity::None);
    assert!(!outcome.telemetry_complete);
}

fn broker() -> McpServer {
    McpServer {
        read_tools: vec![],
        write_tools: vec![],
        command: "/synthetic/bin/machine with spaces".into(),
        args: vec![
            "mcp".into(),
            "serve".into(),
            "--grant".into(),
            "/synthetic/grant \"quoted\" $(touch NEVER) \\ 中文\n.json".into(),
        ],
    }
}

fn adapter_command(provider: &str, value: serde_json::Value) -> std::process::Output {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let adapters = Path::new(env!("CARGO_MANIFEST_DIR")).join("resources/adapters");
    let mut child = Command::new("python3")
        .args(["-B", "-c", "import sys,json,importlib;sys.path.insert(0,sys.argv[1]);from common import read_request,AdapterError\ntry: print(json.dumps(importlib.import_module(sys.argv[2]).command_for(read_request())))\nexcept AdapterError: sys.exit(2)"])
        .arg(adapters).arg(provider).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&serde_json::to_vec(&value).unwrap())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn optional_broker_request_is_backward_compatible_and_strict() {
    let legacy = serde_json::to_value(request()).unwrap();
    assert!(legacy.get("mcp").is_none());
    assert!(serde_json::from_value::<HarnessRequest>(legacy.clone())
        .unwrap()
        .mcp
        .is_none());
    for provider in ["codex", "claude"] {
        assert!(adapter_command(provider, legacy.clone()).status.success());
        for mcp in [
            serde_json::json!({"command":"machine","args":[1]}),
            serde_json::json!({"command":"machine","args":[],"env":{"TOKEN":"synthetic"}}),
            serde_json::json!({"command":"machine\u{0000}","args":[]}),
            serde_json::json!({"command":" ","args":[]}),
            serde_json::json!({"command":"machine","args":["bad\u{0000}"]}),
        ] {
            let mut invalid = legacy.clone();
            invalid["mcp"] = mcp;
            let output = adapter_command(provider, invalid);
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
        }
    }
}

#[test]
fn provider_configuration_preserves_literal_launch_arguments_and_scope() {
    let mut req = request();
    req.mcp = Some(broker());
    let value = serde_json::to_value(&req).unwrap();
    let output = adapter_command("codex", value.clone());
    assert!(output.status.success());
    let args: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
    let settings = args
        .windows(2)
        .filter(|pair| pair[0] == "-c")
        .map(|pair| pair[1].as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let parsed: toml::Value = toml::from_str(&settings).unwrap();
    let server = &parsed["mcp_servers"]["oppa"];
    assert_eq!(server["command"].as_str(), Some(broker().command.as_str()));
    assert_eq!(
        server["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x.as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        broker().args
    );
    assert_eq!(server["required"].as_bool(), Some(true));
    assert_eq!(
        server["default_tools_approval_mode"].as_str(),
        Some("approve")
    );
    // Every host tool family the broker may offer: history, memory, tasks,
    // schedules, wakes, messages and people.
    assert_eq!(server["enabled_tools"].as_array().unwrap().len(), 25);
    assert_eq!(parsed["approval_policy"].as_str(), Some("never"));
    assert_eq!(parsed["model_reasoning_effort"].as_str(), Some("medium"));

    let output = adapter_command("claude", value);
    assert!(output.status.success());
    let args: Vec<String> = serde_json::from_slice(&output.stdout).unwrap();
    let value_after = |key: &str| args[args.iter().position(|arg| arg == key).unwrap() + 1].clone();
    assert!(args.iter().any(|x| x == "--restricted"));
    // A read run loads the account's claude.ai connectors, of which only the
    // Gmail, Calendar and Drive read tools are allowed; a child's restricted run loads none.
    assert!(!args.iter().any(|x| x == "--strict-mcp-config"));
    assert!(!args.iter().any(|x| x == "--safe-mode" || x == "--bare"));
    assert_eq!(value_after("--tools"), "");
    assert_eq!(value_after("--permission-mode"), "dontAsk");
    assert_eq!(value_after("--effort"), "medium");
    let config: serde_json::Value = serde_json::from_str(&value_after("--mcp-config")).unwrap();
    assert_eq!(config["mcpServers"]["oppa"]["command"], broker().command);
    assert_eq!(
        config["mcpServers"]["oppa"]["args"],
        serde_json::json!(broker().args)
    );
    assert_eq!(config["mcpServers"].as_object().unwrap().len(), 1);
    let settings: serde_json::Value = serde_json::from_str(&value_after("--settings")).unwrap();
    assert_eq!(settings["disableAllHooks"], true);
    assert_eq!(settings["autoMemoryEnabled"], false);
    assert_eq!(settings["claudeMdExcludes"], serde_json::json!(["**"]));
    let allowed = value_after("--allowedTools");
    assert_eq!(allowed.split(',').count(), 25 + 16);
    assert!(allowed
        .split(',')
        .any(|t| t == "mcp__claude_ai_Gmail__search_threads"));
    assert!(!allowed.contains("send_message") && !allowed.contains("create_event"));
    let denied = value_after("--disallowedTools");
    assert!(denied
        .split(',')
        .any(|t| t == "mcp__claude_ai_Gmail__send_message"));
    assert!(denied
        .split(',')
        .any(|t| t == "mcp__claude_ai_Claude_Docs__*"));
    assert!(!allowed.contains('*'));
}

#[tokio::test]
async fn only_injected_allowlisted_oppa_calls_are_mediated() {
    for provider in ["codex", "claude"] {
        for (enabled, server, tool, expected) in [
            (true, "oppa", "history.search", Activity::Read),
            (true, "oppa", "memory.read", Activity::Read),
            (true, "oppa", "memory.commit", Activity::Effect),
            (true, "oppa", "tasks.delegate", Activity::Effect),
            (true, "oppa", "tasks.stop", Activity::Effect),
            (true, "oppa", "tasks.list", Activity::Read),
            (true, "oppa", "tasks.read", Activity::Read),
            (true, "other", "memory.read", Activity::Unmediated),
            (true, "oppa", "memory.forget", Activity::Effect),
            (true, "oppa", "memory.erase", Activity::Unmediated),
            (true, "oppa", "tasks.restart", Activity::Unmediated),
            (false, "oppa", "tasks.delegate", Activity::Unmediated),
            (false, "oppa", "tasks.read", Activity::Unmediated),
            (false, "oppa", "history.search", Activity::Unmediated),
        ] {
            let events = if provider == "codex" {
                serde_json::json!([
                    {"type":"item.started","item":{"type":"mcp_tool_call","server":server,"tool":tool}},
                    {"type":"item.completed","item":{"type":"agent_message","text":"Synthetic broker result."}},
                    {"type":"turn.completed"}
                ])
            } else {
                serde_json::json!([
                    {"type":"system","subtype":"init","session_id":"12345678-1234-1234-1234-123456789abc","mcp_servers":[{"name":"oppa","status":"connected"}]},
                    {"type":"assistant","message":{"content":[{"type":"tool_use","name":format!("mcp__{}__{}",server,tool.replace('.',"_"))}]}},
                    {"type":"user","message":{"content":[{"type":"tool_result","content":"SYNTHETIC_PRIVATE_TOOL_RESULT"}]}},
                    {"type":"result","subtype":"success","is_error":false,"result":"Synthetic broker result."}
                ])
            };
            let (_dir, manifest) = native_fixture(provider, events);
            let mut req = request();
            req.mcp = enabled.then(broker);
            let outcome = run(&manifest, &req).await.unwrap();
            assert!(
                outcome.failure.is_none(),
                "{provider}: {:?}",
                outcome.failure
            );
            assert_eq!(outcome.activity, expected);
            assert!(outcome.telemetry_complete);
            assert!(!serde_json::to_string(&outcome)
                .unwrap()
                .contains("SYNTHETIC_PRIVATE_TOOL_RESULT"));
        }
    }
}

#[tokio::test]
async fn connector_read_tools_are_allowed_and_counted_as_reads() {
    // C-8: the host passes the broker's connector read tools; both adapters
    // allow exactly those and count their calls as mediated reads.
    let mut req = request();
    req.mcp = Some(McpServer {
        read_tools: vec!["mac.calendar_events".into()],
        ..broker()
    });
    let value = serde_json::to_value(&req).unwrap();
    let claude: Vec<String> =
        serde_json::from_slice(&adapter_command("claude", value.clone()).stdout).unwrap();
    let allowed = &claude[claude.iter().position(|x| x == "--allowedTools").unwrap() + 1];
    assert!(allowed
        .split(',')
        .any(|tool| tool == "mcp__oppa__mac_calendar_events"));
    let codex: Vec<String> =
        serde_json::from_slice(&adapter_command("codex", value.clone()).stdout).unwrap();
    assert!(codex
        .iter()
        .any(|arg| arg.contains("\"mac.calendar_events\"")));
    for invalid in [
        json!(["history.search"]),
        json!(["no-dot"]),
        json!(["mac.Bad Name"]),
    ] {
        let mut request = value.clone();
        request["mcp"]["read_tools"] = invalid;
        assert_eq!(adapter_command("claude", request).status.code(), Some(2));
    }
    for provider in ["codex", "claude"] {
        let events = if provider == "codex" {
            json!([
                {"type":"item.started","item":{"type":"mcp_tool_call","server":"oppa","tool":"mac.calendar_events"}},
                {"type":"item.completed","item":{"type":"agent_message","text":"Synthetic answer."}},
                {"type":"turn.completed"}
            ])
        } else {
            json!([
                {"type":"system","subtype":"init","session_id":"12345678-1234-1234-1234-123456789abc","mcp_servers":[{"name":"oppa","status":"connected"}]},
                {"type":"assistant","message":{"content":[{"type":"tool_use","name":"mcp__oppa__mac_calendar_events"}]}},
                {"type":"result","subtype":"success","is_error":false,"result":"Synthetic answer."}
            ])
        };
        let (_dir, manifest) = native_fixture(provider, events);
        let outcome = run(&manifest, &req).await.unwrap();
        assert!(
            outcome.failure.is_none(),
            "{provider}: {:?}",
            outcome.failure
        );
        assert_eq!(outcome.activity, Activity::Read, "{provider}");
    }
}

#[tokio::test]
async fn native_file_changes_are_unmediated_even_when_a_broker_is_injected() {
    for enabled in [false, true] {
        let (_dir, manifest) = native_fixture(
            "codex",
            serde_json::json!([
                {"type":"item.completed","item":{"type":"file_change","status":"completed","changes":[{"path":"synthetic.md","kind":"update"}]}},
                {"type":"turn.failed","error":{"message":"Synthetic quota exhausted"}}
            ]),
        );
        let mut req = request();
        req.mcp = enabled.then(broker);
        let outcome = run(&manifest, &req).await.unwrap();
        assert_eq!(
            outcome.failure.as_ref().unwrap().category,
            FailureCategory::Quota
        );
        assert_eq!(outcome.activity, Activity::Unmediated);
        assert!(outcome.telemetry_complete);
    }
}

#[tokio::test]
async fn a_codex_web_search_is_a_read_so_another_harness_may_take_the_turn() {
    // C-5: a web search changes nothing, so a turn that then fails may go on
    // elsewhere; the native calls that could act are unmediated (test above).
    let (_dir, manifest) = native_fixture(
        "codex",
        serde_json::json!([
            {"type":"item.completed","item":{"type":"web_search","query":"synthetic"}},
            {"type":"turn.failed","error":{"message":"Synthetic quota exhausted"}}
        ]),
    );
    let outcome = run(&manifest, &request()).await.unwrap();
    assert_eq!(
        outcome.failure.as_ref().unwrap().category,
        FailureCategory::Quota
    );
    assert_eq!(outcome.activity, Activity::Read);
}

#[tokio::test]
async fn missing_required_claude_broker_fails_before_observed_activity() {
    let (_dir, manifest) = native_fixture(
        "claude",
        serde_json::json!([
            {"type":"system","subtype":"init","session_id":"12345678-1234-1234-1234-123456789abc","mcp_servers":[{"name":"oppa","status":"failed"}]}
        ]),
    );
    let mut req = request();
    req.mcp = Some(broker());
    let outcome = run(&manifest, &req).await.unwrap();
    assert_eq!(
        outcome.failure.as_ref().unwrap().category,
        FailureCategory::Unavailable
    );
    assert_eq!(outcome.activity, Activity::None);
    assert!(outcome.telemetry_complete);
    assert!(outcome.answer.is_none());
}

#[tokio::test]
async fn harness_uses_only_an_explicit_existing_absolute_working_directory() {
    let (_dir, manifest) = fixture("import os,json,sys\njson.load(sys.stdin)\nprint(json.dumps({'type':'answer','text':os.getcwd()}))\nprint(json.dumps({'type':'completed'}))\n");
    let cwd = tempfile::tempdir().unwrap();
    let mut req = request();
    req.working_directory = Some(cwd.path().display().to_string());
    let outcome = run(&manifest, &req).await.unwrap();
    assert!(outcome.failure.is_none());
    assert_eq!(
        Path::new(outcome.answer.as_deref().unwrap())
            .canonicalize()
            .unwrap(),
        cwd.path().canonicalize().unwrap()
    );
    for invalid in ["relative/directory", "/nonexistent/synthetic-directory"] {
        req.working_directory = Some(invalid.into());
        assert!(run(&manifest, &req).await.is_err());
        for provider in ["codex", "claude"] {
            assert_eq!(
                adapter_command(provider, serde_json::to_value(&req).unwrap())
                    .status
                    .code(),
                Some(2)
            );
        }
    }
}

#[tokio::test]
async fn invalid_session_is_distinct_complete_pre_effect_evidence() {
    let (_directory, manifest) = fixture(&emit_fixture(serde_json::json!([
        {"type":"failure","category":"session_invalid","diagnostic":"Synthetic session no longer exists."},
        {"type":"completed"}
    ])));
    let mut req = request();
    req.session = Some("expired-synthetic-session".into());
    let outcome = run(&manifest, &req).await.unwrap();
    assert_eq!(
        outcome.failure.unwrap().category,
        FailureCategory::SessionInvalid
    );
    assert_eq!(outcome.activity, Activity::None);
    assert!(outcome.telemetry_complete);
    assert!(outcome.answer.is_none());
}

#[tokio::test]
async fn late_stream_exit_stderr_and_timeout_faults_retain_answer_only_as_evidence() {
    let prefix = "import json,sys,time\njson.load(sys.stdin)\nprint(json.dumps({'type':'activity','kind':'read'}),flush=True)\nprint(json.dumps({'type':'answer','text':'Observed synthetic answer.'}),flush=True)\n";
    for (tail, expected, timeout) in [
        ("print('malformed late frame',flush=True)\n", FailureCategory::Protocol, 5),
        ("print(json.dumps({'type':'completed'}),flush=True)\nsys.exit(9)\n", FailureCategory::Protocol, 5),
        ("print(json.dumps({'type':'completed'}),flush=True)\ntime.sleep(0.1)\nsys.stderr.write('x'*(70*1024));sys.stderr.flush()\n", FailureCategory::Protocol, 5),
        ("time.sleep(30)\n", FailureCategory::Timeout, 1),
    ] {
        let (_directory, mut manifest) = fixture(&format!("{prefix}{tail}"));
        manifest.timeout_seconds = timeout;
        let outcome = run(&manifest, &request()).await.unwrap();
        assert_eq!(outcome.answer.as_deref(), Some("Observed synthetic answer."));
        assert_eq!(outcome.failure.unwrap().category, expected);
        assert_eq!(outcome.activity, Activity::Read);
        assert!(!outcome.telemetry_complete);
    }
}

const INHERITED_CHILD: &str = r#"
import json,os,pathlib,subprocess,sys,time
json.load(sys.stdin)
pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
subprocess.Popen([sys.executable,'-c',
    "import os,pathlib,sys,time;pathlib.Path(sys.argv[1]).write_text(str(os.getpid()));time.sleep(1);pathlib.Path(sys.argv[2]).write_text('unexpected descendant effect')",
    sys.argv[2],sys.argv[3]])
while not pathlib.Path(sys.argv[2]).exists(): time.sleep(0.005)
if sys.argv[4] == 'success':
    print(json.dumps({'type':'answer','text':'Completed synthetic answer.'}),flush=True)
    print(json.dumps({'type':'completed'}),flush=True)
else:
    if sys.argv[4] == 'observed':
        print(json.dumps({'type':'session','handle':'synthetic-cancelled-session'}),flush=True)
        print(json.dumps({'type':'activity','kind':'effect'}),flush=True)
        print(json.dumps({'type':'answer','text':'Partial diagnostic answer.'}),flush=True)
    time.sleep(30)
"#;

fn inherited_child_fixture(mode: &str) -> (TempDir, AgentManifest) {
    let (directory, mut manifest) = fixture(INHERITED_CHILD);
    manifest.command.extend([
        directory.path().join("parent.pid").display().to_string(),
        directory.path().join("child.pid").display().to_string(),
        directory.path().join("effect").display().to_string(),
        mode.into(),
    ]);
    (directory, manifest)
}

#[tokio::test]
async fn successful_leader_exit_cleans_inherited_pipes_before_reaping() {
    let (directory, manifest) = inherited_child_fixture("success");
    let outcome = run(&manifest, &request()).await.unwrap();
    assert!(outcome.failure.is_none(), "{:?}", outcome.failure);
    assert!(outcome.telemetry_complete);
    assert_eq!(
        outcome.answer.as_deref(),
        Some("Completed synthetic answer.")
    );
    assert!(
        directory.path().join("child.pid").exists(),
        "descendant never started"
    );
    let parent: i32 = fs::read_to_string(directory.path().join("parent.pid"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        unsafe { libc::kill(parent, 0) },
        -1,
        "leader was not reaped"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(
        !directory.path().join("effect").exists(),
        "normal completion left a live descendant"
    );
}

#[tokio::test]
async fn cancellation_kills_owned_group_before_tokio_reaps_the_leader() {
    let (directory, manifest) = inherited_child_fixture("wait");
    let task = tokio::spawn(async move { run(&manifest, &request()).await });
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while !directory.path().join("child.pid").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let parent: i32 = fs::read_to_string(directory.path().join("parent.pid"))
        .unwrap()
        .parse()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while unsafe { libc::kill(parent, 0) } == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("cancelled adapter was not reaped");
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(
        !directory.path().join("effect").exists(),
        "cancelled run left a live descendant"
    );
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn rapid_invalid_exits_remain_protocol_faults_during_darwin_exit_transition() {
    // Invalid output can finish the pipe reader while Darwin has removed the
    // process from signal delivery but has not yet made it waitable. Exercise
    // that boundary repeatedly with real owned children, not fabricated PIDs.
    for _ in 0..8 {
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..8 {
            tasks.spawn(async {
                let (directory, mut manifest) = fixture(
                    "import json,os,pathlib,sys\njson.load(sys.stdin)\npathlib.Path(sys.argv[1]).write_text(str(os.getpid()))\nprint(json.dumps({'type':'answer','text':'Synthetic observed answer.'}),flush=True)\nprint('invalid late frame',flush=True)\nos._exit(0)\n",
                );
                let pid_file = directory.path().join("child.pid");
                manifest.command.push(pid_file.display().to_string());
                let outcome = run(&manifest, &request()).await
                    .expect("terminal process transition must not become a cleanup error");
                assert_eq!(outcome.failure.unwrap().category, FailureCategory::Protocol);
                assert_eq!(outcome.answer.as_deref(), Some("Synthetic observed answer."));
                assert!(!outcome.telemetry_complete);
                let pid: i32 = fs::read_to_string(pid_file).unwrap().parse().unwrap();
                assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "child was not reaped");
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    }
}

#[tokio::test]
async fn ready_stop_prevents_launch_and_preserves_host_reason() {
    let (directory, mut manifest) =
        fixture("import pathlib,sys\npathlib.Path(sys.argv[1]).write_text('unexpected launch')\n");
    let marker = directory.path().join("launched");
    manifest.command.push(marker.display().to_string());
    for reason in [
        StopReason::Requested,
        StopReason::AuthorityChanged,
        StopReason::MonitorUnavailable,
    ] {
        let report = run_controlled(&manifest, &request(), std::future::ready(reason))
            .await
            .unwrap();
        assert_eq!(report.stop, Some(reason));
        assert_eq!(report.cleanup, CleanupStatus::NotStarted);
        assert_eq!(report.outcome.activity, Activity::None);
        assert_eq!(
            report.outcome.failure.unwrap().category,
            FailureCategory::Failed
        );
        assert!(!marker.exists());
    }
}

#[tokio::test]
async fn controlled_stop_retains_partial_observations_and_confirms_descendants_dead() {
    let (directory, manifest) = inherited_child_fixture("observed");
    let stop = async {
        while !directory.path().join("child.pid").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // Let the already emitted normalized observations reach the reader.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        StopReason::Requested
    };
    let report = run_controlled(&manifest, &request(), stop).await.unwrap();
    assert_eq!(report.stop, Some(StopReason::Requested));
    assert_eq!(report.cleanup, CleanupStatus::Confirmed);
    assert_eq!(report.outcome.activity, Activity::Effect);
    assert_eq!(
        report.outcome.session.as_deref(),
        Some("synthetic-cancelled-session")
    );
    assert_eq!(
        report.outcome.answer.as_deref(),
        Some("Partial diagnostic answer.")
    );
    assert_eq!(
        report.outcome.failure.unwrap().category,
        FailureCategory::Failed
    );
    assert!(!report.outcome.telemetry_complete);
    let parent: i32 = fs::read_to_string(directory.path().join("parent.pid"))
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(
        unsafe { libc::kill(parent, 0) },
        -1,
        "leader was not reaped before return"
    );
    let descendant = fs::read_to_string(directory.path().join("child.pid")).unwrap();
    let status = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", descendant.trim()])
        .output()
        .unwrap();
    // Dead is gone, a zombie, or exiting: macOS can show a killed process as
    // "?E" while the kernel tears it down, and that failed a full-suite run.
    let state = String::from_utf8(status.stdout).unwrap();
    let state = state.trim();
    assert!(
        state.is_empty() || state.starts_with('Z') || state.contains('E'),
        "descendant still live at return: {state}"
    );
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(!directory.path().join("effect").exists());
}

#[tokio::test]
async fn controlled_completion_confirms_cleanup_without_inventing_a_stop() {
    let (directory, manifest) = inherited_child_fixture("success");
    let report = run_controlled(&manifest, &request(), std::future::pending())
        .await
        .unwrap();
    assert_eq!(report.stop, None);
    assert_eq!(report.cleanup, CleanupStatus::Confirmed);
    assert!(report.outcome.failure.is_none());
    assert!(report.outcome.telemetry_complete);
    assert!(directory.path().join("child.pid").exists());
}

#[tokio::test]
async fn lost_child_ownership_returns_unconfirmed_with_partial_outcome() {
    let (directory, mut manifest) = fixture(
        "import json,os,pathlib,sys,time\njson.load(sys.stdin)\npathlib.Path(sys.argv[1]).write_text(str(os.getpid()))\nprint(json.dumps({'type':'activity','kind':'effect'}),flush=True)\nprint(json.dumps({'type':'answer','text':'Observed before ownership loss.'}),flush=True)\ntime.sleep(0.2)\nos._exit(0)\n",
    );
    let pid_file = directory.path().join("parent.pid");
    manifest.command.push(pid_file.display().to_string());
    let stop = async {
        while !pid_file.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let pid: i32 = fs::read_to_string(&pid_file).unwrap().parse().unwrap();
        // Deliberately violate exclusive Child ownership in this self-owned
        // fixture. Blocking this single-thread test runtime makes the foreign
        // reap deterministic, before the harness's next waitid observation.
        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
        StopReason::Requested
    };
    let report = run_controlled(&manifest, &request(), stop).await.unwrap();
    assert!(matches!(report.cleanup, CleanupStatus::Unconfirmed { .. }));
    assert_eq!(report.stop, Some(StopReason::Requested));
    assert_eq!(report.outcome.activity, Activity::Effect);
    assert_eq!(
        report.outcome.answer.as_deref(),
        Some("Observed before ownership loss.")
    );
    assert_eq!(
        report.outcome.failure.unwrap().category,
        FailureCategory::Failed
    );
    assert!(!report.outcome.telemetry_complete);
}

#[test]
fn standing_instructions_stay_out_of_the_transcript_on_every_harness() {
    let mut req = request();
    req.instructions = Some("Synthetic standing manual.\nSecond line.".into());
    let fresh = serde_json::to_value(&req).unwrap();
    req.session = Some("00000000-0000-4000-8000-000000000001".into());
    let resumed = serde_json::to_value(&req).unwrap();
    let args = |provider: &str, value: &serde_json::Value| -> Vec<String> {
        let output = adapter_command(provider, value.clone());
        assert!(output.status.success());
        serde_json::from_slice(&output.stdout).unwrap()
    };
    // Claude's appended system prompt is resent each call and never stored.
    for value in [&fresh, &resumed] {
        let claude = args("claude", value);
        let at = claude
            .iter()
            .position(|arg| arg == "--append-system-prompt")
            .unwrap();
        assert_eq!(claude[at + 1], "Synthetic standing manual.\nSecond line.");
    }
    // Codex records developer instructions in a new thread only.
    let developer = |args: &[String]| {
        args.windows(2)
            .filter(|pair| pair[0] == "-c" && pair[1].starts_with("developer_instructions="))
            .map(|pair| pair[1].clone())
            .collect::<Vec<_>>()
    };
    let fresh_codex = developer(&args("codex", &fresh));
    assert_eq!(fresh_codex.len(), 1);
    let parsed: toml::Value = toml::from_str(&fresh_codex[0]).unwrap();
    assert_eq!(
        parsed["developer_instructions"].as_str(),
        Some("Synthetic standing manual.\nSecond line.")
    );
    assert!(developer(&args("codex", &resumed)).is_empty());
    let mut oversized = fresh.clone();
    oversized["instructions"] = serde_json::json!("x".repeat(70 * 1024));
    for provider in ["codex", "claude"] {
        assert_eq!(
            adapter_command(provider, oversized.clone()).status.code(),
            Some(2)
        );
    }
}

#[tokio::test]
async fn full_access_routes_every_native_permission_to_the_broker() {
    // C-5, S-3: an owner run at connectors or
    // computer access is the harness as the owner set it up (their accounts,
    // MCP servers, plugins, settings and memory, the browser when it is on, a
    // high-effort subagent), and the host answers each native permission prompt.
    let arg = |args: &[String], flag: &str| -> Option<String> {
        args.iter()
            .position(|x| x == flag)
            .map(|i| args[i + 1].clone())
    };
    let mut req = request();
    req.mcp = Some(McpServer {
        write_tools: vec!["mac.reminders_create".into()],
        ..broker()
    });
    req.model = Some("synthetic-model-1".into());
    req.effort = Some("high".into());
    for (access, browser) in [
        (Access::Connectors, true),
        (Access::Computer, true),
        (Access::Computer, false),
    ] {
        req.access = access;
        req.browser = browser;
        let value = serde_json::to_value(&req).unwrap();
        let claude: Vec<String> =
            serde_json::from_slice(&adapter_command("claude", value.clone()).stdout).unwrap();
        for absent in [
            "--restricted",
            "--strict-mcp-config",
            "--settings",
            "--disallowedTools",
            "--safe-mode",
        ] {
            assert!(!claude.contains(&absent.into()), "{access:?} {absent}");
        }
        assert!(arg(&claude, "--allowedTools")
            .unwrap()
            .split(',')
            .any(|t| t == "mcp__oppa__mac_reminders_create"));
        assert_eq!(
            arg(&claude, "--permission-prompt-tool").as_deref(),
            Some("mcp__oppa__permission_decide")
        );
        // An owner's bypassPermissions setting cannot skip the host.
        assert_eq!(
            arg(&claude, "--permission-mode").as_deref(),
            Some("default")
        );
        assert_eq!(
            arg(&claude, "--model").as_deref(),
            Some("synthetic-model-1")
        );
        assert_eq!(arg(&claude, "--effort").as_deref(), Some("high"));
        assert_eq!(claude.contains(&"--chrome".into()), browser, "{access:?}");
        assert_eq!(claude.contains(&"--no-chrome".into()), !browser);
        let agents: serde_json::Value =
            serde_json::from_str(&arg(&claude, "--agents").unwrap()).unwrap();
        assert_eq!(agents["deep-worker"]["effort"], "high");
        let config: serde_json::Value =
            serde_json::from_str(&arg(&claude, "--mcp-config").unwrap()).unwrap();
        assert_eq!(config["mcpServers"]["oppa"]["command"], broker().command);
        // Connectors access has the web and subagents but no shell or files.
        assert_eq!(
            arg(&claude, "--tools").as_deref(),
            (access == Access::Connectors).then_some("WebSearch,WebFetch,Task")
        );
        assert_eq!(
            arg(&claude, "--add-dir").is_some(),
            access == Access::Computer
        );
        let codex: Vec<String> =
            serde_json::from_slice(&adapter_command("codex", value.clone()).stdout).unwrap();
        let disabled: Vec<&str> = codex
            .iter()
            .enumerate()
            .filter(|(_, x)| *x == "--disable")
            .map(|(i, _)| codex[i + 1].as_str())
            .collect();
        for kept in ["apps", "memories", "multi_agent", "plugins", "hooks"] {
            assert!(!disabled.contains(&kept), "{access:?} {kept}");
        }
        assert!(!codex.contains(&"--ignore-user-config".into()));
        assert!(!codex.contains(&"--ignore-rules".into()));
        assert_eq!(
            disabled.contains(&"shell_tool"),
            access == Access::Connectors
        );
        assert_eq!(disabled.contains(&"browser_use"), !browser);
        assert_eq!(
            codex.contains(&"--dangerously-bypass-approvals-and-sandbox".into()),
            access == Access::Computer
        );
        assert_eq!(arg(&codex, "-m").as_deref(), Some("synthetic-model-1"));
        assert!(codex.contains(&"model_reasoning_effort=\"high\"".into()));
        let mut unbrokered = value.clone();
        unbrokered.as_object_mut().unwrap().remove("mcp");
        for provider in ["claude", "codex"] {
            assert_eq!(
                adapter_command(provider, unbrokered.clone()).status.code(),
                Some(2),
                "{provider}: full access needs the permission tool"
            );
        }
    }
    req.model = None;
    req.effort = None;
    // A read run carries only the connector writes its broker offers, such as
    // an adult's may: they are allowed as broker tools, with no permission
    // tool and nothing native that writes.
    req.access = Access::Read;
    let claude: Vec<String> = serde_json::from_slice(
        &adapter_command("claude", serde_json::to_value(&req).unwrap()).stdout,
    )
    .unwrap();
    assert!(arg(&claude, "--allowedTools")
        .unwrap()
        .split(',')
        .any(|t| t == "mcp__oppa__mac_reminders_create"));
    assert!(arg(&claude, "--permission-prompt-tool").is_none());
    let codex: Vec<String> = serde_json::from_slice(
        &adapter_command("codex", serde_json::to_value(&req).unwrap()).stdout,
    )
    .unwrap();
    assert!(
        codex.iter().any(|x| x.contains("mac.reminders_create")),
        "{codex:?}"
    );
    req.mcp = Some(broker());
    let codex: Vec<String> = serde_json::from_slice(
        &adapter_command("codex", serde_json::to_value(&req).unwrap()).stdout,
    )
    .unwrap();
    assert!(codex
        .windows(2)
        .any(|w| w[0] == "--disable" && w[1] == "apps"));

    // A broker connector write is an effect the host journals; an account write
    // that a read run rejects is an unmediated action here.
    req.access = Access::Connectors;
    req.mcp = Some(McpServer {
        write_tools: vec!["mac.reminders_create".into()],
        ..broker()
    });
    let init = json!({"type":"system","subtype":"init","session_id":"12345678-1234-1234-1234-123456789abc",
        "model":"claude-other",
        "mcp_servers":[{"name":"oppa","status":"connected"},{"name":"claude.ai Gmail","status":"connected"}],
        "tools":["mcp__oppa__permission_decide","mcp__claude_ai_Gmail__send_message"]});
    let add = json!({"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_add","name":"mcp__oppa__mac_reminders_create"}]}});
    let send = json!({"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_send","name":"mcp__claude_ai_Gmail__send_message"}]}});
    let sent = json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_send","content":"synthetic","is_error":false}]}});
    let done =
        json!({"type":"result","subtype":"success","is_error":false,"result":"Synthetic answer."});
    for (events, expected) in [
        (json!([init, add, done]), Activity::Effect),
        (json!([init, add, send, sent, done]), Activity::Unmediated),
    ] {
        let (_dir, manifest) = native_fixture("claude", events);
        let outcome = run(&manifest, &req).await.unwrap();
        assert!(outcome.failure.is_none(), "{:?}", outcome.failure);
        assert_eq!(outcome.activity, expected);
    }
}

#[tokio::test]
async fn a_turns_attached_files_are_the_only_files_claude_may_read() {
    // G-6, C-5: at read and connectors access Claude gets the Read tool for
    // the folder of this turn's attachments and no other file tool, and
    // reading counts as a read, never an effect.
    let arg = |args: &[String], flag: &str| -> Option<String> {
        args.iter()
            .position(|x| x == flag)
            .map(|i| args[i + 1].clone())
    };
    let mut req = request();
    req.mcp = Some(broker());
    req.readable_dir = Some("/synthetic/runtime/attachments/run-1".into());
    for access in [Access::Read, Access::Connectors] {
        req.access = access;
        let claude: Vec<String> = serde_json::from_slice(
            &adapter_command("claude", serde_json::to_value(&req).unwrap()).stdout,
        )
        .unwrap();
        // Connectors access also has the web and subagents, never a shell.
        let tools = match access {
            Access::Read => "Read",
            _ => "WebSearch,WebFetch,Task,Read",
        };
        assert_eq!(
            arg(&claude, "--tools").as_deref(),
            Some(tools),
            "{access:?}"
        );
        assert_eq!(
            arg(&claude, "--add-dir").as_deref(),
            Some("/synthetic/runtime/attachments/run-1")
        );
        assert!(arg(&claude, "--allowedTools")
            .unwrap()
            .split(',')
            .any(|t| t == "Read"));
    }
    let mut relative = serde_json::to_value(&req).unwrap();
    relative["readable_dir"] = json!("attachments/run-1");
    assert_eq!(adapter_command("claude", relative).status.code(), Some(2));
    req.readable_dir = None;
    req.access = Access::Read;
    let claude: Vec<String> = serde_json::from_slice(
        &adapter_command("claude", serde_json::to_value(&req).unwrap()).stdout,
    )
    .unwrap();
    assert_eq!(arg(&claude, "--tools").as_deref(), Some(""));

    // A Read is graded by its result: one that succeeded inside the folder is a
    // read, one that succeeded outside it means confinement failed, one the
    // harness's permissions denied was refused, and one that failed otherwise
    // read nothing. A PDF comes back whole, as base64, twice in one event, as the
    // CLI sends it: here the largest file a turn may open.
    // A real folder, as the host makes it: the adapter keeps the CLI's temporary files there.
    let folder = tempfile::tempdir().unwrap();
    let run_1 = folder.path().join("run-1");
    fs::create_dir(&run_1).unwrap();
    let run_1 = run_1.to_str().unwrap().to_owned();
    let inside = format!("{run_1}/7-0.pdf");
    let missing = format!("{run_1}/missing.txt");
    let outside = "/synthetic/elsewhere/secret.txt";
    let init = json!({"type":"system","subtype":"init","session_id":"12345678-1234-1234-1234-123456789abc"});
    let reading = |file: &str| {
        json!({"type":"assistant","message":{"content":[{"type":"tool_use","id":"toolu_read","name":"Read",
            "input":{"file_path":file}}]}})
    };
    for (file, result, activity) in [
        (inside.as_str(), "opened", Activity::Read),
        (outside, "opened", Activity::Unmediated),
        (outside, "denied", Activity::None),
        (missing.as_str(), "missing", Activity::None),
    ] {
        let content = if file == inside {
            json!([{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"@pdf@"}}])
        } else {
            json!("synthetic")
        };
        let denials = if result == "denied" {
            json!([{"tool_name":"Read","tool_use_id":"toolu_read","tool_input":{"file_path":file}}])
        } else {
            json!([])
        };
        let (_dir, manifest) = native_fixture(
            "claude",
            json!([
                init,
                reading(file),
                {"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"toolu_read",
                 "is_error":result != "opened","content":content}]},"tool_use_result":content},
                {"type":"result","subtype":"success","is_error":false,"result":"Synthetic answer.",
                 "permission_denials":denials}
            ]),
        );
        let mut attached = request();
        attached.readable_dir = Some(run_1.clone());
        let outcome = run(&manifest, &attached).await.unwrap();
        assert!(outcome.failure.is_none(), "{:?}", outcome.failure);
        assert_eq!(outcome.activity, activity, "{file} {result}");
        // The file's name alone is kept, as read or refused; a failed Read is neither.
        let name = vec![file.rsplit('/').next().unwrap().to_owned()];
        let expected = match result {
            "opened" => (name, vec![]),
            "denied" => (vec![], name),
            _ => (vec![], vec![]),
        };
        assert_eq!(
            (outcome.files_read, outcome.files_refused),
            expected,
            "{file} {result}"
        );
    }
    // With attached files the CLI keeps its temporary files in a folder inside theirs,
    // which goes with them; without, Read is off and the CLI keeps its default.
    for readable in [Some(run_1.clone()), None] {
        let (dir, manifest) = native_fixture(
            "claude",
            json!([init, {"type":"result","subtype":"success",
            "is_error":false,"result":"Synthetic answer."}]),
        );
        let mut attached = request();
        attached.readable_dir = readable.clone();
        run(&manifest, &attached).await.unwrap();
        let seen: (Option<String>, bool) = serde_json::from_str(
            fs::read_to_string(dir.path().join("temporary.txt"))
                .unwrap()
                .trim(),
        )
        .unwrap();
        let expected = readable.map(|folder| format!("{folder}/.tmp"));
        assert_eq!(
            (seen.0.clone(), seen.1),
            (expected.clone(), expected.is_some())
        );
    }
    // A Read that never answered leaves the run's telemetry incomplete, however
    // the run ends: a quota stop with one outstanding is no clean stop for
    // another harness to take over from.
    for end in [
        json!({"type":"result","subtype":"success","is_error":false,"result":"Synthetic answer."}),
        json!({"type":"exit","code":1,"stderr":"Claude AI usage limit reached"}),
    ] {
        let (_dir, manifest) = native_fixture("claude", json!([init, reading(&inside), end]));
        let mut attached = request();
        attached.readable_dir = Some(run_1.clone());
        let outcome = run(&manifest, &attached).await.unwrap();
        assert_eq!(
            outcome.failure.map(|f| f.category),
            Some(FailureCategory::Protocol),
            "{end}"
        );
        assert!(!outcome.telemetry_complete);
    }
    // Every file a turn may open, each at the largest size, fits one run.
    let mut events = vec![init.clone()];
    for page in 0..8 {
        let id = format!("toolu_{page}");
        events.push(json!({"type":"assistant","message":{"content":[{"type":"tool_use","id":id,"name":"Read",
            "input":{"file_path":format!("{run_1}/{page}-0.pdf")}}]}}));
        let pdf = json!([{"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"@pdf@"}}]);
        events.push(
            json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":id,
            "content":pdf}]},"tool_use_result":pdf}),
        );
    }
    events.push(
        json!({"type":"result","subtype":"success","is_error":false,"result":"Synthetic answer."}),
    );
    let (_dir, mut manifest) = native_fixture("claude", json!(events));
    // About 427 MiB of output: room for a slow machine.
    manifest.timeout_seconds = 120;
    let mut attached = request();
    attached.readable_dir = Some(run_1.clone());
    let outcome = run(&manifest, &attached).await.unwrap();
    assert!(outcome.failure.is_none(), "{:?}", outcome.failure);
    assert_eq!(outcome.files_read.len(), 8);
    // However many reads a run makes, and however long a name, the run stands
    // and its record stays bounded: each name once, cut on a character boundary.
    let read = |id: usize, file: String, failed: bool| {
        [
            json!({"type":"assistant","message":{"content":[{"type":"tool_use","id":format!("toolu_{id}"),
                "name":"Read","input":{"file_path":file}}]}}),
            json!({"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":format!("toolu_{id}"),
                "is_error":failed,"content":"synthetic"}]}}),
        ]
    };
    let mut events = vec![
        json!({"type":"system","subtype":"init","session_id":"12345678-1234-1234-1234-123456789abc"}),
    ];
    for page in 0..70 {
        let file = format!("{run_1}/{page}-0.jpg");
        events.extend(read(page, file, false));
    }
    events.extend(read(70, format!("{run_1}/0-0.jpg"), false));
    events.extend(read(
        71,
        format!("/synthetic/elsewhere/{}", "é".repeat(300)),
        true,
    ));
    events.push(
        json!({"type":"result","subtype":"success","is_error":false,"result":"Synthetic answer.",
        "permission_denials":[{"tool_name":"Read","tool_use_id":"toolu_71"}]}),
    );
    let (_dir, manifest) = native_fixture("claude", json!(events));
    let mut reading = request();
    reading.readable_dir = Some(run_1.clone());
    let outcome = run(&manifest, &reading).await.unwrap();
    assert!(outcome.failure.is_none(), "{:?}", outcome.failure);
    assert_eq!(
        outcome.files_read,
        (0..64)
            .map(|page| format!("{page}-0.jpg"))
            .collect::<Vec<_>>()
    );
    assert_eq!(outcome.files_refused, ["é".repeat(127)]);
}

#[test]
fn no_bundled_adapter_shadows_a_python_module() {
    // Incident 2026-10-11: a connector named secrets.py, installed beside the
    // web channel adapter, replaced Python's own secrets module for it, and the
    // web channel could not start. Every bundled adapter shares one folder, so
    // none may share a name with a module Python already finds.
    let names = oppa::harness::HARNESS_ADAPTERS
        .iter()
        .chain(oppa::harness::CHANNEL_ADAPTERS.iter())
        .chain(oppa::harness::CONNECTOR_ADAPTERS.iter())
        .filter_map(|(name, _)| name.strip_suffix(".py"));
    for name in names {
        let found = std::process::Command::new("python3")
            .args(["-I", "-c", "import importlib.util,sys; sys.exit(importlib.util.find_spec(sys.argv[1]) is not None)", name])
            .current_dir(std::env::temp_dir())
            .status()
            .unwrap();
        assert!(found.success(), "{name}.py shadows a Python module");
    }
}
