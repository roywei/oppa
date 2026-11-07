use oppa::{
    broker::{Broker, BrokerLaunch, RunGrant, TaskAccess},
    config::{Config, Paths},
    harness::{Access, AgentManifest},
    mcp::{serve, CURRENT_VERSION, LEGACY_VERSION, MAX_MESSAGE_BYTES, MAX_RESPONSE_BYTES},
    store::{InboundEvent, Store},
    vault::{Edit, Provenance, Vault},
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    io::Cursor,
    path::{Path, PathBuf},
};
use tempfile::TempDir;

struct Fixture {
    _root: TempDir,
    broker: Broker,
    vault: PathBuf,
    journal: PathBuf,
}

fn fixture() -> Fixture {
    fixture_with(Default::default())
}

fn fixture_with(access: Access) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let paths = Paths::new(
        root.path().join("runtime"),
        Some(root.path().join("private")),
    )
    .unwrap();
    paths.prepare().unwrap();
    fs::write(
        paths.config_file(),
        toml::to_string(&Config {
            access: Default::default(),
            version: 1,
            connectors: vec![],
            sessions: Default::default(),
            source: None,
            channels: vec![],
            people: vec![],
            owner: "synthetic-owner".into(),
            agents: vec![AgentManifest {
                id: "synthetic".into(),
                family: "fixture".into(),
                command: vec!["/never/launch".into()],
                timeout_seconds: 30,
                task_timeout_seconds: 30,
                model: None,
                effort: None,
            }],
            groups: vec![],
            heartbeat: None,
            browser: false,
            memory_sync: None,
        })
        .unwrap(),
    )
    .unwrap();
    let database = paths.database();
    let vault_path = paths.vault();
    let journal = root.path().join("journal");
    let store = Store::open(&database).unwrap();
    store
        .create_conversation("chat", "console", "local", "chat", "synthetic-policy")
        .unwrap();
    let event = store
        .append_inbound(&InboundEvent {
            conversation: "chat".into(),
            native_id: "native-1".into(),
            author: "synthetic-owner".into(),
            body: "A synthetic school pickup note.".into(),
            kind: "message".into(),
            ..Default::default()
        })
        .unwrap();
    Vault::init(&vault_path).unwrap();
    let broker = Broker::open(BrokerLaunch {
        private_dir: paths.private.clone(),
        database,
        vault: vault_path.clone(),
        journal: journal.clone(),
        grant: RunGrant {
            access,
            context: "synthetic-context".into(),
            requester: "synthetic-owner".into(),
            role: oppa::config::Role::Owner,
            run_id: "synthetic-run".into(),
            input_seq: event.seq,
            conversations: vec!["chat".into()],
            conversation_scopes: vec![],
            read_scopes: vec!["*".into()],
            write_scopes: vec!["*".into()],
            task_access: TaskAccess::None,
            eligible_agents: vec![],
            expires_at: chrono::Utc::now().timestamp() + 300,
            wakes: false,
        },
    })
    .unwrap();
    Fixture {
        _root: root,
        broker,
        vault: vault_path,
        journal,
    }
}

fn messages(fixture: &Fixture, requests: Vec<Value>) -> Vec<Value> {
    let mut input = Vec::new();
    for request in requests {
        serde_json::to_writer(&mut input, &request).unwrap();
        input.push(b'\n');
    }
    raw(fixture, input)
}
fn raw(fixture: &Fixture, input: Vec<u8>) -> Vec<Value> {
    let mut output = Vec::new();
    serve(Cursor::new(input), &mut output, &fixture.broker).unwrap();
    let text = String::from_utf8(output).unwrap();
    text.lines()
        .map(|line| {
            assert!(line.len() <= MAX_RESPONSE_BYTES);
            serde_json::from_str(line).unwrap()
        })
        .collect()
}
fn initialize() -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":LEGACY_VERSION,"capabilities":{},"clientInfo":{"name":"synthetic-client","version":"1"}}})
}
fn initialized() -> Value {
    json!({"jsonrpc":"2.0","method":"notifications/initialized"})
}
fn modern(id: Value, method: &str, mut params: Value) -> Value {
    params["_meta"] = json!({
        "io.modelcontextprotocol/protocolVersion":CURRENT_VERSION,
        "io.modelcontextprotocol/clientCapabilities":{},
        "io.modelcontextprotocol/clientInfo":{"name":"synthetic-client","version":"1"}
    });
    json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
}
fn journal_files(fixture: &Fixture) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, files);
            } else {
                files.insert(
                    path.strip_prefix(root).unwrap().into(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = BTreeMap::new();
    walk(&fixture.journal, &fixture.journal, &mut files);
    files
}

#[test]
fn legacy_requires_complete_handshake_and_lists_only_supported_tools() {
    let fixture = fixture();
    let replies = messages(
        &fixture,
        vec![
            json!({"jsonrpc":"2.0","id":0,"method":"tools/list"}),
            initialize(),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            initialized(),
            json!({"jsonrpc":"2.0","id":3,"method":"tools/list","params":{}}),
        ],
    );
    assert_eq!(replies.len(), 4);
    assert!(replies[0]["error"].is_object());
    assert_eq!(replies[1]["result"]["protocolVersion"], LEGACY_VERSION);
    assert!(replies[2]["error"].is_object());
    let result = &replies[3]["result"];
    assert!(result.get("resultType").is_none());
    assert_eq!(
        result["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "history.conversations",
            "history.read",
            "history.search",
            "history.turn",
            "memory.commit",
            "memory.forget",
            "memory.read",
            "memory.search",
            "people.list"
        ]
    );
}

#[test]
fn modern_discovery_and_direct_tool_requests_need_no_handshake() {
    let fixture = fixture();
    let replies = messages(
        &fixture,
        vec![
            modern(json!("discover"), "server/discover", json!({})),
            modern(json!("catalog"), "tools/list", json!({})),
            modern(
                json!("read"),
                "tools/call",
                json!({"name":"history.read","arguments":{"context":"synthetic-context","conversation":"chat","after_seq":0,"limit":10}}),
            ),
        ],
    );
    assert_eq!(replies.len(), 3);
    assert_eq!(
        replies[0]["result"]["supportedVersions"],
        json!([CURRENT_VERSION, LEGACY_VERSION])
    );
    for reply in &replies {
        assert_eq!(reply["result"]["resultType"], "complete");
        assert_eq!(
            reply["result"]["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
            "oppa"
        );
    }
    assert_eq!(replies[1]["result"]["cacheScope"], "private");
    assert_eq!(replies[1]["result"]["ttlMs"], 0);
    assert_eq!(replies[2]["result"]["isError"], false);
    let textual: Value =
        serde_json::from_str(replies[2]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(textual, replies[2]["result"]["structuredContent"]);
    let direct = messages(&fixture, vec![modern(json!(7), "tools/list", json!({}))]);
    assert_eq!(direct[0]["result"]["resultType"], "complete");
}

#[test]
fn modern_metadata_is_validated_per_request_and_does_not_identify_the_actor() {
    let fixture = fixture();
    let mut unsupported = modern(json!(2), "tools/list", json!({}));
    unsupported["params"]["_meta"]["io.modelcontextprotocol/protocolVersion"] = json!("2099-01-01");
    let replies = messages(
        &fixture,
        vec![
            modern(json!(1), "server/discover", json!({})),
            unsupported,
            json!({"jsonrpc":"2.0","id":3,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":CURRENT_VERSION}}}),
            json!({"jsonrpc":"2.0","id":4,"method":"tools/list","params":{}}),
            modern(
                json!(5),
                "tools/call",
                json!({"name":"memory.search","arguments":{"context":"wrong-context","query":"pickup"}}),
            ),
        ],
    );
    assert_eq!(replies[1]["error"]["code"], -32022);
    assert_eq!(
        replies[1]["error"]["data"]["supported"],
        json!([CURRENT_VERSION, LEGACY_VERSION])
    );
    assert_eq!(replies[2]["error"]["code"], -32602);
    assert!(
        replies[3]["error"].is_object(),
        "discovery must not make missing metadata valid"
    );
    assert_eq!(replies[4]["result"]["isError"], true);
}

#[test]
fn invalid_frames_and_tool_requests_never_reach_broker_storage() {
    let fixture = fixture();
    let revision = Vault::open(&fixture.vault).unwrap().revision().unwrap();
    let before = journal_files(&fixture);
    let mut input = b"not-json\n[]\n".to_vec();
    let requests = vec![
        json!({"jsonrpc":"2.0","id":null,"method":"tools/list"}),
        json!({"jsonrpc":"2.0","id":1.5,"method":"tools/list"}),
        json!({"jsonrpc":"2.0","id":[],"method":"tools/list"}),
        json!({"jsonrpc":"2.0","id":false,"method":"tools/list"}),
        modern(
            json!(1),
            "tools/call",
            json!({"name":"memory.commit","arguments":[]}),
        ),
        modern(
            json!(2),
            "tools/call",
            json!({"name":"not-a-tool","arguments":{}}),
        ),
        modern(json!(3), "unknown/method", json!({})),
    ];
    for request in requests {
        serde_json::to_writer(&mut input, &request).unwrap();
        input.push(b'\n');
    }
    let replies = raw(&fixture, input);
    assert_eq!(replies.len(), 9);
    assert_eq!(replies[0]["error"]["code"], -32700);
    for reply in &replies[1..6] {
        assert_eq!(reply["error"]["code"], -32600);
    }
    assert_eq!(replies[6]["error"]["code"], -32602);
    assert_eq!(replies[7]["error"]["code"], -32602);
    assert_eq!(replies[8]["error"]["code"], -32601);
    assert_eq!(
        Vault::open(&fixture.vault).unwrap().revision().unwrap(),
        revision
    );
    assert_eq!(journal_files(&fixture), before);
}

#[test]
fn notifications_are_never_tool_calls_and_never_receive_responses() {
    let fixture = fixture();
    let before = journal_files(&fixture);
    let replies = messages(
        &fixture,
        vec![
            json!({"jsonrpc":"2.0","method":"tools/call","params":{"name":"memory.commit","arguments":{"context":"synthetic-context"}}}),
            json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}),
            json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        ],
    );
    assert!(replies.is_empty());
    assert_eq!(journal_files(&fixture), before);
}

#[test]
fn oversized_frame_is_bounded_and_next_frame_remains_parseable() {
    let fixture = fixture();
    let before = journal_files(&fixture);
    let mut input = vec![b'x'; MAX_MESSAGE_BYTES + 100_000];
    input.push(b'\n');
    serde_json::to_writer(&mut input, &modern(json!("next"), "tools/list", json!({}))).unwrap();
    input.push(b'\n');
    let replies = raw(&fixture, input);
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0]["error"]["code"], -32600);
    assert_eq!(replies[1]["id"], "next");
    assert!(replies[1]["result"]["tools"].is_array());
    assert_eq!(journal_files(&fixture), before);
}

#[test]
fn truncated_frame_never_dispatches_and_tool_failure_is_not_protocol_error() {
    let fixture = fixture();
    let incomplete = raw(
        &fixture,
        br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_vec(),
    );
    assert_eq!(incomplete[0]["error"]["code"], -32700);
    let replies = messages(
        &fixture,
        vec![
            initialize(),
            initialized(),
            json!({"jsonrpc":"2.0","id":"missing-note","method":"tools/call","params":{"name":"memory.read","arguments":{"context":"synthetic-context","path":"household/missing.md"}}}),
        ],
    );
    assert_eq!(replies[1]["result"]["isError"], true);
    assert!(replies[1].get("error").is_none());
    assert!(replies[1]["result"]["structuredContent"].is_object());
}

#[test]
fn caller_supplied_actor_cannot_change_memory_attribution() {
    let fixture = fixture();
    let vault = Vault::open(&fixture.vault).unwrap();
    let revision = vault.revision().unwrap();
    let replies = messages(
        &fixture,
        vec![modern(
            json!(1),
            "tools/call",
            json!({
                "name":"memory.commit", "arguments":{
                    "context":"synthetic-context", "actor":"another-person", "expected_revision":revision,
                    "edits":[{"path":"household/note.md","content":"Synthetic note"}], "reason":"Synthetic test"
                }
            }),
        )],
    );
    assert_eq!(replies[0]["result"]["isError"], true);
    assert_eq!(vault.revision().unwrap(), revision);
    let arguments = json!({"context":"synthetic-context", "expected_revision":revision,
        "edits":[{"path":"household/note.md","content":"Synthetic note"}], "reason":"Synthetic test"});
    let successful = messages(
        &fixture,
        vec![
            modern(
                json!(2),
                "tools/call",
                json!({"name":"memory.commit","arguments":arguments}),
            ),
            modern(
                json!(3),
                "tools/call",
                json!({"name":"memory.commit","arguments":arguments}),
            ),
        ],
    );
    assert_eq!(successful[0]["result"]["isError"], false);
    assert_eq!(successful[1]["result"]["isError"], false);
    let receipt = &successful[0]["result"]["structuredContent"]["receipt"];
    assert_eq!(receipt["provenance"]["author"], "synthetic-owner");
    assert_eq!(receipt["provenance"]["run_id"], "synthetic-run");
    assert!(receipt["provenance"]["source"]
        .as_str()
        .unwrap()
        .starts_with("broker:1:"));
    assert_eq!(
        successful[1]["result"]["structuredContent"]["receipt"],
        *receipt
    );
    assert_eq!(
        successful[1]["result"]["structuredContent"]["replayed"],
        true
    );
    assert_eq!(vault.history(10).unwrap().len(), 1);
}

#[test]
fn memory_read_returns_same_structured_and_text_result() {
    let fixture = fixture();
    let vault = Vault::open(&fixture.vault).unwrap();
    vault
        .commit(
            &vault.revision().unwrap(),
            &[Edit::Write {
                path: "household/note.md".into(),
                content: "A synthetic bilingual note: 游泳。".into(),
            }],
            &Provenance {
                author: "synthetic-owner".into(),
                source: "fixture".into(),
                run_id: "fixture".into(),
                reason: "Create synthetic note".into(),
            },
        )
        .unwrap();
    let replies = messages(
        &fixture,
        vec![modern(
            json!(1),
            "tools/call",
            json!({"name":"memory.read","arguments":{"context":"synthetic-context","path":"household/note.md"}}),
        )],
    );
    let result = &replies[0]["result"];
    assert_eq!(result["isError"], false);
    let text: Value = serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(text, result["structuredContent"]);
    assert!(text.to_string().contains("游泳"));
    assert!(text.to_string().contains("synthetic-owner"));
}

#[test]
fn permission_decision_is_a_single_text_block() {
    // Incident 2026-10-03: with structuredContent, Claude Code rejected an
    // allowed decision as "an invalid result" and blocked the Gmail call.
    let fixture = fixture_with(Access::Connectors);
    let replies = messages(
        &fixture,
        vec![modern(
            json!(1),
            "tools/call",
            json!({"name":"permission.decide","arguments":{"tool_name":"mcp__claude_ai_Gmail__list_labels","input":{}}}),
        )],
    );
    let result = &replies[0]["result"];
    assert!(result.get("structuredContent").is_none());
    assert_eq!(result["content"].as_array().unwrap().len(), 1);
    let decision: Value =
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(decision["behavior"], "allow");
}
