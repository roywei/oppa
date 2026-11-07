use oppa::{
    config::{ConnectorKind, ConnectorManifest},
    connectors,
};
use serde_json::json;
use std::time::Instant;

/// A synthetic connector: its behaviour is chosen by its first argument.
const FAKE: &str = r#"
import json, os, sys, time
mode = sys.argv[1]
request = json.loads(sys.stdin.read())
if mode == "slow":
    time.sleep(30)
if mode == "linger":
    import subprocess
    subprocess.Popen(["sleep", "30"])
    print(json.dumps({"status": "observed", "result": "answered"}))
    sys.exit(0)
if mode == "env":
    print(json.dumps({"status": "observed", "result": {"cargo": "CARGO_MANIFEST_DIR" in os.environ, "path": "PATH" in os.environ}}))
    sys.exit(0)
if mode == "noise":
    print("not json")
    sys.exit(0)
if mode == "huge":
    print(json.dumps({"status": "observed", "result": "x" * (2 * 1024 * 1024)}))
    sys.exit(0)
if request["method"] == "describe":
    ops = [
        {"name": "list_events", "tier": "read", "description": "Synthetic events.", "input_schema": {"type": "object"}},
        {"name": "add_event", "tier": "reversible", "description": "Synthetic write.", "input_schema": {"type": "object"}},
    ]
    if mode == "bad-name":
        ops[0]["name"] = "List Events"
    if mode == "bad-schema":
        ops[0]["input_schema"] = {"type": "object", "properties": []}
    if mode == "bad-tier":
        ops[1]["tier"] = "admin"
    if mode == "many":
        ops = [dict(ops[0], name="op_%d" % i) for i in range(65)]
    print(json.dumps({"protocol_version": 1, "operations": ops}))
elif request["method"] == "call":
    if request["arguments"].get("fail"):
        print(json.dumps({"status": "error", "error": "Synthetic source unavailable.", "fix": "Try again."}))
    else:
        print(json.dumps({"status": "observed", "result": {"echo": request["arguments"], "prompt": request["may_prompt"]}}))
elif request["method"] == "health":
    print(json.dumps({"ready": mode != "unready", "fix": "Synthetic fix."}))
"#;

fn connector(mode: &str, timeout_seconds: u64) -> ConnectorManifest {
    ConnectorManifest {
        id: "synthetic".into(),
        kind: ConnectorKind::Script,
        command: vec!["python3".into(), "-c".into(), FAKE.into(), mode.into()],
        timeout_seconds,
        owner_only: false,
    }
}

#[test]
fn operations_keep_their_tier_and_malformed_ones_refuse_the_connector() {
    // C-5: the broker decides which tiers a run sees, so describe keeps them all
    // and refuses a tier it cannot enforce.
    let operations = connectors::describe(&connector("ok", 5)).unwrap();
    assert_eq!(
        operations
            .iter()
            .map(|o| (o.name.as_str(), o.tier.as_str()))
            .collect::<Vec<_>>(),
        [("list_events", "read"), ("add_event", "reversible")]
    );
    assert!(connectors::describe(&connector("bad-tier", 5)).is_err());
    assert!(connectors::describe(&connector("bad-name", 5)).is_err());
    // A schema the broker could not extend refuses the connector, never panics it.
    assert!(connectors::describe(&connector("bad-schema", 5)).is_err());
    // Past this many operations the harness adapters would refuse every turn.
    assert!(connectors::describe(&connector("many", 5)).is_err());
}

#[test]
fn calls_return_the_adapters_observation_or_its_plain_failure() {
    // C-4: a failed source read comes back as a failure with its fix.
    let ok = connectors::call(
        &connector("ok", 5),
        "list_events",
        json!({"day":"2026-01-02"}),
        false,
    )
    .unwrap();
    assert_eq!(ok["status"], "observed");
    assert_eq!(ok["result"]["echo"]["day"], "2026-01-02");
    assert_eq!(ok["result"]["prompt"], false);
    let failed = connectors::call(
        &connector("ok", 5),
        "list_events",
        json!({"fail":true}),
        false,
    )
    .unwrap();
    assert_eq!(failed["status"], "error");
    assert_eq!(failed["fix"], "Try again.");
    assert_eq!(
        connectors::health(&connector("unready", 5), false).unwrap()["ready"],
        false
    );
}

#[test]
fn a_hung_noisy_or_oversized_connector_is_an_error_not_a_result() {
    let started = Instant::now();
    assert!(connectors::call(&connector("slow", 1), "list_events", json!({}), false).is_err());
    assert!(
        started.elapsed().as_secs() < 10,
        "the hung connector was killed"
    );
    assert!(connectors::call(&connector("noise", 5), "list_events", json!({}), false).is_err());
    assert!(connectors::call(&connector("huge", 5), "list_events", json!({}), false).is_err());
    // An adapter that answered but left a child holding stdout still answers
    // promptly, and the child is stopped with it.
    let started = Instant::now();
    let lingered =
        connectors::call(&connector("linger", 5), "list_events", json!({}), false).unwrap();
    assert_eq!(lingered["result"], "answered");
    assert!(
        started.elapsed().as_secs() < 4,
        "a lingering child delayed the answer"
    );
}

#[test]
fn a_connector_holds_only_its_own_pipes_while_others_start() {
    // Incidents 2026-10-09 and 2026-10-10: under load, connector calls failed
    // with "did not answer in time" seconds before their deadline. On macOS a
    // spawn's pipes become close-on-exec only after they are made, so a
    // process started by another thread in between kept a copy, and the call
    // waited for an end of file that came only when that process exited.
    // `ls` holds descriptors of its own for the folder it lists (3 and 4 on
    // macOS, 3 on Linux), so a call made alone sets what every call shows.
    let listing = ConnectorManifest {
        id: "fds".into(),
        kind: ConnectorKind::Script,
        command: vec![
            "/bin/sh".into(),
            "-c".into(),
            r#"cat >/dev/null; printf '{"status":"observed","result":"%s"}' "$(/bin/ls /dev/fd | /usr/bin/tr '\n' ' ')""#.into(),
        ],
        timeout_seconds: 30,
        owner_only: false,
    };
    let alone = connectors::call(&listing, "list", json!({}), false).unwrap()["result"].clone();
    assert!(alone.as_str().unwrap().starts_with("0 1 2 "), "{alone}");
    let strays = std::sync::Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for _ in 0..16 {
            scope.spawn(|| {
                for _ in 0..150 {
                    let answer = connectors::call(&listing, "list", json!({}), false).unwrap();
                    if answer["result"] != alone {
                        strays.lock().unwrap().push(answer["result"].clone());
                    }
                }
            });
        }
    });
    let strays = strays.into_inner().unwrap();
    assert!(
        strays.is_empty(),
        "{} of 2400 connectors held other pipes, such as {}",
        strays.len(),
        strays[0]
    );
}

#[test]
fn an_adapter_gets_a_minimal_environment() {
    // Secrets in the host's environment never reach a connector.
    assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_some());
    let value = connectors::call(&connector("env", 5), "list_events", json!({}), false).unwrap();
    assert_eq!(value["result"], json!({"cargo":false,"path":true}));
}

#[test]
fn the_mac_shim_answers_in_the_methods_shape_when_it_cannot_build() {
    // A compile failure must still reach the owner as a plain fix in health.
    let dir = tempfile::tempdir().unwrap();
    let source = concat!(env!("CARGO_MANIFEST_DIR"), "/resources/connectors/apple.py");
    std::fs::copy(source, dir.path().join("apple.py")).unwrap();
    std::fs::write(dir.path().join("apple.swift"), "this is not swift").unwrap();
    let shim = ConnectorManifest {
        id: "mac".into(),
        kind: ConnectorKind::Script,
        command: vec![
            "python3".into(),
            dir.path().join("apple.py").to_str().unwrap().into(),
        ],
        timeout_seconds: 120,
        owner_only: false,
    };
    let health = connectors::health(&shim, false).unwrap();
    assert_eq!(health["ready"], false);
    assert!(health["fix"].as_str().unwrap().contains("xcode-select"));
}

#[cfg(target_os = "macos")]
#[test]
fn the_mac_connector_checks_every_argument_before_it_asks_for_access() {
    // C-8, C-5: a bad request never reaches Calendar, Reminders or Contacts. The
    // adapter runs in a sandbox that cannot reach them, so no case can read or
    // change the Mac's data, and a good request stops at the access check.
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("mac-connector");
    std::fs::create_dir_all(&dir).unwrap();
    for name in ["apple.py", "apple.swift"] {
        let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("resources/connectors");
        std::fs::copy(source.join(name), dir.join(name)).unwrap();
    }
    let sandbox = r#"(version 1)(allow default)(deny mach-lookup
        (global-name "com.apple.tccd") (global-name "com.apple.tccd.system")
        (global-name "com.apple.calaccessd") (global-name "com.apple.CalendarAgent")
        (global-name "com.apple.remindd") (global-name "com.apple.contactsd")
        (global-name "com.apple.contactsd.persistence"))"#;
    let mac = ConnectorManifest {
        id: "mac".into(),
        kind: ConnectorKind::Script,
        // A fixed zone puts a daylight-saving gap on the same day everywhere.
        command: [
            "/usr/bin/env",
            "TZ=America/Los_Angeles",
            "/usr/bin/sandbox-exec",
            "-p",
            sandbox,
            "python3",
            dir.join("apple.py").to_str().unwrap(),
        ]
        .map(String::from)
        .to_vec(),
        timeout_seconds: 120,
        owner_only: false,
    };
    let health = connectors::health(&mac, false).unwrap();
    assert_eq!(
        health["ready"], false,
        "the sandbox must hide the stores: {health}"
    );
    // Operation, arguments, and what the answer says. The last eight are good
    // requests: they pass every check and stop where the store would begin.
    let cases = r#"
calendar_events       | {"from":"2026-02-30"}                         | must exist
calendar_events       | {"from":"2027-02-29"}                         | must exist
calendar_events       | {"from":"2026-03-08T02:30"}                   | must exist
calendar_events       | {"from":"2026-02-30T10:00:00Z"}               | must exist
calendar_events       | {"from":20260102}                             | dates are strings
calendar_events       | {"from":"2026-01-02","to":"2026-01-01"}       | must end after
calendar_events       | {"calendars":"Home"}                          | calendars must be a list
reminders_find        | {}                                            | query is required
reminders_find        | {"query":5}                                   | query must be text
reminders_due         | {"due_before":"2026-13-01"}                   | must exist
contacts_search       | {"query":" "}                                 | query is required
calendar_create_event | {"start":"2026-01-02T10:00"}                  | title is required
calendar_create_event | {"title":"t"}                                 | start is required
calendar_create_event | {"title":"t","start":"2026-01-02T10:00","end":"2026-01-02T09:00"} | must end after
calendar_create_event | {"title":"t","start":"2026-01-02","calendar":7}       | calendar must be text
calendar_create_event | {"title":"t","start":"2026-01-02","all_day":"yes"}    | all_day must be true
calendar_update_event | {"title":"t"}                                 | id is required
calendar_update_event | {"id":"e","span":"all"}                       | span is this or future
calendar_update_event | {"id":"e","occurrence_start":"2026-04-31"}    | must exist
calendar_update_event | {"id":"e","location":5}                       | location must be text
calendar_delete_event | {"id":"e","span":true}                        | span must be text
reminders_create      | {"title":"t","due":"2026-02-30"}              | must exist
reminders_create      | {"title":"t","priority":"high"}               | priority must be a whole number
reminders_update      | {"id":"r","completed":"yes"}                  | completed must be true
reminders_update      | {"id":"r","completed":1}                      | completed must be true
reminders_create      | {"title":"t","priority":true}                 | priority must be a whole number
reminders_delete      | {}                                            | id is required
contacts_create       | {"add_phones":["555"]}                        | needs a name or an organization
contacts_create       | {"given_name":"A","add_phones":"555"}         | add_phones must be a list
contacts_update       | {"id":"c","family_name":5}                    | family_name must be text
contacts_delete       | {}                                            | id is required
calendar_create_event | {"title":"t","start":"2026-01-02","repeat":"monthly"}                       | repeat must be an object
calendar_create_event | {"title":"t","start":"2026-01-02","repeat":{"every":"fortnight"}}           | repeat every is day
calendar_create_event | {"title":"t","start":"2026-01-02","repeat":{"every":"day","count":3}}       | repeat takes every
calendar_create_event | {"title":"t","start":"2026-01-02","repeat":{"every":"week","interval":0}}   | interval is 1 to 99
calendar_create_event | {"title":"t","start":"2026-01-02","repeat":{"every":"month","on":["monday"]}} | only for every week
calendar_create_event | {"title":"t","start":"2026-01-02","repeat":{"every":"week","on":["someday"]}} | day names
calendar_create_event | {"title":"t","start":"2026-01-02","repeat":{"every":"week","on":["monday","monday"]}} | each day once
calendar_create_event | {"title":"t","start":"2026-01-02","repeat":{"every":"day","until":"2026-02-30"}} | must exist
calendar_update_event | {"id":"e","span":"this","repeat":{"every":"week"}}                          | give span future
reminders_create      | {"title":"t","repeat":{"every":"month"}}                                    | needs a due date
reminders_update      | {"id":"r","repeat":{"every":"never","interval":2}}                          | never takes nothing else
calendar_events       | {"from":"2026-03-08","calendars":["Home"]}    | Calendars access is
calendar_create_event | {"title":"t","start":"2026-01-02T10:00:00-08:00"}     | Calendars access is
calendar_update_event | {"id":"e","span":"future","occurrence_start":"2026-01-02T18:00:00Z","location":""} | Calendars access is
reminders_create      | {"title":"t","due":"2026-01-02"}              | Reminders access is
calendar_create_event | {"title":"t","start":"2026-01-02T10:00","repeat":{"every":"week","on":["tuesday"],"until":"2026-03-01"}} | Calendars access is
reminders_create      | {"title":"t","due":"2026-01-19T09:00","repeat":{"every":"month","interval":2}} | Reminders access is
reminders_update      | {"id":"r","due":"","completed":true}          | Reminders access is
contacts_create       | {"given_name":"A","add_phones":["555"]}       | Contacts access is
"#;
    for case in cases.lines().filter(|line| !line.is_empty()) {
        let [operation, arguments, reason] =
            case.split(" | ").map(str::trim).collect::<Vec<_>>()[..]
        else {
            panic!("malformed case: {case}");
        };
        let arguments: serde_json::Value = serde_json::from_str(arguments).unwrap();
        let answer = connectors::call(&mac, operation, arguments.clone(), false).unwrap();
        assert_eq!(
            answer["status"], "error",
            "{operation} {arguments}: {answer}"
        );
        assert!(
            answer["error"].as_str().unwrap().contains(reason),
            "{operation} {arguments}: {answer}"
        );
    }
}

/// What tests/apple/checks.swift prints, from a build of the Mac adapter with
/// those checks in place of its request handling; built once for every test.
#[cfg(target_os = "macos")]
fn apple_checks() -> &'static serde_json::Value {
    static CHECKS: std::sync::OnceLock<serde_json::Value> = std::sync::OnceLock::new();
    CHECKS.get_or_init(|| {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        // Only a file named main.swift may run code at the top, as the adapter does.
        let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("apple-checks");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::copy(
            root.join("resources/connectors/apple.swift"),
            dir.join("main.swift"),
        )
        .unwrap();
        std::fs::copy(
            root.join("tests/apple/checks.swift"),
            dir.join("checks.swift"),
        )
        .unwrap();
        let binary = dir.join("apple-checks");
        // Started as the host starts processes, so none keeps a copy of a
        // concurrent connector call's pipes.
        let built = oppa::process::output(
            std::process::Command::new("/usr/bin/xcrun")
                .args(["swiftc", "-D", "TESTING"])
                .arg(dir.join("main.swift"))
                .arg(dir.join("checks.swift"))
                .arg("-o")
                .arg(&binary),
        )
        .unwrap();
        assert!(
            built.status.success(),
            "{}",
            String::from_utf8_lossy(&built.stderr)
        );
        let output = oppa::process::output(
            std::process::Command::new(&binary).env("TZ", "America/Los_Angeles"),
        )
        .unwrap();
        serde_json::from_slice(&output.stdout).unwrap()
    })
}

#[cfg(target_os = "macos")]
#[test]
fn a_reminders_alert_follows_its_due_time() {
    // C-8: a due time sets one alert at that instant, to the minute; a day alone
    // or an empty due sets none; a change that leaves due out keeps the alert. The
    // adapter's own code runs on reminders that are never saved.
    let cases = &apple_checks()["due"];
    // The seconds of 09:00:45 go from the due time and from its alert alike.
    let time = &cases["time"];
    assert!(
        time["due_at"]
            .as_str()
            .unwrap()
            .starts_with("2031-01-06T09:00:00"),
        "{cases}"
    );
    assert_eq!(time["alerts"], json!([time["due_at"]]));
    assert_eq!(cases["day"]["due"], "-:-:-");
    assert_eq!(cases["day"]["alerts"], json!([]));
    assert!(cases["cleared"].get("due").is_none());
    assert_eq!(cases["cleared"]["alerts"], json!([]));
    assert_eq!(cases["absent"]["due"], "8:0:-");
    assert_eq!(cases["absent"]["alerts"].as_array().unwrap().len(), 1);
    // On the night clocks go back, each 1:30 alerts at the instant it names.
    assert_eq!(
        cases["daylight"]["alerts"],
        json!(["2030-11-03T01:30:00-07:00"])
    );
    assert_eq!(
        cases["standard"]["alerts"],
        json!(["2030-11-03T01:30:00-08:00"])
    );
}

#[cfg(target_os = "macos")]
#[test]
fn a_repeat_reads_back_as_it_was_set() {
    // C-8: a repeat replaces an item's rule, or removes it, and reads back as
    // given, its last day included whole; a rule made elsewhere, which no write
    // here can make, reads back whole too.
    let cases = &apple_checks()["repeat"];
    assert_eq!(
        cases["weekly"],
        json!({"every":"week","interval":1,"on":["tuesday","thursday"],"until":"2031-03-01T23:59:59-08:00"})
    );
    assert_eq!(
        cases["bimonthly"],
        json!({"every":"month","interval":2,"until":"2031-06-01T09:00:00-07:00"})
    );
    assert_eq!(cases["biennial"], json!({"every":"year","interval":2}));
    assert_eq!(cases["never"], json!(null));
    assert_eq!(
        cases["elsewhere"],
        json!({"every":"month","interval":1,"on":["-1 friday"],"times":5})
    );
    assert_eq!(
        cases["imported"],
        json!({"every":"year","interval":1,"on":["monday"],"days_of_month":[1,-1],"months":[3,9],
            "weeks_of_year":[10],"days_of_year":[100],"positions":[1],"week_starts":"sunday"})
    );
    assert_eq!(
        cases["two_rules"],
        json!({"every":"week","interval":1,"also":[{"every":"month","interval":3}]})
    );
}

#[cfg(target_os = "macos")]
#[test]
fn a_reminders_repeat_changes_and_never_outlives_its_due_date() {
    // C-8: a reminder's repeat is set, changed and stopped as given; a due date
    // cleared while it still repeats is refused, and cleared with the repeat
    // stopped it is not.
    let cases = &apple_checks()["reminders"];
    let none = serde_json::Value::Null;
    assert_eq!(
        cases["set"]["repeat"],
        json!({"every":"month","interval":1})
    );
    assert_eq!(cases["set"]["refused"], none);
    assert_eq!(
        cases["changed"]["repeat"],
        json!({"every":"week","interval":1,"on":["monday"]})
    );
    assert_eq!(cases["changed"]["refused"], none);
    assert_eq!(cases["stopped"]["refused"], none);
    assert_eq!(cases["stopped"]["repeat"], none);
    assert_eq!(cases["stopped"]["due"], true);
    assert!(cases["due_cleared"]["refused"]
        .as_str()
        .unwrap()
        .contains("needs a due date"));
    assert_eq!(cases["due_cleared_and_stopped"]["refused"], none);
    assert_eq!(
        (
            &cases["due_cleared_and_stopped"]["due"],
            &cases["due_cleared_and_stopped"]["repeat"]
        ),
        (&json!(false), &none)
    );
}

#[test]
fn connector_manifests_are_validated_by_kind() {
    let base = oppa::config::Config {
        access: Default::default(),
        version: 1,
        source: None,
        connectors: vec![],
        owner: "synthetic-owner".into(),
        agents: vec![oppa::harness::AgentManifest {
            id: "first".into(),
            family: "synthetic".into(),
            command: vec!["true".into()],
            timeout_seconds: 5,
            task_timeout_seconds: 5,
            model: None,
            effort: None,
        }],
        channels: vec![],
        people: vec![],
        sessions: Default::default(),
        groups: vec![],
        heartbeat: None,
        browser: false,
        memory_sync: None,
    };
    let mut config = base.clone();
    config.connectors = vec![connector("ok", 5)];
    config.validate().unwrap();
    for broken in [
        ConnectorManifest {
            command: vec![],
            ..connector("ok", 5)
        },
        ConnectorManifest {
            id: "memory".into(),
            ..connector("ok", 5)
        },
        // Its `decide` would collide with the harness permission handler.
        ConnectorManifest {
            id: "permission".into(),
            ..connector("ok", 5)
        },
        connector("ok", 0),
    ] {
        let mut config = base.clone();
        config.connectors = vec![broken];
        assert!(config.validate().is_err());
    }
    // Each harness runs the model and effort the
    // owner chose, and `full` is another name for computer access.
    for (model, effort) in [(Some("bad model"), None), (None, Some("extreme"))] {
        let mut config = base.clone();
        config.agents[0].model = model.map(String::from);
        config.agents[0].effort = effort.map(String::from);
        assert!(config.validate().is_err(), "{model:?} {effort:?}");
    }
    let mut config = base.clone();
    config.agents[0].model = Some("claude-opus-5-5".into());
    config.agents[0].effort = Some("high".into());
    config.validate().unwrap();
    let text = toml::to_string(&base)
        .unwrap()
        .replace("access = \"read\"", "access = \"full\"")
        .replace("browser = false\n", "");
    let parsed: oppa::config::Config = toml::from_str(&text).unwrap();
    assert_eq!(parsed.access, oppa::harness::Access::Computer);
    assert!(
        parsed.browser,
        "the browser is on unless the owner turns it off"
    );
}
