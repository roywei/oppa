//! The secrets connector through the host's connector protocol, against a fake
//! 1Password CLI and Keychain whose values are made up here (one connector reads everything in the owner's OPPA vault).

use oppa::{
    config::{ConnectorKind, ConnectorManifest},
    connectors,
};
use serde_json::json;
use std::{fs, os::unix::fs::PermissionsExt, path::Path, process::Command};

const ADAPTER: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/resources/connectors/onepassword.py"
);
const TOKEN: &str = "ops_synthetic_token";
const KEY: &str = "sk-synthetic-4d2f";

/// A fake `op` that answers only with the right token and vault, and a fake
/// `security` that holds the token.
fn fakes(dir: &Path) -> Vec<String> {
    let op = dir.join("op");
    fs::write(
        &op,
        format!(
            r#"#!/usr/bin/env python3
import json, os, sys
args = sys.argv[1:]
if os.environ.get("OP_SERVICE_ACCOUNT_TOKEN") != "{TOKEN}":
    sys.stderr.write("[ERROR] unauthorized\n"); sys.exit(1)
if args[args.index("--vault") + 1] != "Synthetic":
    sys.stderr.write("[ERROR] \"x\" isn't a vault\n"); sys.exit(1)
items = {{"X_API_KEY": {{"id": "i1", "title": "X_API_KEY", "category": "API_CREDENTIAL", "fields": [
            {{"id": "credential", "label": "credential", "type": "CONCEALED", "value": "{KEY}"}},
            {{"id": "notes", "label": "notesPlain", "type": "STRING", "value": ""}}]}},
         "Synthetic Bank": {{"id": "i2", "title": "Synthetic Bank", "category": "LOGIN",
            "urls": [{{"href": "https://bank.example"}}], "fields": [
            {{"id": "username", "label": "username", "purpose": "USERNAME", "value": "sam"}},
            {{"id": "password", "label": "password", "purpose": "PASSWORD", "type": "CONCEALED", "value": "hunter2-synthetic"}},
            {{"id": "otp", "label": "one-time password", "type": "OTP", "value": "otpauth://x", "totp": "123456"}}]}}}}
if args[:2] == ["item", "list"]:
    print(json.dumps([{{"id": i["id"], "title": i["title"]}} for i in items.values()]))
elif args[:3] == ["item", "get", "-"]:
    for wanted in json.load(sys.stdin):
        item = json.loads(json.dumps(next(i for i in items.values() if i["id"] == wanted["id"])))
        for field in item["fields"]:
            if field.get("type") in ("CONCEALED", "OTP"):
                field["value"] = "[concealed]"
                field.pop("totp", None)
        print(json.dumps(item, indent=2))
elif args[:2] == ["item", "get"]:
    item = next((i for i in items.values() if args[2] in (i["id"], i["title"])), None)
    if item is None:
        sys.stderr.write("[ERROR] \"x\" isn't an item\n"); sys.exit(1)
    if "--reveal" not in args:
        item = json.loads(json.dumps(item))
        for field in item["fields"]:
            if field.get("type") in ("CONCEALED", "OTP"):
                field["value"] = "[concealed]"
                field.pop("totp", None)
    print(json.dumps(item))
"#
        ),
    )
    .unwrap();
    let security = dir.join("security");
    fs::write(
        &security,
        format!("#!/bin/sh\n[ \"$*\" = \"find-generic-password -s oppa-1password -w\" ] && echo {TOKEN}\n"),
    )
    .unwrap();
    for file in [&op, &security] {
        fs::set_permissions(file, fs::Permissions::from_mode(0o755)).unwrap();
    }
    [
        "--op",
        op.to_str().unwrap(),
        "--vault",
        "Synthetic",
        "--security",
        security.to_str().unwrap(),
    ]
    .map(String::from)
    .to_vec()
}

fn manifest(extra: Vec<String>) -> ConnectorManifest {
    let mut command = vec!["python3".to_string(), ADAPTER.into()];
    command.extend(extra);
    ConnectorManifest {
        id: "secrets".into(),
        kind: ConnectorKind::Script,
        command,
        timeout_seconds: 30,
        owner_only: false,
    }
}

#[test]
fn the_vault_is_listed_without_values_and_read_whole_on_request() {
    // C-6: items lists what the vault holds and never a value; get returns an
    // item's values and a current one-time code; a missing item or field is a
    // plain error with a fix; health counts the items.
    let dir = tempfile::tempdir().unwrap();
    let secrets = manifest(fakes(dir.path()));
    let described = connectors::describe(&secrets).unwrap();
    let tiers: Vec<_> = described
        .iter()
        .map(|op| (op.name.clone(), op.tier.clone()))
        .collect();
    assert_eq!(
        serde_json::to_value(&tiers).unwrap(),
        json!([["items", "read"], ["get", "read"]])
    );
    let listed = connectors::call(&secrets, "items", json!({}), false).unwrap();
    assert_eq!(listed["status"], "observed", "{listed}");
    let text = listed.to_string();
    assert!(!text.contains(KEY) && !text.contains("hunter2"), "{text}");
    assert_eq!(
        listed["result"]["items"][1]["websites"],
        json!(["https://bank.example"])
    );
    assert_eq!(
        listed["result"]["items"][1]["fields"],
        json!(["username", "password", "one-time password"])
    );
    let got = connectors::call(&secrets, "get", json!({"item":"Synthetic Bank"}), false).unwrap();
    assert_eq!(got["result"]["values"]["password"], "hunter2-synthetic");
    assert_eq!(got["result"]["values"]["one-time password"], "123456");
    let one = connectors::call(
        &secrets,
        "get",
        json!({"item":"X_API_KEY","field":"credential"}),
        false,
    )
    .unwrap();
    assert_eq!(one["result"]["values"], json!({"credential": KEY}));
    for (arguments, error) in [
        (json!({"item":"Nope"}), "no single item by that name"),
        (
            json!({"item":"X_API_KEY","field":"nope"}),
            "no field named nope",
        ),
        (json!({"item":"--reveal"}), "item must be"),
    ] {
        let refused = connectors::call(&secrets, "get", arguments.clone(), false).unwrap();
        assert_eq!(refused["status"], "error", "{arguments}");
        assert!(
            refused["error"].as_str().unwrap().contains(error),
            "{refused}"
        );
    }
    let health = connectors::health(&secrets, false).unwrap();
    assert_eq!(
        (&health["ready"], &health["items"]),
        (&json!(true), &json!(2))
    );
}

#[test]
fn a_script_reads_one_value_and_a_wrong_setup_says_how_to_fix_it() {
    // C-3, C-4: a skill's script gets the value alone on stdout; without the
    // token or the CLI, or with another vault, the reason and fix go to stderr.
    let dir = tempfile::tempdir().unwrap();
    let flags = fakes(dir.path());
    let run = |flags: &[String], words: &[&str]| {
        Command::new("python3")
            .arg(ADAPTER)
            .args(flags)
            .args(words)
            .output()
            .unwrap()
    };
    let key = run(&flags, &["get", "X_API_KEY"]);
    assert!(key.status.success());
    assert_eq!(String::from_utf8(key.stdout).unwrap(), format!("{KEY}\n"));
    let password = run(&flags, &["get", "Synthetic Bank"]);
    assert_eq!(
        String::from_utf8(password.stdout).unwrap(),
        "hunter2-synthetic\n"
    );
    let user = run(&flags, &["get", "Synthetic Bank", "username"]);
    assert_eq!(String::from_utf8(user.stdout).unwrap(), "sam\n");
    let titles = run(&flags, &["items"]);
    assert_eq!(
        String::from_utf8(titles.stdout).unwrap(),
        "X_API_KEY\nSynthetic Bank\n"
    );
    let mut other_vault = flags.clone();
    other_vault[3] = "Elsewhere".into();
    let mut no_token = flags.clone();
    no_token[5] = "/usr/bin/false".into();
    let mut no_cli = flags.clone();
    no_cli[1] = "/nonexistent/op".into();
    for (flags, reason) in [
        (other_vault, "cannot read a vault named Elsewhere"),
        (no_token, "no 1Password service account token"),
        (no_cli, "1Password CLI (op) is not installed"),
    ] {
        let failed = run(&flags, &["get", "X_API_KEY"]);
        assert!(!failed.status.success());
        assert!(failed.stdout.is_empty());
        let stderr = String::from_utf8(failed.stderr).unwrap();
        assert!(stderr.contains(reason), "{stderr}");
    }
}
