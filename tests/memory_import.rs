//! `oppa memory import`: a folder of notes becomes one managed commit (M-2, M-3).

use oppa::vault::{Edit, Provenance, Vault};
use oppa::MAX_NOTE;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

fn oppa(root: &Path, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_oppa"));
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_")
            || key == "OPPA_HOME"
            || key == "OPPA_PRIVATE_DIR"
        {
            command.env_remove(key);
        }
    }
    command
        .arg("--home")
        .arg(root.join("runtime"))
        .arg("--private-dir")
        .arg(root.join("private"))
        .args(args)
        .output()
        .unwrap()
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git(repository: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(repository)
        .args(args)
        .output()
        .unwrap();
    assert_success(&output);
    String::from_utf8(output.stdout).unwrap()
}

fn instance() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let init = ["init", "--agent", "codex", "--owner", "synthetic-owner"];
    assert_success(&oppa(dir.path(), &init));
    dir
}

fn import(root: &Path, source: &Path, extra: &[&str]) -> Output {
    let mut args = vec!["memory", "import", source.to_str().unwrap()];
    args.extend([
        "--into",
        "imported/previous",
        "--source",
        "previous-assistant",
    ]);
    args.extend(["--reason", "Port earlier memories"]);
    args.extend(extra);
    oppa(root, &args)
}

/// The vault revision and every file outside Git metadata, so any write shows.
fn vault_state(root: &Path) -> (String, BTreeMap<PathBuf, Vec<u8>>) {
    fn walk(base: &Path, folder: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(folder).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() && !path.ends_with(".git") {
                walk(base, &path, files);
            } else if path.is_file() {
                let relative = path.strip_prefix(base).unwrap().to_owned();
                files.insert(relative, fs::read(&path).unwrap());
            }
        }
    }
    let vault = root.join("private/vault");
    let mut files = BTreeMap::new();
    walk(&vault, &vault, &mut files);
    let revision = oppa(root, &["memory", "revision"]);
    assert_success(&revision);
    let revision = String::from_utf8(revision.stdout).unwrap();
    (revision.trim().to_owned(), files)
}

#[test]
fn a_dry_run_writes_nothing_and_the_import_is_one_commit_that_search_finds() {
    let dir = instance();
    let root = dir.path();
    // The source sits in a hidden folder, as a previous assistant's may; only
    // entries inside it are judged.
    let source = root.join(".previous/memory");
    fs::create_dir_all(source.join("family")).unwrap();
    fs::create_dir_all(source.join(".obsidian")).unwrap();
    fs::write(source.join("preferences.md"), "Prefers green tea.\n").unwrap();
    fs::write(source.join("family/妈妈.md"), "妈妈喜欢喝龙井茶。\n").unwrap();
    fs::write(source.join("at-limit.md"), "a".repeat(MAX_NOTE)).unwrap();
    fs::write(source.join("over-limit.md"), "a".repeat(MAX_NOTE + 1)).unwrap();
    fs::write(source.join(".draft.md"), "Hidden draft.\n").unwrap();
    fs::write(source.join(".obsidian/workspace.md"), "Editor state.\n").unwrap();
    fs::write(source.join("export.txt"), "Not a note.\n").unwrap();
    fs::write(source.join("latin1.md"), b"caf\xe9\n").unwrap();
    // Finder stores a '/' typed in a name as ':', which vault paths refuse.
    fs::write(source.join("Q3:Q4 plan.md"), "Plans.\n").unwrap();
    fs::write(root.join("outside.md"), "Outside the folder.\n").unwrap();
    fs::hard_link(root.join("outside.md"), source.join("hard-link.md")).unwrap();
    std::os::unix::fs::symlink(root.join("outside.md"), source.join("symlink.md")).unwrap();

    let before = vault_state(root);
    let plan = import(root, &source, &["--dry-run"]);
    assert_success(&plan);
    assert_eq!(vault_state(root), before, "the dry run wrote to the vault");
    let done = import(root, &source, &[]);
    assert_success(&done);

    let notes = json!([
        "imported/previous/at-limit.md",
        "imported/previous/family/妈妈.md",
        "imported/previous/preferences.md",
    ]);
    let skipped = json!([
        {"file": ".draft.md", "reason": "hidden"},
        {"file": ".obsidian", "reason": "hidden"},
        {"file": "Q3:Q4 plan.md", "reason": "name has a colon, backslash or line break"},
        {"file": "export.txt", "reason": "not a .md file"},
        {"file": "hard-link.md", "reason": "hard link"},
        {"file": "latin1.md", "reason": "not UTF-8"},
        {"file": "over-limit.md", "reason": "larger than 256 KiB"},
        {"file": "symlink.md", "reason": "symlink"},
    ]);
    let (plan, done): (Value, Value) = (
        serde_json::from_slice(&plan.stdout).unwrap(),
        serde_json::from_slice(&done.stdout).unwrap(),
    );
    for (report, count) in [(&plan, "would_import"), (&done, "imported")] {
        assert_eq!(report["notes"], notes);
        assert_eq!(report["skipped_files"], skipped);
        assert_eq!(
            (report[count].as_u64(), report["skipped"].as_u64()),
            (Some(3), Some(8))
        );
    }
    assert!(plan["receipt"].is_null());
    // One commit: its parent is the revision before the import, and it is HEAD now.
    let receipt = &done["receipt"];
    assert_eq!(receipt["paths"], notes);
    assert_eq!(receipt["provenance"]["source"], "previous-assistant");
    assert_eq!(receipt["provenance"]["reason"], "Port earlier memories");
    assert_eq!(receipt["parent_revision"], before.0.as_str());
    assert_eq!(receipt["revision"], vault_state(root).0.as_str());
    let log: Value = serde_json::from_slice(&oppa(root, &["memory", "log"]).stdout).unwrap();
    assert_eq!(log, json!([receipt]));
    let search = oppa(root, &["memory", "search", "green 龙井"]);
    let hits: Value = serde_json::from_slice(&search.stdout).unwrap();
    let mut found: Vec<_> = hits
        .as_array()
        .unwrap()
        .iter()
        .map(|hit| hit["path"].as_str().unwrap())
        .collect();
    found.sort();
    assert_eq!(
        found,
        [
            "imported/previous/family/妈妈.md",
            "imported/previous/preferences.md"
        ]
    );
}

#[test]
fn a_dry_run_refuses_what_the_commit_would_refuse() {
    let dir = instance();
    let root = dir.path();
    let source = root.join("memory");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("note.md"), "Synthetic note.\n").unwrap();
    let plan = ["memory", "import", source.to_str().unwrap(), "--dry-run"];
    for (into, reason, refusal) in [
        ("imported/previous", " ", "provenance"),
        ("../outside", "Port earlier memories", "--into"),
    ] {
        let flags = [
            "--into",
            into,
            "--source",
            "previous-assistant",
            "--reason",
            reason,
        ];
        let refused = oppa(root, &[&plan[..], &flags].concat());
        assert!(!refused.status.success(), "a dry run accepted {flags:?}");
        assert!(String::from_utf8_lossy(&refused.stderr).contains(refusal));
        assert!(
            refused.stdout.is_empty(),
            "a refused dry run printed a plan"
        );
    }
}

#[test]
fn an_existing_target_refuses_the_whole_import_and_writes_nothing() {
    let dir = instance();
    let root = dir.path();
    let source = root.join("memory");
    fs::create_dir_all(&source).unwrap();
    for name in ["committed.md", "owner.md", "new.md"] {
        fs::write(source.join(name), format!("Imported {name}\n")).unwrap();
    }
    // A committed note and an owner's uncommitted file already hold two targets.
    let existing = root.join("existing.md");
    fs::write(&existing, "Committed before the import.\n").unwrap();
    let revision = vault_state(root).0;
    let put = [
        "memory",
        "put",
        "imported/previous/committed.md",
        "--file",
        existing.to_str().unwrap(),
        "--expected-revision",
        &revision,
        "--reason",
        "Synthetic existing note",
        "--source",
        "owner",
    ];
    assert_success(&oppa(root, &put));
    let vault = root.join("private/vault");
    fs::write(vault.join("imported/previous/owner.md"), "Owner draft.\n").unwrap();

    let before = vault_state(root);
    let refused = import(root, &source, &[]);
    assert!(!refused.status.success(), "a conflicting import succeeded");
    assert!(String::from_utf8_lossy(&refused.stderr).contains("nothing was written"));
    let report: Value = serde_json::from_slice(&refused.stdout).unwrap();
    assert_eq!(
        report["conflicts"],
        json!([
            {"path": "imported/previous/committed.md", "reason": "already exists in the vault"},
            {"path": "imported/previous/owner.md", "reason": "already exists in the vault"},
        ])
    );
    assert!(report["receipt"].is_null());
    assert_eq!(
        vault_state(root),
        before,
        "a refused import wrote to the vault"
    );
}

#[test]
fn an_import_past_the_argument_limit_commits_and_recovers() {
    let dir = instance();
    let root = dir.path();
    // Long folder names carry the note paths past what one command line may
    // hold (1 MiB on macOS, usually 2 MiB on Linux) with about a thousand files.
    let limit = unsafe { libc::sysconf(libc::_SC_ARG_MAX) }.clamp(1 << 17, 1 << 23) as usize;
    let folder = ["a", "b", "c", "d"]
        .map(|letter| letter.repeat(190))
        .join("/");
    let source = root.join("memory");
    fs::create_dir_all(source.join(&folder)).unwrap();
    let (mut changes, mut bytes) = (Vec::new(), 0);
    while bytes <= limit {
        let (file, content) = (
            format!("{folder}/note-{:05}.md", changes.len()),
            format!("Synthetic note {}.\n", changes.len()),
        );
        fs::write(source.join(&file), &content).unwrap();
        let path = format!("imported/previous/{file}");
        bytes += path.len() + 1;
        changes.push(json!({"path": path, "before": null, "after": content}));
    }
    let done = import(root, &source, &[]);
    assert_success(&done);
    let receipt = serde_json::from_slice::<Value>(&done.stdout).unwrap()["receipt"].clone();
    assert_eq!(receipt["paths"].as_array().unwrap().len(), changes.len());
    let vault = root.join("private/vault");
    assert_eq!(git(&vault, &["status", "--porcelain"]), "");

    // Interrupted after HEAD moved: the index still has the old tree and the
    // journal remains. Recovery has to finish an edit this large as well.
    let parent = receipt["parent_revision"].as_str().unwrap();
    git(&vault, &["read-tree", parent]);
    let journal = json!({"receipt": receipt, "changes": changes});
    let journal_path = vault.join(".git/oppa-vault-transaction.json");
    fs::write(&journal_path, serde_json::to_vec(&journal).unwrap()).unwrap();
    let recovered = oppa(root, &["memory", "recover"]);
    assert_success(&recovered);
    assert_eq!(
        serde_json::from_slice::<Value>(&recovered.stdout).unwrap(),
        receipt
    );
    assert!(!journal_path.exists());
    assert_eq!(git(&vault, &["status", "--porcelain"]), "");
}

#[test]
fn targets_one_file_with_each_other_or_the_vault_are_conflicts() {
    // A source on a case-sensitive volume can hold both spellings of a name,
    // and a vault on a case-insensitive one would store them as one file, so
    // the plan is checked from a listing rather than from such a disk.
    let temporary = tempfile::tempdir().unwrap();
    // APFS also folds ß and ss; where the volume keeps them apart, so does the plan.
    fs::write(temporary.path().join("Stra\u{df}e.md"), "").unwrap();
    let folds = temporary.path().join("Strasse.md").exists();
    let vault = Vault::init(&temporary.path().join("vault")).unwrap();
    let existing = ["people/Mom.md", "imported/Previous/a.md"].map(|path| Edit::Write {
        path: path.into(),
        content: "Synthetic existing note.\n".into(),
    });
    let provenance = Provenance {
        author: "synthetic-owner".into(),
        source: "synthetic".into(),
        run_id: "synthetic-run".into(),
        reason: "Existing notes".into(),
    };
    let base = vault.revision().unwrap();
    let revision = vault
        .commit(&base, &existing, &provenance)
        .unwrap()
        .revision;
    let targets = [
        "people/mom.md",
        "imported/previous/b.md",
        "imported/previous/d.md",
        "imported/Previous/Tea.md",
        "imported/Previous/tea.md",
        "imported/Previous/a.md",
        "imported/Previous/c.md",
        "imported/Previous/Stra\u{df}e.md",
        "imported/Previous/Strasse.md",
        "imported/Previous/e.md",
        "imported/Previous/e.md/g/f.md",
    ]
    .map(String::from);
    let conflicts = vault.new_note_conflicts(&revision, &targets).unwrap();
    let clash = |other: &str| Some(format!("differs only in case from {other}"));
    assert_eq!(
        conflicts,
        [
            clash("people/Mom.md"),
            clash("imported/Previous"),
            clash("imported/Previous"),
            None,
            clash("imported/Previous/Tea.md"),
            Some("already exists in the vault".into()),
            None,
            None,
            folds.then(|| {
                "is the same file as imported/Previous/Stra\u{df}e.md on this volume".into()
            }),
            None,
            Some("collides with a file or folder of another path on this volume".into()),
        ]
    );
}
