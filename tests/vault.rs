use oppa::vault::{CommitFailure, CommitReceipt, Edit, Provenance, Vault};
use serde_json::json;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

fn fixture() -> (TempDir, Vault) {
    let temporary = TempDir::new().unwrap();
    let vault = Vault::init(&temporary.path().join("vault")).unwrap();
    (temporary, vault)
}

fn provenance() -> Provenance {
    Provenance {
        author: "Synthetic Owner".into(),
        source: "synthetic-test".into(),
        run_id: "test-run-1".into(),
        reason: "Record synthetic notes".into(),
    }
}

fn write(path: &str, content: &str) -> Edit {
    Edit::Write {
        path: path.into(),
        content: content.into(),
    }
}

fn git(root: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn journal(vault: &Vault, receipt: &CommitReceipt, changes: serde_json::Value) {
    fs::write(
        vault.root().join(".git/oppa-vault-transaction.json"),
        serde_json::to_vec(&json!({
            "receipt": receipt,
            "changes": changes,
        }))
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn atomic_multifile_commit_has_retrievable_provenance() {
    let (_temporary, vault) = fixture();
    let base = vault.revision().unwrap();
    let receipt = vault
        .commit(
            &base,
            &[
                write("notes/a.md", "# Synthetic A"),
                write("b.md", "# Synthetic B"),
            ],
            &provenance(),
        )
        .unwrap();
    assert_ne!(base, receipt.revision);
    assert_eq!(base, receipt.parent_revision);
    assert_eq!(vault.read("notes/a.md").unwrap(), "# Synthetic A");
    assert_eq!(vault.history(10).unwrap(), vec![receipt]);
    assert_eq!(git(vault.root(), &["rev-list", "--count", "HEAD"]), "2");
    assert!(git(vault.root(), &["status", "--porcelain"]).is_empty());
    assert!(git(vault.root(), &["log", "-1", "--format=%B"]).contains("Oppa-Run: test-run-1"));
    assert_eq!(
        Vault::open(vault.root()).unwrap().revision().unwrap(),
        vault.revision().unwrap()
    );
}

#[test]
fn stale_revision_and_concurrent_writers_cannot_lose_updates() {
    let (_temporary, vault) = fixture();
    let base = vault.revision().unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let handles: Vec<_> = (0..2)
        .map(|number| {
            let vault = vault.clone();
            let barrier = barrier.clone();
            let base = base.clone();
            std::thread::spawn(move || {
                barrier.wait();
                vault.commit(
                    &base,
                    &[write(&format!("note-{number}.md"), "synthetic")],
                    &provenance(),
                )
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let error = results
        .iter()
        .find_map(|result| result.as_ref().err())
        .unwrap();
    assert_eq!(
        error.downcast_ref::<CommitFailure>(),
        Some(&CommitFailure::Rejected)
    );
    assert!(format!("{error:#}").contains("stale"));
    assert_eq!(vault.search("synthetic").unwrap().len(), 1);
    assert_eq!(vault.history(10).unwrap().len(), 1);
}

#[test]
fn preserves_unrelated_owner_edits_and_refuses_affected_or_staged_work() {
    let (_temporary, vault) = fixture();
    let receipt = vault
        .commit(
            &vault.revision().unwrap(),
            &[write("a.md", "a"), write("b.md", "b")],
            &provenance(),
        )
        .unwrap();
    fs::write(vault.root().join("b.md"), "owner draft").unwrap();
    fs::write(vault.root().join("untracked.md"), "untracked owner draft").unwrap();
    vault.check_ready().unwrap();
    let edited = vault
        .commit(
            &receipt.revision,
            &[write("a.md", "updated")],
            &provenance(),
        )
        .unwrap();
    assert_eq!(vault.read("b.md").unwrap(), "owner draft");
    assert_eq!(git(vault.root(), &["show", "HEAD:b.md"]), "b");
    assert!(git(vault.root(), &["ls-tree", "--name-only", "HEAD"])
        .lines()
        .all(|path| path != "untracked.md"));
    let error = vault
        .commit(
            &edited.revision,
            &[write("b.md", "overwrite")],
            &provenance(),
        )
        .unwrap_err();
    assert!(format!("{error:#}").contains("dirty file"));
    git(vault.root(), &["add", "b.md"]);
    let error = vault
        .commit(&edited.revision, &[write("a.md", "next")], &provenance())
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<CommitFailure>(),
        Some(&CommitFailure::Rejected)
    );
    assert!(format!("{error:#}").contains("staged"));
    assert_eq!(git(vault.root(), &["show", ":b.md"]), "owner draft");
}

#[test]
fn reject_path_escapes_hidden_paths_and_duplicate_edits() {
    let (_temporary, vault) = fixture();
    let base = vault.revision().unwrap();
    for path in [
        "../outside.md",
        "/tmp/outside.md",
        ".git/config.md",
        "notes/../outside.md",
        "./a.md",
        "notes/.secret.md",
        "notes//a.md",
        "bad.txt",
        "x\\a.md",
        // Longer than a receipt may name, in folders short enough to create.
        &format!("{}/b.md", vec!["a".repeat(255); 4].join("/")),
    ] {
        assert!(
            vault
                .commit(&base, &[write(path, "x")], &provenance())
                .is_err(),
            "accepted {path}"
        );
        assert!(vault.read(path).is_err(), "read accepted {path}");
    }
    assert!(vault
        .commit(
            &base,
            &[write("a.md", "a"), write("a.md", "b")],
            &provenance()
        )
        .is_err());
    assert_eq!(vault.revision().unwrap(), base);
}

#[cfg(unix)]
#[test]
fn denies_symlinks_and_hardlinks_without_touching_the_target() {
    use std::os::unix::fs::symlink;
    let (temporary, vault) = fixture();
    let outside = temporary.path().join("outside.md");
    fs::write(&outside, "private synthetic outside").unwrap();
    symlink(&outside, vault.root().join("link.md")).unwrap();
    symlink(temporary.path(), vault.root().join("directory")).unwrap();
    fs::hard_link(&outside, vault.root().join("hard.md")).unwrap();
    for path in ["link.md", "directory/outside.md", "hard.md"] {
        assert!(vault.read(path).is_err());
        assert!(vault
            .commit(
                &vault.revision().unwrap(),
                &[write(path, "overwrite")],
                &provenance()
            )
            .is_err());
    }
    assert_eq!(
        fs::read_to_string(outside).unwrap(),
        "private synthetic outside"
    );
}

#[test]
fn searches_short_english_and_chinese_terms_with_or_ranking() {
    let (_temporary, vault) = fixture();
    vault
        .commit(
            &vault.revision().unwrap(),
            &[
                write("a.md", "# 苹果财报\nAI 苹果 最新报告"),
                write("b.md", "# Schedule\nAI system"),
                write("c.md", "# Travel\n飞机"),
            ],
            &provenance(),
        )
        .unwrap();
    let hits = vault.search("AI 苹果").unwrap();
    assert_eq!(
        hits.iter().map(|hit| hit.path.as_str()).collect::<Vec<_>>(),
        vec!["a.md", "b.md"]
    );
    assert_eq!(vault.search("苹果财报").unwrap()[0].path, "a.md");
    assert_eq!(vault.search("机").unwrap()[0].path, "c.md");
    fs::write(vault.root().join("owner.md"), "uncommitted testterm").unwrap();
    assert_eq!(vault.search("testterm").unwrap()[0].path, "owner.md");
    assert!(vault.search(" ").unwrap().is_empty());
}

#[test]
fn a_note_is_found_by_its_path() {
    // Incident 2026-09-28: a delegated summary of what memory held about the
    // owner said memory was empty, because no note text said "owner".
    let (_temporary, vault) = fixture();
    vault
        .commit(
            &vault.revision().unwrap(),
            &[write("people/owner/profile.md", "Likes black coffee.")],
            &provenance(),
        )
        .unwrap();
    let hits = vault.search("owner").unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].path, "people/owner/profile.md");
    assert_eq!(hits[0].excerpt, "Likes black coffee.");
}

#[test]
fn revert_is_a_new_commit_preserves_unrelated_edits_and_detects_conflicts() {
    let (_temporary, vault) = fixture();
    let first = vault
        .commit(
            &vault.revision().unwrap(),
            &[write("a.md", "first")],
            &provenance(),
        )
        .unwrap();
    let second = vault
        .commit(
            &first.revision,
            &[write("a.md", "second"), write("b.md", "added")],
            &provenance(),
        )
        .unwrap();
    fs::write(vault.root().join("owner.md"), "unrelated draft").unwrap();
    let reverted = vault
        .revert(&second.revision, &second.revision, &provenance())
        .unwrap();
    assert_eq!(reverted.reverted_commit, Some(second.revision.clone()));
    assert_eq!(vault.read("a.md").unwrap(), "first");
    assert!(vault.read("b.md").is_err());
    assert_eq!(vault.read("owner.md").unwrap(), "unrelated draft");
    assert_eq!(
        git(
            vault.root(),
            &["show", &format!("{}:a.md", second.revision)]
        ),
        "second"
    );
    vault
        .commit(&reverted.revision, &[write("a.md", "later")], &provenance())
        .unwrap();
    assert!(vault
        .revert(&vault.revision().unwrap(), &first.revision, &provenance())
        .unwrap_err()
        .to_string()
        .contains("later committed"));
}

#[test]
fn interrupted_precommit_transaction_rolls_back_all_files() {
    let (_temporary, vault) = fixture();
    let receipt = vault
        .commit(
            &vault.revision().unwrap(),
            &[write("a.md", "after"), write("b.md", "after")],
            &provenance(),
        )
        .unwrap();
    git(vault.root(), &["reset", "--hard", &receipt.parent_revision]);
    // Crash after the first working file, before HEAD was advanced.
    fs::write(vault.root().join("a.md"), "after").unwrap();
    journal(
        &vault,
        &receipt,
        json!([
            {"path": "a.md", "before": null, "after": "after"},
            {"path": "b.md", "before": null, "after": "after"}
        ]),
    );
    assert!(vault
        .check_ready()
        .unwrap_err()
        .to_string()
        .contains("interrupted"));
    assert!(vault
        .read("a.md")
        .unwrap_err()
        .to_string()
        .contains("interrupted"));
    assert_eq!(vault.recover().unwrap(), None);
    vault.check_ready().unwrap();
    assert!(!vault.root().join("a.md").exists());
    assert!(!vault.root().join("b.md").exists());
    assert_eq!(vault.revision().unwrap(), receipt.parent_revision);
    assert!(git(vault.root(), &["status", "--porcelain"]).is_empty());
}

#[test]
fn interrupted_postcommit_transaction_finishes_index_without_changing_owner_files() {
    let (_temporary, vault) = fixture();
    let receipt = vault
        .commit(
            &vault.revision().unwrap(),
            &[write("a.md", "after")],
            &provenance(),
        )
        .unwrap();
    git(vault.root(), &["read-tree", &receipt.parent_revision]);
    fs::write(vault.root().join("owner.md"), "draft").unwrap();
    journal(
        &vault,
        &receipt,
        json!([{ "path": "a.md", "before": null, "after": "after" }]),
    );
    assert_eq!(vault.recover().unwrap(), Some(receipt));
    assert_eq!(vault.read("owner.md").unwrap(), "draft");
    assert_eq!(git(vault.root(), &["status", "--porcelain"]), "?? owner.md");
}

#[test]
fn recovery_refuses_owner_changes_before_touching_any_files() {
    let (_temporary, vault) = fixture();
    let receipt = vault
        .commit(
            &vault.revision().unwrap(),
            &[write("a.md", "after"), write("b.md", "after")],
            &provenance(),
        )
        .unwrap();
    git(vault.root(), &["reset", "--hard", &receipt.parent_revision]);
    fs::write(vault.root().join("a.md"), "after").unwrap();
    fs::write(vault.root().join("b.md"), "owner recovery draft").unwrap();
    journal(
        &vault,
        &receipt,
        json!([
            {"path": "a.md", "before": null, "after": "after"},
            {"path": "b.md", "before": null, "after": "after"}
        ]),
    );
    assert!(vault
        .recover()
        .unwrap_err()
        .to_string()
        .contains("owner edit"));
    assert_eq!(
        fs::read_to_string(vault.root().join("a.md")).unwrap(),
        "after"
    );
    assert_eq!(
        fs::read_to_string(vault.root().join("b.md")).unwrap(),
        "owner recovery draft"
    );
}

#[test]
fn failed_ref_update_rolls_back_worktree_and_keeps_head() {
    let (_temporary, vault) = fixture();
    let base = vault.revision().unwrap();
    fs::write(
        vault.root().join(".git/refs/heads/main.lock"),
        "synthetic external lock",
    )
    .unwrap();
    let error = vault
        .commit(&base, &[write("a.md", "synthetic")], &provenance())
        .unwrap_err();
    assert!(error.to_string().contains("rolled back"));
    assert_eq!(vault.revision().unwrap(), base);
    assert!(!vault.root().join("a.md").exists());
    assert!(!vault
        .root()
        .join(".git/oppa-vault-transaction.json")
        .exists());
}

#[test]
fn failed_index_update_retains_receipt_and_recovers_without_replay() {
    let (_temporary, vault) = fixture();
    let base = vault.revision().unwrap();
    let index_lock = vault.root().join(".git/index.lock");
    fs::write(&index_lock, "synthetic external index lock").unwrap();
    let error = vault
        .commit(&base, &[write("a.md", "synthetic")], &provenance())
        .unwrap_err();
    assert!(error.downcast_ref::<CommitFailure>() == Some(&CommitFailure::NeedsRecovery));
    let committed_revision = vault.revision().unwrap();
    assert_ne!(committed_revision, base);
    assert!(vault
        .read("a.md")
        .unwrap_err()
        .to_string()
        .contains("interrupted"));
    fs::remove_file(index_lock).unwrap();
    let receipt = vault.recover().unwrap().unwrap();
    assert_eq!(receipt.revision, committed_revision);
    assert_eq!(vault.read("a.md").unwrap(), "synthetic");
    assert_eq!(vault.history(10).unwrap(), vec![receipt]);
    assert!(git(vault.root(), &["status", "--porcelain"]).is_empty());
}

#[test]
fn git_pathspec_metacharacters_are_literal_note_names() {
    let (_temporary, vault) = fixture();
    let first = vault
        .commit(
            &vault.revision().unwrap(),
            &[write("a*.md", "literal"), write("abc.md", "other")],
            &provenance(),
        )
        .unwrap();
    vault
        .commit(&first.revision, &[write("a*.md", "updated")], &provenance())
        .unwrap();
    assert_eq!(vault.read("a*.md").unwrap(), "updated");
    assert_eq!(vault.read("abc.md").unwrap(), "other");
}

#[test]
fn refuses_initialization_over_existing_files_and_provenance_injection() {
    let temporary = TempDir::new().unwrap();
    fs::write(temporary.path().join("owner.md"), "draft").unwrap();
    assert!(Vault::init(temporary.path()).is_err());
    assert_eq!(
        fs::read_to_string(temporary.path().join("owner.md")).unwrap(),
        "draft"
    );
    let (_temporary, vault) = fixture();
    let mut invalid = provenance();
    invalid.reason = "reason\nOppa-Author: attacker".into();
    assert!(vault
        .commit(&vault.revision().unwrap(), &[write("a.md", "x")], &invalid)
        .is_err());
}

#[test]
fn initialization_never_imports_environment_or_global_git_templates() {
    const CHILD: &str = "OPPA_TEST_VAULT_GIT_TEMPLATE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        // The child still creates its own TempDir: no test accepts a vault path
        // from the environment or ever opens the operator's actual records.
        let (_temporary, vault) = fixture();
        assert!(git(vault.root(), &["remote"]).is_empty());
        assert!(!vault.root().join(".git/template-marker").exists());
        return;
    }
    let temporary = TempDir::new().unwrap();
    let template = temporary.path().join("template");
    fs::create_dir(&template).unwrap();
    fs::write(
        template.join("config"),
        "[remote \"origin\"]\nurl = https://example.test/synthetic-public.git\n",
    )
    .unwrap();
    fs::write(template.join("template-marker"), "synthetic template data").unwrap();
    let global = temporary.path().join("global-config");
    fs::write(
        &global,
        format!("[init]\ntemplateDir = {}\n", template.display()),
    )
    .unwrap();
    for environment_template in [true, false] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "initialization_never_imports_environment_or_global_git_templates",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("GIT_CONFIG_GLOBAL", &global)
            .env("GIT_CONFIG_NOSYSTEM", "1");
        if environment_template {
            command.env("GIT_TEMPLATE_DIR", &template);
        } else {
            command.env_remove("GIT_TEMPLATE_DIR");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "template selection via environment={environment_template}: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn reason_text_cannot_create_host_receipt_metadata() {
    let (_temporary, vault) = fixture();
    let mut source = provenance();
    source.reason = format!("Oppa-Reverts: {}", "a".repeat(40));
    let receipt = vault
        .commit(
            &vault.revision().unwrap(),
            &[write("note.md", "synthetic")],
            &source,
        )
        .unwrap();
    assert!(receipt.reverted_commit.is_none());
    assert_eq!(vault.history(10).unwrap(), vec![receipt]);
}

#[test]
fn foreign_malformed_and_merge_commits_do_not_break_managed_history() {
    let (_temporary, vault) = fixture();
    let initial = vault.revision().unwrap();
    let receipt = vault
        .commit(&initial, &[write("note.md", "synthetic")], &provenance())
        .unwrap();
    for message in [
        "Oppa-Author: not a receipt",
        "Owner note\n\nOppa-Author: partial",
    ] {
        git(
            vault.root(),
            &[
                "-c",
                "user.name=Synthetic Owner",
                "-c",
                "user.email=owner@example.test",
                "commit",
                "--allow-empty",
                "-m",
                message,
            ],
        );
    }
    let parent = vault.revision().unwrap();
    let tree = git(vault.root(), &["rev-parse", "HEAD^{tree}"]);
    let message = git(
        vault.root(),
        &["show", "-s", "--format=%B", &receipt.revision],
    );
    let merge = git(
        vault.root(),
        &[
            "-c",
            "user.name=Synthetic Owner",
            "-c",
            "user.email=owner@example.test",
            "commit-tree",
            &tree,
            "-p",
            &parent,
            "-p",
            &initial,
            "-m",
            &message,
        ],
    );
    git(vault.root(), &["update-ref", "HEAD", &merge, &parent]);
    assert_eq!(vault.history(10).unwrap(), vec![receipt]);
}

#[test]
fn edits_one_file_on_this_volume_are_refused_before_any_write() {
    // A case-insensitive volume, the macOS default, stores each pair as one
    // file: APFS folds case, ß and ss, and composed and decomposed é. Admitting
    // both left a transaction that could neither finish nor roll back. Case
    // alone is refused everywhere; the rest only where the volume folds it.
    let pairs = [
        ("notes/Tea.md", "notes/tea.md"),
        ("notes/Stra\u{df}e.md", "notes/Strasse.md"),
        ("notes/caf\u{e9}.md", "notes/cafe\u{301}.md"),
    ];
    for (index, (first, second)) in pairs.into_iter().enumerate() {
        let (temporary, vault) = fixture();
        let volume = temporary.path().join("volume");
        fs::create_dir(&volume).unwrap();
        fs::write(volume.join(first.replace('/', "-")), "").unwrap();
        let one_file = index == 0 || volume.join(second.replace('/', "-")).exists();
        let base = vault.revision().unwrap();
        let edits = [write(first, "first"), write(second, "second")];
        let result = vault.commit(&base, &edits, &provenance());
        vault.check_ready().unwrap();
        if one_file {
            let error = result.unwrap_err();
            assert_eq!(
                error.downcast_ref::<CommitFailure>(),
                Some(&CommitFailure::Rejected),
                "{first} and {second}: {error:#}"
            );
            assert_eq!(vault.revision().unwrap(), base);
            assert!(!vault.root().join("notes").exists());
        } else {
            result.unwrap();
            assert_eq!(vault.read(second).unwrap(), "second");
        }
    }
}

#[test]
fn a_note_path_near_the_system_limit_is_written() {
    // The one-file check lays paths out below a scratch folder in .git. That
    // prefix must not count toward the system's limit (1,024 bytes on macOS),
    // or a note the vault itself can hold would be refused.
    let (_temporary, vault) = fixture();
    let room = 1015 - vault.root().as_os_str().len() - 1;
    let mut path = String::new();
    while room - path.len() > 250 {
        path += &"a".repeat(200);
        path.push('/');
    }
    path += &"b".repeat(room - path.len() - 3);
    path += ".md";
    let base = vault.revision().unwrap();
    vault
        .commit(&base, &[write(&path, "near the limit")], &provenance())
        .unwrap();
    assert_eq!(vault.read(&path).unwrap(), "near the limit");
}
