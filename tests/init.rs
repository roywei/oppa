use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    os::fd::FromRawFd,
    path::{Path, PathBuf},
    process::{Command, Output},
};

fn clean_command(program: &str) -> Command {
    let mut command = Command::new(program);
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_")
            || key == "OPPA_HOME"
            || key == "OPPA_PRIVATE_DIR"
        {
            command.env_remove(key);
        }
    }
    command
}

fn git(root: &Path, home: &Path, args: &[&str]) -> Output {
    clean_command("git")
        .env("HOME", home)
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "commit.gpgsign=false",
        ])
        .arg("-C")
        .arg(root)
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

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, current: &Path, entries: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(current).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                walk(root, &path, entries);
            } else {
                entries.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut entries = BTreeMap::new();
    walk(root, root, &mut entries);
    entries
}

fn oppa_init(runtime: &Path, private: &Path) -> Command {
    let mut command = clean_command(env!("CARGO_BIN_EXE_oppa"));
    command
        .arg("--home")
        .arg(runtime)
        .arg("--private-dir")
        .arg(private)
        .args(["init", "--agent", "codex", "--owner", "synthetic-owner"]);
    command
}

#[test]
fn init_ignores_git_redirects_and_environment_or_global_templates() {
    for environment_template in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let home = dir.path().join("fake-home");
        let unrelated = dir.path().join("unrelated-source");
        let template = dir.path().join("hostile-template");
        let runtime = dir.path().join("runtime");
        let private = dir.path().join("private");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&unrelated).unwrap();
        fs::create_dir_all(template.join("hooks")).unwrap();
        assert_success(&git(
            &unrelated,
            &home,
            &["init", "--template=", "-b", "main"],
        ));
        fs::write(unrelated.join("keep.txt"), "synthetic unrelated source\n").unwrap();
        assert_success(&git(&unrelated, &home, &["add", "keep.txt"]));
        assert_success(&git(
            &unrelated,
            &home,
            &[
                "-c",
                "user.name=Synthetic",
                "-c",
                "user.email=synthetic@example.test",
                "commit",
                "-m",
                "Initial fixture",
            ],
        ));
        let before = snapshot(&unrelated);
        fs::write(
            template.join("config"),
            "[remote \"origin\"]\n\turl = https://example.invalid/unwanted.git\n",
        )
        .unwrap();
        fs::write(template.join("hooks/pre-commit"), "#!/bin/sh\nexit 97\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                template.join("hooks/pre-commit"),
                fs::Permissions::from_mode(0o755),
            )
            .unwrap();
        }
        fs::write(home.join(".gitconfig"), format!("[init]\n\ttemplateDir = {}\n[commit]\n\tgpgSign = true\n[gpg]\n\tprogram = /definitely/missing/synthetic-signer\n[core]\n\thooksPath = {}\n", template.display(), template.join("hooks").display())).unwrap();
        let mut command = oppa_init(&runtime, &private);
        command
            .env("HOME", &home)
            .env("GIT_DIR", unrelated.join(".git"))
            .env("GIT_WORK_TREE", &unrelated)
            .env("GIT_INDEX_FILE", unrelated.join(".git/index"));
        if environment_template {
            command.env("GIT_TEMPLATE_DIR", &template);
        }
        let output = command.output().unwrap();
        assert_success(&output);
        assert_eq!(
            snapshot(&unrelated),
            before,
            "init modified the unrelated repository"
        );
        for repository in [&private, &private.join("vault")] {
            let remotes = git(repository, &home, &["remote"]);
            assert_success(&remotes);
            assert!(
                remotes.stdout.is_empty(),
                "a private repository inherited a remote"
            );
            let root = git(repository, &home, &["rev-parse", "--show-toplevel"]);
            assert_success(&root);
            assert_eq!(
                Path::new(String::from_utf8_lossy(&root.stdout).trim())
                    .canonicalize()
                    .unwrap(),
                repository.canonicalize().unwrap()
            );
            assert!(!repository.join(".git/hooks/pre-commit").exists());
        }
        assert!(runtime.join("state.db").is_file());
        let config = fs::read_to_string(private.join("config.toml")).unwrap();
        assert!(config.contains("synthetic-owner"));
        assert!(config.contains("runtime/adapters/codex.py"));
        assert!(!runtime.join(".git").exists());
        assert!(!private.join("state.db").exists());
    }
}

#[test]
fn explicit_directories_work_without_home_and_second_init_preserves_data() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("runtime");
    let private = dir.path().join("private");
    let output = oppa_init(&runtime, &private)
        .env_remove("HOME")
        .output()
        .unwrap();
    assert_success(&output);
    let private_before = snapshot(&private);
    let runtime_before = snapshot(&runtime);
    let retry = oppa_init(&runtime, &private)
        .env_remove("HOME")
        .output()
        .unwrap();
    assert!(!retry.status.success());
    assert_eq!(snapshot(&private), private_before);
    assert_eq!(snapshot(&runtime), runtime_before);
}

#[test]
fn a_process_that_runs_turns_reinstalls_bundled_adapters_that_drifted() {
    // Incident: an upgraded core met adapters an older build had installed, and
    // every turn failed until they were refreshed by hand.
    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("runtime");
    let private = dir.path().join("private");
    assert_success(&oppa_init(&runtime, &private).output().unwrap());
    let installed = runtime.join("adapters/common.py");
    fs::write(&installed, "# installed by an older build\n").unwrap();
    // The channel adapter is bundled too, so a live channel never runs a script
    // from a source checkout that may be on another branch.
    let channel = runtime.join("adapters/imessage.py");
    fs::write(&channel, "# installed by an older build\n").unwrap();
    let oppa = |args: &[&str]| {
        clean_command(env!("CARGO_BIN_EXE_oppa"))
            .arg("--home")
            .arg(&runtime)
            .arg("--private-dir")
            .arg(&private)
            .args(args)
            .output()
            .unwrap()
    };
    let adapters_check = || {
        let checks: serde_json::Value = serde_json::from_slice(&oppa(&["doctor"]).stdout).unwrap();
        checks
            .as_array()
            .unwrap()
            .iter()
            .find(|check| check["check"] == "bundled_adapters")
            .unwrap()
            .clone()
    };
    let drifted = adapters_check();
    assert_eq!(drifted["ready"], false);
    assert_eq!(
        drifted["stale"],
        serde_json::json!(["common.py", "imessage.py"])
    );
    assert_success(&oppa(&["tasks", "present", "--conversation", "console"]));
    assert_eq!(
        fs::read_to_string(&installed).unwrap(),
        include_str!("../resources/adapters/common.py")
    );
    assert_eq!(
        fs::read_to_string(&channel).unwrap(),
        include_str!("../resources/adapters/imessage.py")
    );
    assert_eq!(adapters_check()["ready"], true);
}

#[test]
fn channel_and_people_tables_appended_to_a_new_configuration_parse() {
    // Incident 2026-09-30: init wrote `channels = []`, so appending a
    // `[[channels]]` table as the README says made the file invalid TOML.
    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("runtime");
    let private = dir.path().join("private");
    assert_success(&oppa_init(&runtime, &private).output().unwrap());
    let config = private.join("config.toml");
    let mut text = fs::read_to_string(&config).unwrap();
    text.push_str(
        "\n[[channels]]\nid = \"imessage\"\naccount = \"assistant\"\n\
         command = [\"/usr/bin/true\"]\n\n[[people]]\nid = \"synthetic-owner\"\n\
         role = \"owner\"\nhandles = [\"imessage:+15555550100\"]\n",
    );
    fs::write(&config, &text).unwrap();
    let load = || {
        clean_command(env!("CARGO_BIN_EXE_oppa"))
            .arg("--home")
            .arg(&runtime)
            .arg("--private-dir")
            .arg(&private)
            .arg("connectors")
            .output()
            .unwrap()
    };
    assert_success(&load());
    // G-3: a configuration from before the people registry is refused with how
    // to move the owner's handles, never read as having no owner handles.
    let old = text.replace(
        "command = [\"/usr/bin/true\"]\n",
        "command = [\"/usr/bin/true\"]\nowner_handles = [\"+15555550100\"]\n",
    );
    fs::write(&config, old).unwrap();
    let refused = load();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("move them to a [[people]] entry"));
}

#[test]
fn the_readme_channel_examples_load_in_the_order_it_gives_them() {
    // Review 2026-10-05: the iMessage example listed "web:owner" before any
    // web channel, and adding it again for the web channel listed it twice.
    let blocks: Vec<&str> = include_str!("../README.md")
        .split("```toml\n")
        .skip(1)
        .map(|block| block.split("```").next().unwrap())
        .collect();
    // The iMessage channel and people; init already wrote `[sessions]`.
    let imessage = blocks[0].split("[sessions]").next().unwrap();
    let web = blocks[1];
    assert!(imessage.contains("id = \"imessage\"") && web.contains("id = \"web\""));
    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("runtime");
    let private = dir.path().join("private");
    assert_success(&oppa_init(&runtime, &private).output().unwrap());
    let config = private.join("config.toml");
    let load = |text: &str| {
        fs::write(&config, text).unwrap();
        clean_command(env!("CARGO_BIN_EXE_oppa"))
            .arg("--home")
            .arg(&runtime)
            .arg("--private-dir")
            .arg(&private)
            .arg("connectors")
            .output()
            .unwrap()
    };
    // The README's owner entry uses the id init chooses by default.
    let text = fs::read_to_string(&config)
        .unwrap()
        .replace("owner = \"synthetic-owner\"", "owner = \"owner\"")
        + imessage;
    assert_success(&load(&text));
    // Then the web channel, and "web:owner" in the owner's handles.
    let owner = "id = \"owner\"\nrole = \"owner\"\nhandles = [";
    assert_eq!(text.matches(owner).count(), 1);
    let text = text.replace(owner, &format!("{owner}\"web:owner\", ")) + web;
    assert_success(&load(&text));
}

#[test]
fn init_records_the_access_the_owner_chose() {
    // C-5, S-3: an owner who chooses reads only never has a turn at the default.
    let dir = tempfile::tempdir().unwrap();
    let (runtime, private) = (dir.path().join("runtime"), dir.path().join("private"));
    assert_success(
        &oppa_init(&runtime, &private)
            .args(["--access", "read"])
            .output()
            .unwrap(),
    );
    let config = fs::read_to_string(private.join("config.toml")).unwrap();
    assert!(config.contains("access = \"read\""), "{config}");
    // A script chooses full access, the model, the
    // effort, no browser and the vault by flag.
    let dir = tempfile::tempdir().unwrap();
    let (runtime, private) = (dir.path().join("runtime"), dir.path().join("private"));
    let flags = [
        "--access",
        "full",
        "--model",
        "gpt-synthetic-1",
        "--effort",
        "high",
        "--no-browser",
        "--vault",
        "OPPA",
    ];
    assert_success(&oppa_init(&runtime, &private).args(flags).output().unwrap());
    let config = oppa::config::Paths::new(runtime, Some(private))
        .unwrap()
        .load()
        .unwrap();
    assert_eq!(config.access, oppa::harness::Access::Computer);
    assert_eq!(config.agents[0].model.as_deref(), Some("gpt-synthetic-1"));
    assert_eq!(config.agents[0].effort.as_deref(), Some("high"));
    assert!(!config.browser);
    assert_eq!(config.connectors[0].command[2..], ["--vault", "OPPA"]);
    assert!(
        config.memory_sync.is_some(),
        "full access syncs memory daily"
    );
}

#[test]
fn settings_report_what_the_dashboards_configuration_card_shows() {
    // The dashboard shows the model and effort, the
    // browser, the skills the assistant built, the knowledge wiki and the
    // memory sync, from `oppa settings`.
    let dir = tempfile::tempdir().unwrap();
    let (runtime, private) = (dir.path().join("runtime"), dir.path().join("private"));
    let flags = [
        "--access",
        "full",
        "--model",
        "synthetic-model-1",
        "--effort",
        "high",
        "--no-browser",
    ];
    assert_success(&oppa_init(&runtime, &private).args(flags).output().unwrap());
    let skill = private.join("skills/tea-timer");
    fs::create_dir_all(&skill).unwrap();
    fs::write(skill.join("SKILL.md"), "description: Times a pot of tea.\n").unwrap();
    let output = clean_command(env!("CARGO_BIN_EXE_oppa"))
        .arg("--home")
        .arg(&runtime)
        .arg("--private-dir")
        .arg(&private)
        .arg("settings")
        .output()
        .unwrap();
    assert_success(&output);
    let shown: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(shown["agents"][0]["model"], "synthetic-model-1");
    assert_eq!(shown["agents"][0]["effort"], "high");
    assert_eq!(shown["browser"], false);
    assert_eq!(shown["memory_sync"], "03:30");
    assert_eq!(
        shown["skills"],
        serde_json::json!([{"name":"tea-timer","description":"Times a pot of tea."}])
    );
    assert_eq!(
        shown["knowledge"],
        private
            .join("knowledge")
            .canonicalize()
            .unwrap()
            .display()
            .to_string()
    );
}

#[test]
fn init_at_full_access_links_or_starts_the_knowledge_wiki() {
    // Public knowledge lives in a wiki of its own,
    // apart from the personal vault; an owner who keeps one links it.
    let dir = tempfile::tempdir().unwrap();
    let (runtime, private) = (dir.path().join("runtime"), dir.path().join("private"));
    assert_success(
        &oppa_init(&runtime, &private)
            .args(["--access", "full"])
            .output()
            .unwrap(),
    );
    let wiki = private.join("knowledge");
    assert!(fs::read_to_string(wiki.join("AGENTS.md"))
        .unwrap()
        .contains("never here"));
    assert!(wiki.join("wiki/index.md").is_file() && wiki.join("wiki/sources").is_dir());
    let log = git(&wiki, dir.path(), &["log", "--format=%s"]);
    assert_eq!(
        String::from_utf8_lossy(&log.stdout),
        "Start the knowledge wiki\n"
    );
    // The instance's own repository leaves the wiki to its own.
    let status = git(&private, dir.path(), &["status", "--porcelain"]);
    assert_eq!(String::from_utf8_lossy(&status.stdout), "");

    // A path that cannot be a wiki is refused before anything is written, so
    // init can run again.
    let existing = dir.path().join("brain");
    fs::create_dir(&existing).unwrap();
    let (runtime, private) = (dir.path().join("runtime2"), dir.path().join("private2"));
    for missing in [dir.path().join("nowhere"), existing.clone()] {
        let refused = oppa_init(&runtime, &private)
            .args(["--access", "full", "--knowledge"])
            .arg(&missing)
            .output()
            .unwrap();
        assert!(!refused.status.success());
        assert!(!private.exists() && !runtime.exists());
    }
    fs::write(existing.join("AGENTS.md"), "# Synthetic wiki\n").unwrap();
    assert_success(&git(&existing, dir.path(), &["init", "--quiet"]));
    assert_success(
        &oppa_init(&runtime, &private)
            .args(["--access", "full", "--knowledge"])
            .arg(&existing)
            .output()
            .unwrap(),
    );
    assert_eq!(
        fs::read_link(private.join("knowledge")).unwrap(),
        existing.canonicalize().unwrap()
    );

    // Below full access the model cannot open files, so there is no wiki.
    let (runtime, private) = (dir.path().join("runtime3"), dir.path().join("private3"));
    assert_success(
        &oppa_init(&runtime, &private)
            .args(["--access", "connectors"])
            .output()
            .unwrap(),
    );
    assert!(!private.join("knowledge").exists());
    let knowledge = |link: Option<&Path>| {
        let mut command = clean_command(env!("CARGO_BIN_EXE_oppa"));
        command
            .env("HOME", dir.path())
            .arg("--home")
            .arg(&runtime)
            .arg("--private-dir")
            .arg(&private)
            .arg("knowledge");
        if let Some(link) = link {
            command.arg("--link").arg(link);
        }
        command.output().unwrap()
    };
    let refused = knowledge(Some(&existing));
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("full access"));

    // At full access an existing instance starts one, and its own repository
    // ignores it in a commit, leaving nothing uncommitted.
    let config = private.join("config.toml");
    let text = fs::read_to_string(&config).unwrap();
    fs::write(
        &config,
        text.replace("access = \"connectors\"", "access = \"computer\""),
    )
    .unwrap();
    assert_success(&git(
        &private,
        dir.path(),
        &["commit", "--quiet", "-am", "Full access"],
    ));
    assert_success(&knowledge(None));
    assert!(private.join("knowledge/AGENTS.md").is_file());
    assert!(fs::read_to_string(private.join(".gitignore"))
        .unwrap()
        .lines()
        .any(|line| line == "/knowledge"));
    let status = git(&private, dir.path(), &["status", "--porcelain"]);
    assert_eq!(String::from_utf8_lossy(&status.stdout), "");
    // A second wiki is refused while one is there; moved away, a link takes its place.
    assert!(!knowledge(Some(&existing)).status.success());
    fs::rename(private.join("knowledge"), dir.path().join("old-wiki")).unwrap();
    assert_success(&knowledge(Some(&existing)));
    assert_eq!(
        fs::read_link(private.join("knowledge")).unwrap(),
        existing.canonicalize().unwrap()
    );
    // A leading ~/ is the home folder, as typed at a terminal.
    fs::remove_file(private.join("knowledge")).unwrap();
    assert_success(&knowledge(Some(Path::new("~/brain"))));
    assert_eq!(
        fs::read_link(private.join("knowledge")).unwrap(),
        existing.canonicalize().unwrap()
    );
    // A folder inside the private directory is refused, and so is one that
    // holds it.
    let inside = private.join("evals");
    fs::write(inside.join("AGENTS.md"), "# Not a wiki\n").unwrap();
    assert_success(&git(&inside, dir.path(), &["init", "--quiet"]));
    fs::remove_file(private.join("knowledge")).unwrap();
    assert!(!knowledge(Some(&inside)).status.success());
    fs::write(dir.path().join("AGENTS.md"), "# Not a wiki\n").unwrap();
    assert_success(&git(dir.path(), dir.path(), &["init", "--quiet"]));
    assert!(!knowledge(Some(dir.path())).status.success());
}

#[test]
fn init_writes_the_channels_the_owner_chose_and_refuses_a_handle_no_sender_has() {
    // X-7, plan slice 6: where to talk is chosen at init, so the owner edits no
    // file for it. Each chosen channel is written with the owner's handles on
    // it, and the configuration loads.
    let dir = tempfile::tempdir().unwrap();
    let (runtime, private) = (dir.path().join("runtime"), dir.path().join("private"));
    assert_success(
        &oppa_init(&runtime, &private)
            .args(["--web", "--imessage", "+15555550100,owner@example.com"])
            .output()
            .unwrap(),
    );
    let config = oppa::config::Paths::new(runtime.clone(), Some(private.clone()))
        .unwrap()
        .load()
        .unwrap();
    let channels: Vec<_> = config
        .channels
        .iter()
        .map(|c| (c.id.as_str(), c.account.as_str(), c.command[0].as_str()))
        .collect();
    assert_eq!(
        channels,
        [
            ("imessage", "assistant", "/usr/bin/python3"),
            ("web", "local", "/usr/bin/python3")
        ]
    );
    let web = config.channels[1].command.join(" ");
    let adapter = runtime.join("adapters/web.py");
    assert!(web.contains(&format!("{} ", adapter.display())), "{web}");
    assert!(
        web.ends_with("--port 8765") && !web.contains("--oppa"),
        "{web}"
    );
    assert_eq!(config.people.len(), 1);
    assert_eq!(
        (config.people[0].id.as_str(), config.people[0].role),
        ("synthetic-owner", oppa::config::Role::Owner)
    );
    assert_eq!(
        config.people[0].handles,
        [
            "imessage:+15555550100",
            "imessage:owner@example.com",
            "web:owner"
        ]
    );

    // A number as it is often typed would never match a sender, so init
    // refuses it and creates nothing.
    let other = dir.path().join("other");
    let refused = oppa_init(&other.join("runtime"), &other.join("private"))
        .args(["--imessage", "(555) 555-0100"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("+15555550100"));
    assert!(!other.exists());
    // So is an email address no sender can have.
    for malformed in ["a@", "@example.com", "owner@example", "owner@@example.com"] {
        let refused = oppa_init(&other.join("runtime"), &other.join("private"))
            .args(["--imessage", malformed])
            .output()
            .unwrap();
        assert!(!refused.status.success(), "{malformed} was accepted");
        assert!(!other.exists());
    }
}

#[test]
fn init_at_a_terminal_asks_where_to_talk_and_writes_the_answers() {
    // X-7: the owner at a terminal answers the questions instead of editing a
    // file. Declining web chat and typing two handles with stray spaces
    // writes only the iMessage channel, with both handles trimmed.
    let dir = tempfile::tempdir().unwrap();
    let (runtime, private) = (dir.path().join("runtime"), dir.path().join("private"));
    let (mut leader, mut follower) = (0, 0);
    let opened = unsafe {
        libc::openpty(
            &mut leader,
            &mut follower,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(opened, 0);
    let terminal = || unsafe { std::process::Stdio::from_raw_fd(libc::dup(follower)) };
    let mut child = oppa_init(&runtime, &private)
        .stdin(terminal())
        .stdout(terminal())
        .stderr(terminal())
        .spawn()
        .unwrap();
    unsafe { libc::close(follower) };
    // The terminal keeps typed lines until init reads them, one per question.
    let mut keyboard = unsafe { fs::File::from_raw_fd(leader) };
    // Access, the browser, web chat, iMessage handles, the vault.
    keyboard
        .write_all(b"connectors\nn\nn\n +15555550100 , owner@example.com \nOPPA\n")
        .unwrap();
    // Reading the screen until init closes it keeps its writes from blocking.
    let mut screen = Vec::new();
    let _ = keyboard.read_to_end(&mut screen);
    let status = child.wait().unwrap();
    let screen = String::from_utf8_lossy(&screen);
    assert!(status.success(), "{screen}");
    assert!(
        screen.contains("[Y/n]") && screen.contains("iMessage"),
        "{screen}"
    );
    let config = oppa::config::Paths::new(runtime, Some(private))
        .unwrap()
        .load()
        .unwrap();
    let channels: Vec<_> = config.channels.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(channels, ["imessage"]);
    assert_eq!(
        config.people[0].handles,
        ["imessage:+15555550100", "imessage:owner@example.com"]
    );
    // The onboarding asks how far it may act,
    // whether it uses the browser and which vault it reads, and runs the
    // harness at medium effort.
    assert_eq!(config.access, oppa::harness::Access::Connectors);
    assert!(!config.browser);
    assert_eq!(config.connectors.len(), 1);
    assert_eq!(config.connectors[0].id, "secrets");
    assert_eq!(config.connectors[0].command[2..], ["--vault", "OPPA"]);
    assert!(
        config.connectors[0].owner_only,
        "an adult's turn never reads the vault"
    );
    assert_eq!(config.agents[0].effort.as_deref(), Some("medium"));
    // The memory sync needs a shell, so connectors access gets none.
    assert!(config.memory_sync.is_none());
    assert!(screen.contains("full/connectors/read") && screen.contains("browser"));
}

// X-6/X-7: Python alone cannot run a subscription harness. Diagnose the
// missing CLI before asking a new owner to chat; never invoke a login here.
#[test]
fn doctor_checks_the_selected_native_cli_not_just_python() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let runtime = dir.path().join("runtime");
    let private = dir.path().join("private");
    assert_success(&oppa_init(&runtime, &private).output().unwrap());
    let bin = dir.path().join("bin");
    fs::create_dir(&bin).unwrap();
    std::os::unix::fs::symlink("/usr/bin/python3", bin.join("python3")).unwrap();
    let doctor = || {
        clean_command(env!("CARGO_BIN_EXE_oppa"))
            .args([
                "--home",
                runtime.to_str().unwrap(),
                "--private-dir",
                private.to_str().unwrap(),
                "doctor",
            ])
            .env("PATH", &bin)
            .output()
            .unwrap()
    };
    let check = |output: &Output| {
        let checks: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        checks
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["check"] == "harness_cli")
            .expect("native CLI readiness check")
            .clone()
    };
    let missing = doctor();
    assert_eq!(check(&missing)["ready"], false);
    assert_eq!(check(&missing)["program"], "codex");
    assert!(!missing.status.success());
    let cli = bin.join("codex");
    fs::write(&cli, "#!/bin/sh\nexit 99\n").unwrap();
    fs::set_permissions(&cli, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(
        check(&doctor())["ready"],
        false,
        "non-executable is not ready"
    );
    fs::set_permissions(&cli, fs::Permissions::from_mode(0o755)).unwrap();
    let present = doctor();
    assert_eq!(
        check(&present)["ready"],
        true,
        "readiness must not run the CLI or require Claude"
    );
}
