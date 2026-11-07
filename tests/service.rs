//! `oppa service` against a stand-in launchd: the fake `launchctl` keeps its
//! loaded jobs in a JSON file and runs a one-shot job (KeepAlive false) the way
//! launchd would, from the plist's own ProgramArguments, environment and log
//! paths. The fake `codesign` records what it signed. Serves X-7 (setup without
//! editing files) and S-2 (a release replaces `oppa` and keeps the launcher).
#![cfg(target_os = "macos")]
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use tempfile::TempDir;

const LAUNCHCTL: &str = r#"#!/usr/bin/python3
import json, os, plistlib, subprocess, sys
state_file = os.path.join(os.environ["FAKE_LAUNCHD"], "jobs.json")
jobs = json.load(open(state_file)) if os.path.exists(state_file) else {}
log = open(os.path.join(os.environ["FAKE_LAUNCHD"], "calls"), "a")
log.write(" ".join(sys.argv[1:]) + "\n")
verb, target = sys.argv[1], sys.argv[2:]
def save():
    json.dump(jobs, open(state_file, "w"))
if verb == "bootstrap":
    plist = plistlib.load(open(target[1], "rb"))
    label = plist["Label"]
    if label in jobs:  # loaded, or still draining after a bootout
        sys.exit(5)
    job = {"runs": 1, "arguments": plist.get("ProgramArguments", []),
           "environment": plist.get("EnvironmentVariables", {})}
    def busy(name):
        # A job's process with a harness or connector in a process group of its
        # own, as oppa starts one; the child's pid goes to <name>.pid. With
        # `late`, the child starts only once the job is told to stop, as a
        # draining agent's next turn would.
        late = os.path.exists(os.path.join(os.environ["FAKE_LAUNCHD"], "late"))
        script = ("import os, signal, subprocess, sys, time\n"
                  "def child(*_):\n"
                  "    c = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)'], preexec_fn=os.setpgrp)\n"
                  "    open(os.path.join(os.environ['FAKE_LAUNCHD'], %r), 'w').write(str(c.pid))\n"
                  "    if _: time.sleep(0.6); sys.exit(0)\n"
                  "signal.signal(signal.SIGTERM, child) if %r else child()\n"
                  "time.sleep(120)\n") % (name + ".pid", late)
        process = subprocess.Popen([sys.executable, "-c", script], preexec_fn=os.setpgrp,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        job.update(state="running", pid=process.pid, real=True)
    launcher = (plist.get("ProgramArguments") or [""])[0]
    installed = os.path.join(os.path.dirname(launcher), "oppa")
    if plist.get("KeepAlive") and os.path.exists(installed) and b"LOAD_FAILS = True" in open(installed, "rb").read():
        print("Bootstrap failed: 5: Input/output error", file=sys.stderr)
        sys.exit(5)
    if plist.get("KeepAlive") and os.path.exists(installed) and b"MOVES_LIVE = True" in open(installed, "rb").read():
        # A release that moves the instance's database on as it starts.
        import sqlite3
        live = sqlite3.connect(os.path.join(plist["EnvironmentVariables"]["OPPA_HOME"], "state.db"))
        live.execute("PRAGMA user_version=%d" % (live.execute("PRAGMA user_version").fetchone()[0] + 1))
        live.close()
    if plist.get("KeepAlive") and os.path.exists(installed) and b"CRASHES = True" in open(installed, "rb").read():
        # An agent whose `oppa` exits at once: launchd starts it again and again.
        job.update(state="spawn scheduled", code=1, crashing=True)
    elif plist.get("KeepAlive"):
        if os.path.exists(os.path.join(os.environ["FAKE_LAUNCHD"], "slow-spawn")):
            job["spawn_prints"] = 2
        if os.path.exists(os.path.join(os.environ["FAKE_LAUNCHD"], "busy")):
            busy(label)
        else:
            # A real process this test owns, so nothing ever looks up a stranger's pid.
            process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"],
                                       preexec_fn=os.setpgrp, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL)
            job.update(state="running", pid=process.pid, real=True)
    elif os.path.exists(os.path.join(os.environ["FAKE_LAUNCHD"], "hang")):
        # A one-shot job that is still waiting, as on a macOS prompt nobody answers.
        busy("connector")
    else:
        with open(plist["StandardOutPath"], "w") as out, open(plist["StandardErrorPath"], "w") as err:
            code = subprocess.call(plist["ProgramArguments"], env=plist["EnvironmentVariables"],
                                   cwd=plist["WorkingDirectory"], stdout=out, stderr=err)
        log.write("ran " + " ".join(plist["ProgramArguments"]) + "\n")
        # launchd reports a one-shot job as running for a while after it starts.
        job.update(state="not running", code=code, running_prints=1)
    jobs[label] = job
elif verb == "bootout":
    label = target[0].split("/")[-1]
    job = jobs.get(label)
    if job is None:
        sys.exit(3)
    if job["state"] == "running":
        if job.get("real"):
            # launchd stops the job's own process group, and only that.
            import signal
            os.killpg(job["pid"], signal.SIGTERM)
        # bootout returns while the process drains; it stays loaded meanwhile.
        job.update(state="draining", drain_prints=3)
    else:
        del jobs[label]
elif verb == "print":
    if os.path.exists(os.path.join(os.environ["FAKE_LAUNCHD"], "broken")):
        print("Could not reach launchd", file=sys.stderr)
        sys.exit(112)
    label = target[0].split("/")[-1]
    job = jobs.get(label)
    if job is None:
        sys.exit(113)
    state = job["state"]
    if job.get("crashing"):
        job["runs"] += 1
    spawning = job.get("spawn_prints", 0) > 0
    if spawning:
        job["spawn_prints"] -= 1
    if state == "draining":
        job["drain_prints"] -= 1
        if job["drain_prints"] <= 0:
            del jobs[label]
        state = "running"
    elif job.get("running_prints", 0) > 0:
        job["running_prints"] -= 1
        state = "running"
    if spawning:
        state = "spawn scheduled"
    print("gui/501/x = {")
    print("\tstate = %s" % state)
    print("\truns = %d" % (0 if spawning else job["runs"]))
    if "pid" in job and not spawning:
        print("\tpid = %d" % job["pid"])
    code = job.get("code", "(never exited)") if state != "running" else "(never exited)"
    print("\tlast exit code = %s" % code)
    print("\targuments = {")
    for argument in job.get("arguments", []):
        print("\t\t%s" % argument)
    print("\t}")
    print("\tenvironment = {")
    for key, value in job.get("environment", {}).items():
        print("\t\t%s => %s" % (key, value))
    print("\t}")
    print("\tendpoints = {\n\t\tstate = active\n\t}")
save()
"#;

/// Signing marks the file's inode, which a rename keeps; `-dv` reports the
/// identifier only for a signed file.
const CODESIGN: &str = r#"#!/bin/sh
if [ "$1" = "-dv" ]; then
  grep -qx "$(stat -f %i "$2")" "$FAKE_LAUNCHD/signed" 2>/dev/null && echo "Identifier=ai.oppa.launch" >&2
  exit 0
fi
for last; do :; done
stat -f %i "$last" >> "$FAKE_LAUNCHD/signed"
"#;

/// A connector whose health says whether it was allowed to prompt, or, with
/// `denied`, that it has no access.
const CONNECTOR: &str = r#"#!/usr/bin/python3
import json, sys
request = json.load(sys.stdin)
if request["method"] == "describe":
    print(json.dumps({"protocol_version": 1, "operations": [{"name": "probe", "tier": "read",
        "description": "synthetic", "input_schema": {"type": "object"}}, {"name": "change",
        "tier": "reversible", "description": "synthetic", "input_schema": {"type": "object"}}]}))
elif sys.argv[1:] == ["denied"]:
    print(json.dumps({"ready": False, "problem": "no access", "fix": "allow it"}))
else:
    print(json.dumps({"ready": True, "prompted": request.get("may_prompt")}))
"#;

/// A release of `oppa` for upgrades. Its canary turn answers with the word it
/// is asked for unless SILENT, moves the copy's database a version on with
/// MIGRATES, and records what it was given in $FAKE_LAUNCHD/build.log. With
/// CRASHES the stand-in launchd runs the agents as if it exits at once.
const BUILD: &str = r#"#!/usr/bin/python3
import json, os, sqlite3, sys
SILENT = MIGRATES = CRASHES = LOAD_FAILS = MOVES_LIVE = False
args = sys.argv[1:]
if args[:1] != ["chat"]:
    sys.exit(2)
value = lambda flag: args[args.index(flag) + 1]
home, private = value("--home"), value("--private-dir")
connection = sqlite3.connect(os.path.join(home, "state.db"))
version = connection.execute("PRAGMA user_version").fetchone()[0]
if version == 0:
    connection.execute("PRAGMA user_version=3")  # as the store makes a new database
if MIGRATES:
    connection.execute("PRAGMA user_version=%d" % (version + 1))
config = open(os.path.join(private, "config.toml")).read()
with open(os.path.join(os.environ["FAKE_LAUNCHD"], "build.log"), "a") as log:
    objects = sorted(list(row) for row in connection.execute(
        "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"))
    log.write(json.dumps({"home": home, "private": private, "version": version, "config": config,
                          "objects": objects,
                          "config_names_copy": home in config, "environment": sorted(os.environ)}) + "\n")
if SILENT:
    print("no answer", file=sys.stderr)
    sys.exit(1)
print(value("--message").rsplit(" ", 1)[1])
"#;

struct Mac {
    _root: TempDir,
    home: PathBuf,
    runtime: PathBuf,
    private: PathBuf,
    launchd: PathBuf,
    fake_bin: PathBuf,
    /// TMPDIR for every `oppa` here: canary copies go there, and an upgrade
    /// sweeps it for copies a dead upgrade left. Suites running at once would
    /// otherwise sweep and plant each other's in the shared temp folder.
    temporary: PathBuf,
}

fn script(path: &Path, body: &str) {
    fs::write(path, body).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

impl Mac {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("oppa-service-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let home = root.path().join("home");
        let launchd = root.path().join("launchd");
        let fake_bin = root.path().join("fake-bin");
        let temporary = root.path().join("tmp");
        for dir in [&home, &launchd, &fake_bin, &temporary] {
            fs::create_dir_all(dir).unwrap();
        }
        script(&fake_bin.join("launchctl"), LAUNCHCTL);
        script(&fake_bin.join("codesign"), CODESIGN);
        let mac = Self {
            runtime: home.join(".oppa"),
            private: home.join(".oppa-private"),
            home,
            launchd,
            fake_bin,
            temporary,
            _root: root,
        };
        let init = mac.oppa(&["init", "--agent", "claude"]);
        assert!(init.status.success(), "{}", stderr(&init));
        mac
    }

    fn oppa_with_path(&self, extra: &str, args: &[&str]) -> Output {
        self.oppa_from(Path::new("."), extra, args)
    }

    fn oppa_from(&self, dir: &Path, extra: &str, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_oppa"))
            .args(args)
            .current_dir(dir)
            .env_clear()
            .env("HOME", &self.home)
            .env(
                "PATH",
                format!("{}:/usr/bin:/bin{extra}", self.fake_bin.display()),
            )
            .env("OPPA_HOME", &self.runtime)
            .env("OPPA_PRIVATE_DIR", &self.private)
            .env("FAKE_LAUNCHD", &self.launchd)
            .env("TMPDIR", &self.temporary)
            .output()
            .unwrap()
    }

    fn oppa(&self, args: &[&str]) -> Output {
        self.oppa_with_path("", args)
    }

    fn add_connector(&self, mode: &str) {
        let connector = self.home.join("probe.py");
        script(&connector, CONNECTOR);
        let config = self.private.join("config.toml");
        let mut text = fs::read_to_string(&config).unwrap();
        text.push_str(&format!(
            "\n[[connectors]]\nid = \"probe\"\nkind = \"script\"\ncommand = [\"{}\", \"{mode}\"]\n",
            connector.display()
        ));
        fs::write(&config, text).unwrap();
    }

    fn plist(&self, label: &str) -> PathBuf {
        self.home
            .join("Library/LaunchAgents")
            .join(format!("{label}.plist"))
    }

    fn signed(&self) -> String {
        fs::read_to_string(self.launchd.join("signed")).unwrap_or_default()
    }

    /// Load a plist the way a hand-run `launchctl bootstrap` would.
    fn load(&self, plist: &Path) {
        let loaded = Command::new(self.fake_bin.join("launchctl"))
            .args(["bootstrap", "gui/501"])
            .arg(plist)
            .env("FAKE_LAUNCHD", &self.launchd)
            .output()
            .unwrap();
        assert!(loaded.status.success(), "{}", stderr(&loaded));
    }

    fn calls(&self) -> String {
        fs::read_to_string(self.launchd.join("calls")).unwrap_or_default()
    }

    /// A release with the given switches turned on, at a path of its own.
    fn release(&self, name: &str, switches: &[&str]) -> PathBuf {
        let on: String = switches.iter().map(|s| format!("{s} = True\n")).collect();
        // Each release is its own bytes, as two builds are.
        let body = BUILD.replacen(
            "args = sys.argv",
            &format!("{on}# {name}\nargs = sys.argv"),
            1,
        );
        let path = self.home.join(name);
        script(&path, &body);
        path
    }

    fn builds(&self) -> Vec<serde_json::Value> {
        fs::read_to_string(self.launchd.join("build.log"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn json(output: &Output) -> serde_json::Value {
    assert!(output.status.success(), "{}", stderr(output));
    serde_json::from_slice(&output.stdout).unwrap()
}

fn plist_value(path: &Path, key: &str) -> String {
    let output = Command::new("/usr/bin/plutil")
        .args(["-extract", key, "raw", "-o", "-"])
        .arg(path)
        .output()
        .unwrap();
    assert!(output.status.success(), "{key}: {}", stderr(&output));
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn upgrade(mac: &Mac, release: &Path, extra: &[&str]) -> (bool, serde_json::Value) {
    let mut args = vec!["service", "upgrade", "--from", release.to_str().unwrap()];
    args.extend(extra);
    let output = mac.oppa(&args);
    let report = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|_| panic!("no report: {}", stderr(&output)));
    (output.status.success(), report)
}

/// The service's `oppa`, its previous build and both agents' processes.
fn installed(mac: &Mac) -> (Vec<u8>, Option<Vec<u8>>, Vec<String>) {
    let status = json(&mac.oppa(&["service", "status"]));
    let pids = status["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|agent| agent["pid"].as_str().unwrap_or_default().to_owned())
        .collect();
    let bin = mac.runtime.join("bin");
    (
        fs::read(bin.join("oppa")).unwrap(),
        fs::read(bin.join("oppa.previous")).ok(),
        pids,
    )
}

fn signatures(mac: &Mac) -> usize {
    mac.signed().lines().count()
}

fn inode(path: &Path) -> u64 {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path).unwrap().ino()
}

#[test]
fn install_copies_this_build_signs_the_launcher_once_and_writes_valid_agents() {
    let mac = Mac::new();
    // /Library stands for a directory where the owner's harness CLI lives.
    let report = json(&mac.oppa_with_path(":/Library", &["service", "install"]));
    assert_eq!(report["launcher"]["action"], "installed");
    let bin = mac.runtime.join("bin");
    let launcher = bin.join("oppa-launch");
    assert_eq!(
        fs::read(bin.join("oppa")).unwrap(),
        fs::read(env!("CARGO_BIN_EXE_oppa")).unwrap()
    );
    assert_eq!(signatures(&mac), 1);
    assert!(!launcher.with_extension("new").exists());
    for (label, subcommand) in [("ai.oppa.serve", "serve"), ("ai.oppa.work", "work")] {
        let plist = mac.plist(label);
        let lint = Command::new("/usr/bin/plutil")
            .arg("-lint")
            .arg(&plist)
            .output()
            .unwrap();
        assert!(
            lint.status.success(),
            "{}",
            String::from_utf8_lossy(&lint.stdout)
        );
        assert_eq!(
            plist_value(&plist, "ProgramArguments.0"),
            launcher.to_string_lossy()
        );
        assert_eq!(plist_value(&plist, "ProgramArguments.1"), subcommand);
        assert_eq!(
            plist_value(&plist, "EnvironmentVariables.OPPA_HOME"),
            mac.runtime.to_string_lossy()
        );
        assert_eq!(plist_value(&plist, "ExitTimeOut"), "30");
        assert_eq!(
            plist_value(&plist, "EnvironmentVariables.OPPA_LAUNCH_GRACE_SECONDS"),
            "25"
        );
        // The system interpreter the adapters are accepted with comes first;
        // the installer's directories follow, but no temporary one.
        let path = plist_value(&plist, "EnvironmentVariables.PATH");
        assert!(path.starts_with(&format!("{}:/usr/bin:", bin.display())));
        assert!(path.ends_with(":/Library"), "{path}");
        assert!(!path.contains(&*mac.fake_bin.to_string_lossy()), "{path}");
    }
    assert!(!mac.calls().contains("bootstrap"), "install starts nothing");

    // A release from a shell with a shorter PATH: the new `oppa` replaces the
    // old, and the signed launcher (with its grants) and the PATH stay.
    let granted = inode(&launcher);
    let report = json(&mac.oppa(&["service", "install"]));
    assert_eq!(report["launcher"]["action"], "kept");
    assert_eq!(inode(&launcher), granted);
    assert_eq!(signatures(&mac), 1);
    let path = plist_value(&mac.plist("ai.oppa.serve"), "EnvironmentVariables.PATH");
    assert!(path.ends_with(":/Library"), "{path}");

    // Only on request is the launcher replaced and signed again.
    let report = json(&mac.oppa(&["service", "install", "--replace-launcher"]));
    assert_eq!(report["launcher"]["action"], "installed");
    assert_eq!(signatures(&mac), 2);

    // A launcher that is not signed as OPPA's is repaired, not kept.
    fs::remove_file(&launcher).unwrap();
    fs::copy(env!("CARGO_BIN_EXE_oppa-launch"), &launcher).unwrap();
    let report = json(&mac.oppa(&["service", "install"]));
    assert_eq!(report["launcher"]["action"], "installed");
    assert_eq!(signatures(&mac), 3);
}

#[test]
fn another_instances_agents_are_never_replaced_stopped_or_removed() {
    let mac = Mac::new();
    let plist = mac.plist("ai.oppa.serve");
    fs::create_dir_all(plist.parent().unwrap()).unwrap();
    let another = mac.home.join("another-instance");
    fs::create_dir_all(&another).unwrap();
    let other = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<plist version=\"1.0\"><dict>\
         <key>EnvironmentVariables</key><dict><key>OPPA_HOME</key>\
         <string>{}</string></dict></dict></plist>\n",
        another.display()
    );
    fs::write(&plist, &other).unwrap();
    for command in ["install", "start", "stop", "uninstall"] {
        let refused = mac.oppa(&["service", command]);
        assert!(!refused.status.success(), "{command}");
        assert!(
            stderr(&refused).contains("belongs to another instance"),
            "{command}"
        );
    }
    assert_eq!(fs::read_to_string(&plist).unwrap(), other);
    assert!(!mac.runtime.join("bin").exists());
    assert!(!mac.calls().contains("boot"));
    // An instance named on the command line outranks the environment, as it
    // does for oppa itself.
    let by_argument = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<plist version=\"1.0\"><dict>\
         <key>ProgramArguments</key><array><string>oppa</string><string>--home</string>\
         <string>{}</string><string>serve</string></array>\
         <key>EnvironmentVariables</key><dict><key>OPPA_HOME</key>\
         <string>{}</string></dict></dict></plist>\n",
        another.display(),
        mac.runtime.display()
    );
    fs::write(&plist, &by_argument).unwrap();
    let refused = mac.oppa(&["service", "stop"]);
    assert!(stderr(&refused).contains("belongs to another instance"));
    // Its status names that instance and never counts it as this one's.
    let status = json(&mac.oppa(&["service", "status"]));
    assert_eq!(status["agents"][0]["this_instance"], false);
    assert_eq!(
        status["agents"][0]["instance"],
        another.canonicalize().unwrap().to_string_lossy().as_ref()
    );
    assert_eq!(status["running"], false);
    fs::write(&plist, &other).unwrap();

    // Agents of an instance that no longer exists belong to nobody.
    fs::write(
        &plist,
        other.replace("another-instance", "deleted-instance"),
    )
    .unwrap();
    json(&mac.oppa(&["service", "uninstall"]));
    assert!(!plist.exists());

    // Agents written without OPPA_HOME run the default instance, ~/.oppa,
    // which is this one.
    fs::write(
        &plist,
        "<?xml version=\"1.0\"?><plist version=\"1.0\"><dict/></plist>",
    )
    .unwrap();
    json(&mac.oppa(&["service", "install"]));

    // The owner's own agents, reformatted by plutil, are still theirs.
    let reformat = Command::new("/usr/bin/plutil")
        .args(["-replace", "ThrottleInterval", "-integer", "10"])
        .arg(&plist)
        .output()
        .unwrap();
    assert!(reformat.status.success());
    assert!(fs::read_to_string(&plist)
        .unwrap()
        .contains("\n\t\t<string>"));
    json(&mac.oppa(&["service", "install"]));
}

#[test]
fn a_loaded_agent_belongs_to_the_instance_it_runs_whatever_its_plist_says_now() {
    let mac = Mac::new();
    let plist = mac.plist("ai.oppa.serve");
    fs::create_dir_all(plist.parent().unwrap()).unwrap();
    let another = mac.home.join("another-instance");
    fs::create_dir_all(&another).unwrap();
    let naming = |home: &Path| {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<plist version=\"1.0\"><dict>\
             <key>Label</key><string>ai.oppa.serve</string><key>KeepAlive</key><true/>\
             <key>ProgramArguments</key><array><string>oppa</string><string>serve</string></array>\
             <key>EnvironmentVariables</key><dict><key>OPPA_HOME</key><string>{}</string></dict>\
             </dict></plist>\n",
            home.display()
        )
    };
    // Another instance's agent is running; its plist was later edited to name
    // this instance, and then removed. Neither makes it this instance's.
    fs::write(&plist, naming(&another)).unwrap();
    mac.load(&plist);
    fs::write(&plist, naming(&mac.runtime)).unwrap();
    for command in ["stop", "uninstall"] {
        assert!(stderr(&mac.oppa(&["service", command])).contains("belongs to another instance"));
    }
    fs::remove_file(&plist).unwrap();
    assert!(stderr(&mac.oppa(&["service", "stop"])).contains("belongs to another instance"));
    let status = json(&mac.oppa(&["service", "status"]));
    assert_eq!(status["agents"][0]["this_instance"], false);
    // This instance's running agent stays its own without a plist.
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    let plist = mac.plist("ai.oppa.serve");
    let ours = fs::read_to_string(&plist).unwrap();
    fs::remove_file(&plist).unwrap();
    let status = json(&mac.oppa(&["service", "status"]));
    assert_eq!(status["agents"][0]["this_instance"], true);
    // But a plist now naming another instance is what the next start would
    // load, so start and uninstall refuse to touch it.
    let another = mac.home.join("another-instance");
    fs::create_dir_all(&another).unwrap();
    fs::write(&plist, naming(&another)).unwrap();
    for command in ["start", "uninstall"] {
        assert!(stderr(&mac.oppa(&["service", command])).contains("belongs to another instance"));
    }
    fs::write(&plist, ours).unwrap();
    json(&mac.oppa(&["service", "stop"]));
    // A relative path in a loaded agent is relative to the job, so it is never
    // taken for this instance, even from where it would name this instance.
    let mac = Mac::new();
    let plist = mac.plist("ai.oppa.serve");
    fs::create_dir_all(plist.parent().unwrap()).unwrap();
    fs::write(&plist, naming(Path::new(mac.runtime.file_name().unwrap()))).unwrap();
    mac.load(&plist);
    let beside = mac.oppa_from(mac.runtime.parent().unwrap(), "", &["service", "stop"]);
    assert!(stderr(&beside).contains("belongs to another instance"));
    // An agent that sets its own HOME, and no instance, serves the default
    // instance of that HOME, not the caller's.
    let mac = Mac::new();
    let plist = mac.plist("ai.oppa.serve");
    fs::create_dir_all(plist.parent().unwrap()).unwrap();
    let elsewhere = mac.home.parent().unwrap().join("elsewhere");
    fs::create_dir_all(elsewhere.join(".oppa")).unwrap();
    fs::write(
        &plist,
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<plist version=\"1.0\"><dict>\
             <key>Label</key><string>ai.oppa.serve</string><key>KeepAlive</key><true/>\
             <key>ProgramArguments</key><array><string>oppa</string><string>serve</string></array>\
             <key>EnvironmentVariables</key><dict><key>HOME</key><string>{}</string></dict>\
             </dict></plist>\n",
            elsewhere.display()
        ),
    )
    .unwrap();
    mac.load(&plist);
    assert!(stderr(&mac.oppa(&["service", "stop"])).contains("belongs to another instance"));
}

#[test]
fn start_waits_for_a_draining_agent_and_stop_and_uninstall_unload_them() {
    let mac = Mac::new();
    assert!(!mac.oppa(&["service", "start"]).status.success());
    json(&mac.oppa(&["service", "install"]));
    let status = json(&mac.oppa(&["service", "start"]));
    assert_eq!(status["running"], true);
    assert_eq!(status["oppa"]["is_this_build"], true);
    assert_eq!(status["launcher"]["signed_as"], "ai.oppa.launch");
    // Restarting: bootout returns while each agent drains, and launchd refuses
    // a bootstrap until it is gone, so start must wait rather than fail.
    let status = json(&mac.oppa(&["service", "start"]));
    assert_eq!(status["running"], true);
    assert!(status["agents"][1]["pid"]
        .as_str()
        .is_some_and(|pid| pid.parse::<i32>().is_ok()));
    assert!(!mac.calls().contains("bootstrap failed"));

    let status = json(&mac.oppa(&["service", "stop"]));
    assert_eq!(status["running"], false);
    assert_eq!(status["agents"][0]["loaded"], false);
    assert!(mac.plist("ai.oppa.serve").exists());

    json(&mac.oppa(&["service", "start"]));
    let status = json(&mac.oppa(&["service", "uninstall"]));
    assert_eq!(status["agents"][0]["loaded"], false);
    assert_eq!(status["agents"][1]["loaded"], false);
    assert!(!mac.plist("ai.oppa.serve").exists());
    assert!(mac.runtime.join("bin/oppa-launch").is_file());
}

#[test]
fn check_runs_the_connector_test_through_the_launcher_and_may_prompt() {
    let mac = Mac::new();
    mac.add_connector("ready");
    assert!(!mac.oppa(&["service", "check"]).status.success());
    json(&mac.oppa(&["service", "install"]));
    let report = json(&mac.oppa(&["service", "check"]));
    assert_eq!(report["ready"], true, "{report}");
    assert_eq!(report["connectors"][0]["id"], "probe");
    assert_eq!(report["connectors"][0]["prompted"], true);
    assert_eq!(
        report["connectors"][0]["read_operations"],
        serde_json::json!(["probe"])
    );
    assert_eq!(
        report["connectors"][0]["write_operations"],
        serde_json::json!(["change"])
    );
    let launcher = mac.runtime.join("bin/oppa-launch");
    let calls = mac.calls();
    assert!(calls.contains(&format!(
        "ran {} connectors test --prompt",
        launcher.display()
    )));
    assert!(calls.contains("bootout gui/") && calls.contains("ai.oppa.check."));
    // Each script connector may take its prompt window, then describe itself.
    assert_eq!(report["deadline_seconds"], 60 + 400 + 30);
    // Another instance's check runs under its own label.
    let label = |calls: &str| {
        calls
            .split_whitespace()
            .find(|word| word.contains("ai.oppa.check."))
            .unwrap()
            .rsplit('/')
            .next()
            .unwrap()
            .to_owned()
    };
    let other = Mac::new();
    other.add_connector("ready");
    json(&other.oppa(&["service", "install"]));
    json(&other.oppa(&["service", "check"]));
    assert_ne!(label(&calls), label(&other.calls()));
    assert!(!mac.runtime.join("check.plist").exists());
    // One check per instance at a time: a held lock refuses a second one.
    let lock = fs::OpenOptions::new()
        .write(true)
        .open(mac.runtime.join("locks/check.lock"))
        .unwrap();
    lock.try_lock().unwrap();
    let refused = mac.oppa(&["service", "check"]);
    assert!(stderr(&refused).contains("another service check is running"));
}

#[test]
fn a_check_the_service_cannot_pass_fails_with_its_report() {
    let mac = Mac::new();
    mac.add_connector("denied");
    json(&mac.oppa(&["service", "install"]));
    let output = mac.oppa(&["service", "check"]);
    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["ready"], false);
    assert_eq!(report["connectors"][0]["problem"], "no access");
}

#[test]
fn a_check_past_its_deadline_is_unloaded_and_says_so() {
    let mac = Mac::new();
    mac.add_connector("ready");
    json(&mac.oppa(&["service", "install"]));
    fs::write(mac.launchd.join("hang"), "").unwrap();
    let output = mac.oppa(&["service", "check", "--timeout-seconds", "1"]);
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("did not finish in time"),
        "{}",
        stderr(&output)
    );
    let calls = mac.calls();
    let label = calls
        .split_whitespace()
        .find(|word| word.contains("ai.oppa.check."))
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap();
    let jobs: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(mac.launchd.join("jobs.json")).unwrap()).unwrap();
    assert!(
        jobs.get(label).is_none(),
        "the check job is still loaded: {jobs}"
    );
    // Its connector ran in a process group of its own, and is stopped too.
    assert!(
        stopped(&mac.launchd.join("connector.pid")),
        "the check's connector still runs"
    );
}

/// Whether the process whose pid a fake job wrote has stopped.
fn stopped(pid_file: &Path) -> bool {
    let pid: i32 = (0..50)
        .find_map(|_| {
            std::thread::sleep(std::time::Duration::from_millis(100));
            fs::read_to_string(pid_file).ok()?.trim().parse().ok()
        })
        .expect("the fake job wrote no pid");
    (0..50).any(|_| {
        std::thread::sleep(std::time::Duration::from_millis(100));
        (unsafe { libc::kill(pid, 0) }) != 0
    })
}

#[test]
fn stop_also_stops_what_the_agents_started() {
    // S-2: a harness or connector runs in a process group of its own, which
    // launchd does not stop with its agent; a stopped service acts no more,
    // whether the child ran before the stop or started while the agent drained.
    for late in [false, true] {
        let mac = Mac::new();
        json(&mac.oppa(&["service", "install"]));
        fs::write(mac.launchd.join("busy"), "").unwrap();
        if late {
            fs::write(mac.launchd.join("late"), "").unwrap();
        }
        json(&mac.oppa(&["service", "start"]));
        json(&mac.oppa(&["service", "stop"]));
        for label in ["ai.oppa.serve", "ai.oppa.work"] {
            assert!(
                stopped(&mac.launchd.join(format!("{label}.pid"))),
                "{label}'s child still runs (late: {late})"
            );
        }
    }
}

#[test]
fn a_launchctl_that_cannot_answer_is_an_error_not_a_stopped_service() {
    // S-2: a failure to read launchd never passes for "not loaded".
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    fs::write(mac.launchd.join("broken"), "").unwrap();
    for command in ["stop", "status"] {
        let output = mac.oppa(&["service", command]);
        assert!(!output.status.success(), "{command} succeeded");
        assert!(stderr(&output).contains("launchctl could not read"));
    }
    fs::remove_file(mac.launchd.join("broken")).unwrap();
    json(&mac.oppa(&["service", "stop"]));
}

// S-2: a release whose canary turn answers, on a copy of the instance, takes
// the service's place and keeps running; the previous build stays beside it.
#[test]
fn an_upgrade_installs_a_release_whose_canary_answers_and_keeps_the_previous_build() {
    let mac = Mac::new();
    // A path that only starts like the instance's runtime is not the instance's.
    let elsewhere = format!("source = \"{}-elsewhere/checkout\"", mac.runtime.display());
    let config = mac.private.join("config.toml");
    fs::write(
        &config,
        format!("{elsewhere}\n{}", fs::read_to_string(&config).unwrap()),
    )
    .unwrap();
    mac.add_connector("ok");
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    let (before, _, _) = installed(&mac);
    let release = mac.release("release-good", &[]);
    let (ok, report) = upgrade(&mac, &release, &[]);
    assert!(ok, "{report}");
    assert_eq!(report["upgraded"], true);
    assert_eq!(report["canary"]["answered"], true);
    assert_eq!(report["health"]["settled"], true);
    let (now, previous, _) = installed(&mac);
    assert_eq!(now, fs::read(&release).unwrap());
    assert_eq!(previous.unwrap(), before);
    assert_eq!(json(&mac.oppa(&["service", "status"]))["running"], true);
    // The canary ran on a copy, gone now, whose configuration names the copy,
    // without the instance's environment.
    let build = &mac.builds()[0];
    for (copy, live) in [("home", &mac.runtime), ("private", &mac.private)] {
        let copy = Path::new(build[copy].as_str().unwrap());
        assert!(!copy.starts_with(live) && !copy.exists(), "{build}");
    }
    assert_eq!(build["config_names_copy"], true, "{build}");
    let copied = build["config"].as_str().unwrap();
    assert!(copied.contains(&elsewhere), "{build}");
    // Read access and no connector: the canary can neither act nor reach an account.
    assert!(
        copied.contains("access = \"read\"") && !copied.contains("[[connectors]]"),
        "{build}"
    );
    // The copy has the live database's tables, indexes and triggers, and nothing else.
    let live = Command::new("/usr/bin/sqlite3")
        .arg(mac.runtime.join("state.db"))
        .arg("SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name")
        .output()
        .unwrap();
    let mut live: Vec<Vec<String>> = String::from_utf8_lossy(&live.stdout)
        .lines()
        .map(|line| line.split('|').map(String::from).collect())
        .collect();
    live.sort();
    let objects: Vec<Vec<String>> = serde_json::from_value(build["objects"].clone()).unwrap();
    assert!(live.iter().any(|object| object[0] == "trigger"), "{live:?}");
    assert_eq!(objects, live);
    assert_eq!(build["version"], 3, "{build}");
    let environment = build["environment"].as_array().unwrap();
    assert!(!environment
        .iter()
        .any(|name| name == "OPPA_HOME" || name == "OPPA_PRIVATE_DIR"));
    // A copy an interrupted upgrade left behind is removed by the next one.
    assert!(
        Path::new(build["home"].as_str().unwrap()).starts_with(&mac.temporary),
        "{build}"
    );
    let stale = mac.temporary.join("oppa-canary-99999999-dead");
    fs::create_dir_all(stale.join("private")).unwrap();
    let next = mac.release("release-next", &[]);
    let (ok, report) = upgrade(&mac, &next, &[]);
    assert!(ok, "{report}");
    assert!(!stale.exists());
    // The same build again changes nothing.
    let again = mac.oppa(&["service", "upgrade", "--from", next.to_str().unwrap()]);
    assert!(!again.status.success());
    assert!(
        stderr(&again).contains("already the service's build"),
        "{}",
        stderr(&again)
    );
}

// S-2: a release that cannot answer its canary turn never reaches the service:
// its `oppa` and both agents' processes are as they were.
#[test]
fn a_release_whose_canary_does_not_answer_is_not_installed() {
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    let before = installed(&mac);
    let silent = mac.release("release-silent", &["SILENT"]);
    let output = mac.oppa(&["service", "upgrade", "--from", silent.to_str().unwrap()]);
    assert!(
        stderr(&output).contains("the release did not answer its canary turn"),
        "{}",
        stderr(&output)
    );
    let (ok, report) = upgrade(&mac, &silent, &[]);
    assert!(!ok, "{report}");
    assert_eq!(report["service"], "unchanged");
    assert_eq!(report["canary"]["answered"], false);
    assert_eq!(report["canary"]["error"], "no answer");
    assert_eq!(installed(&mac), before);
}

// S-2: a release that does not keep running as the service is replaced by the
// previous build, which runs again; the failed build is kept for a look.
#[test]
fn a_release_that_does_not_keep_running_rolls_back_to_the_previous_build() {
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    let (before, _, _) = installed(&mac);
    let release = mac.release("release-crash", &["CRASHES"]);
    let started = std::time::Instant::now();
    let (ok, report) = upgrade(&mac, &release, &[]);
    // A restart counts at once; the agents are not watched for the whole 30 seconds.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "{:?}",
        started.elapsed()
    );
    assert!(!ok, "{report}");
    assert_eq!(report["rolled_back"], true, "{report}");
    assert_eq!(report["health"]["restarted"], true, "{report}");
    assert_eq!(report["after_rollback"]["settled"], true, "{report}");
    assert_eq!(installed(&mac).0, before);
    assert_eq!(
        fs::read(mac.runtime.join("bin/oppa.failed")).unwrap(),
        fs::read(&release).unwrap()
    );
    assert_eq!(json(&mac.oppa(&["service", "status"]))["running"], true);
}

// S-2: a release that moves the database on cannot be undone by the previous
// build, so it needs --migrate, and is then not rolled back.
#[test]
fn a_release_that_moves_the_database_on_needs_migrate_and_is_not_rolled_back() {
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    let before = installed(&mac);
    let release = mac.release("release-migrates", &["MIGRATES"]);
    let (ok, report) = upgrade(&mac, &release, &[]);
    assert!(!ok, "{report}");
    assert_eq!(report["service"], "unchanged");
    assert_eq!(report["canary"]["schema_before"], 3);
    assert_eq!(report["canary"]["schema_after"], 4);
    assert!(
        report["problem"].as_str().unwrap().contains("--migrate"),
        "{report}"
    );
    assert_eq!(installed(&mac), before);
    let (ok, report) = upgrade(&mac, &release, &["--migrate"]);
    assert!(ok, "{report}");
    assert_eq!(installed(&mac).0, fs::read(&release).unwrap());

    // One that fails before it moves the live database goes back like any other.
    let (now, _, _) = installed(&mac);
    let early = mac.release("release-fails-early", &["MIGRATES", "CRASHES"]);
    let (ok, report) = upgrade(&mac, &early, &["--migrate"]);
    assert!(!ok, "{report}");
    assert_eq!(report["rolled_back"], true, "{report}");
    assert_eq!(installed(&mac).0, now);
    // One that moved it stays: the previous build would refuse the database.
    let crashing = mac.release(
        "release-migrates-crash",
        &["MIGRATES", "CRASHES", "MOVES_LIVE"],
    );
    let (ok, report) = upgrade(&mac, &crashing, &["--migrate"]);
    assert!(!ok, "{report}");
    assert_eq!(report["upgraded"], true, "{report}");
    assert!(report.get("rolled_back").is_none(), "{report}");
    assert_eq!(installed(&mac).0, fs::read(&crashing).unwrap());
}

// S-2: a run inside the service cannot upgrade it, since the upgrade stops
// the service and the run with it.
#[test]
fn an_upgrade_from_inside_a_run_is_refused() {
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    let release = mac.release("release-good", &[]);
    let output = Command::new(env!("CARGO_BIN_EXE_oppa"))
        .args(["service", "upgrade", "--from", release.to_str().unwrap()])
        .env_clear()
        .env("HOME", &mac.home)
        .env("PATH", format!("{}:/usr/bin:/bin", mac.fake_bin.display()))
        .env("OPPA_HOME", &mac.runtime)
        .env("OPPA_PRIVATE_DIR", &mac.private)
        .env("FAKE_LAUNCHD", &mac.launchd)
        .env("OPPA_RUN", "1")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr(&output).contains("run it from a terminal"),
        "{}",
        stderr(&output)
    );
    assert!(mac.builds().is_empty());
}

// S-2: a real build's canary turn runs `oppa chat` on a prepared copy of the
// instance. The harness lives in ~/.oppa-private beside ~/.oppa, so it is only
// found if each folder's path was replaced whole, and it must run from the
// copy: on a fresh conversation with no native session, at read access, and
// without the owner's console history. The instance has a channel, a person
// whose handle names it, a group shared with another assistant on it and a
// heartbeat in it, as the owner's may: a copy
// without its channels had people's handles name no channel, and the
// 2026-10-05 release failed its canary. A group may hold only new_people.
// The channel's command leaves a mark if anything starts it; nothing may.
#[test]
fn a_real_build_answers_its_canary_from_a_prepared_copy_of_the_instance() {
    let mac = Mac::new();
    let harness = mac.private.join("harness.py");
    script(
        &harness,
        "#!/usr/bin/python3\nimport json, os, re, sys\nrequest = json.load(sys.stdin)\n\
         with open(os.path.join(os.environ['FAKE_LAUNCHD'], 'harness.log'), 'a') as log:\n    \
         log.write(json.dumps({'path': os.path.abspath(__file__), 'conversation': request['conversation'], \
         'session': request.get('session'), 'access': request.get('access'), \
         'planted': 'lilac-ferry' in request['prompt'] + (request.get('instructions') or '')}) + '\\n')\n\
         found = re.search(r'canary-[0-9a-f]+', request['prompt'])\n\
         print(json.dumps({'type': 'session', 'handle': 'synthetic-session'}))\n\
         print(json.dumps({'type': 'answer', 'text': found.group(0) if found else 'noted'}))\n\
         print(json.dumps({'type': 'completed'}))\n",
    );
    let config = mac.private.join("config.toml");
    let text = fs::read_to_string(&config).unwrap();
    let command = text
        .lines()
        .skip_while(|line| !line.starts_with("command = ["))
        .take_while(|line| !line.starts_with(']'))
        .chain(std::iter::once("]"))
        .collect::<Vec<_>>()
        .join("\n");
    let text = text.replacen(
        &command,
        &format!(
            "command = [\"/usr/bin/python3\", \"{}\"]",
            harness.display()
        ),
        1,
    );
    let text: Vec<&str> = text
        .lines()
        .filter(|line| !line.starts_with("access "))
        .collect();
    let channel = mac.home.join("channel.sh");
    script(&channel, "#!/bin/sh\ntouch \"$0.started\"\n");
    fs::write(
        &config,
        format!(
            "access = \"computer\"\n{}\n\n[[channels]]\nid = \"imessage\"\naccount = \"assistant\"\n\
             command = [\"{}\"]\n\n[[connectors]]\nid = \"mac\"\nkind = \"script\"\n\
             command = [\"/never/launch/a/connector\"]\n\n[[people]]\nid = \"owner\"\nrole = \"owner\"\n\
             handles = [\"imessage:+15550100\"]\n\n[[people]]\nid = \"sam\"\nrole = \"adult\"\n\
             handles = [\"imessage:+15550101\"]\n\n[[groups]]\n\
             conversation = \"imessage/assistant/family\"\ntakes_unaddressed = true\n\n\
             [[groups]]\nconversation = \"imessage/assistant/school\"\nnew_people = false\n\n\
             [heartbeat]\nevery_minutes = 60\n\
             waking_from = \"08:00\"\nwaking_until = \"21:00\"\n\
             conversation = \"imessage/assistant/any;-;+15550100\"\n",
            text.join("\n"),
            channel.display()
        ),
    )
    .unwrap();
    // The owner's console conversation, with a native session and history.
    let planted = mac.oppa(&["chat", "--message", "Remember the code lilac-ferry."]);
    assert!(planted.status.success(), "{}", stderr(&planted));
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    let release = mac.home.join("oppa-release");
    fs::copy(env!("CARGO_BIN_EXE_oppa"), &release).unwrap();
    let signed = Command::new("/usr/bin/codesign")
        .args([
            "--force",
            "--sign",
            "-",
            "--identifier",
            "ai.oppa.test-release",
        ])
        .arg(&release)
        .output()
        .unwrap();
    assert!(signed.status.success(), "{}", stderr(&signed));
    let (ok, report) = upgrade(&mac, &release, &[]);
    assert!(ok, "{report}");
    assert_eq!(report["canary"]["answered"], true, "{report}");
    assert_eq!(installed(&mac).0, fs::read(&release).unwrap());
    let runs: Vec<serde_json::Value> = fs::read_to_string(mac.launchd.join("harness.log"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(runs.len(), 2, "{runs:?}");
    assert_eq!(runs[0]["access"], "computer", "{runs:?}");
    assert!(Path::new(runs[0]["path"].as_str().unwrap()).starts_with(&mac.private));
    let canary = &runs[1];
    let ran = Path::new(canary["path"].as_str().unwrap());
    assert!(
        ran.ends_with("private/harness.py") && !ran.starts_with(&mac.private),
        "{canary}"
    );
    assert!(
        canary["conversation"]
            .as_str()
            .unwrap()
            .starts_with("canary-"),
        "{canary}"
    );
    assert_eq!(canary["session"], serde_json::Value::Null, "{canary}");
    assert_eq!(canary["access"], serde_json::Value::Null, "{canary}");
    assert_eq!(canary["planted"], false, "{canary}");
    assert!(!mac.home.join("channel.sh.started").exists());
}

// S-2: a release launchd will not even load is a failure like one that exits:
// the previous build goes back and runs.
#[test]
fn a_release_launchd_cannot_load_rolls_back_too() {
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    let (before, _, _) = installed(&mac);
    let (ok, report) = upgrade(
        &mac,
        &mac.release("release-unloadable", &["LOAD_FAILS"]),
        &[],
    );
    assert!(!ok, "{report}");
    assert_eq!(report["rolled_back"], true, "{report}");
    assert!(
        report["health"]["error"]
            .as_str()
            .unwrap()
            .contains("could not load"),
        "{report}"
    );
    assert_eq!(report["after_rollback"]["settled"], true, "{report}");
    assert_eq!(installed(&mac).0, before);
}

// S-2: an instance with no database yet has nothing a release could move on.
#[test]
fn an_instance_without_a_database_is_not_taken_for_a_migration() {
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    for name in ["state.db", "state.db-wal", "state.db-shm"] {
        let _ = fs::remove_file(mac.runtime.join(name));
    }
    let (ok, report) = upgrade(&mac, &mac.release("release-fresh", &[]), &[]);
    assert!(ok, "{report}");
    assert_eq!(report["canary"]["migrates"], false, "{report}");
}

// A trial service's agents carry its own label base and never the owner's;
// a base that is not a plain label is refused before anything is written.
#[test]
fn a_trial_label_keeps_the_agents_apart_from_the_owners() {
    let mac = Mac::new();
    let with_label = |label: &str, args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_oppa"))
            .args(args)
            .env_clear()
            .env("HOME", &mac.home)
            .env("PATH", format!("{}:/usr/bin:/bin", mac.fake_bin.display()))
            .env("OPPA_HOME", &mac.runtime)
            .env("OPPA_PRIVATE_DIR", &mac.private)
            .env("FAKE_LAUNCHD", &mac.launchd)
            .env("OPPA_SERVICE_LABEL", label)
            .output()
            .unwrap()
    };
    json(&with_label("ai.oppa.trial", &["service", "install"]));
    let status = json(&with_label("ai.oppa.trial", &["service", "start"]));
    assert_eq!(status["agents"][0]["label"], "ai.oppa.trial.serve");
    assert_eq!(status["running"], true);
    assert!(mac.plist("ai.oppa.trial.work").exists());
    assert!(!mac.plist("ai.oppa.serve").exists() && !mac.plist("ai.oppa.work").exists());
    // One pair of agents per instance, in either order: the owner's are refused
    // while the trial's serve it, and the trial's can still be stopped and removed.
    let owners = mac.oppa(&["service", "install"]);
    assert!(!owners.status.success());
    assert!(
        stderr(&owners).contains("ai.oppa.trial.serve already serves this instance"),
        "{}",
        stderr(&owners)
    );
    assert!(!mac.plist("ai.oppa.serve").exists());
    json(&with_label("ai.oppa.trial", &["service", "stop"]));
    json(&with_label("ai.oppa.trial", &["service", "uninstall"]));
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    let second = with_label("ai.oppa.trial", &["service", "install"]);
    assert!(!second.status.success());
    assert!(
        stderr(&second).contains("ai.oppa.serve already serves this instance"),
        "{}",
        stderr(&second)
    );
    assert!(!mac.plist("ai.oppa.trial.serve").exists());
    for bad in ["ai..oppa", ".ai.oppa", "ai oppa", "ai.oppa/serve"] {
        let refused = with_label(bad, &["service", "status"]);
        assert!(!refused.status.success(), "{bad}");
        assert!(
            stderr(&refused).contains("is not a label base"),
            "{bad}: {}",
            stderr(&refused)
        );
    }
}

// S-2: launchd can report a just-loaded job as not yet spawned, with no runs;
// that is not a restart, and the release stays.
#[test]
fn a_job_not_yet_spawned_is_not_taken_for_a_restart() {
    let mac = Mac::new();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    fs::write(mac.launchd.join("slow-spawn"), "").unwrap();
    let release = mac.release("release-good", &[]);
    let (ok, report) = upgrade(&mac, &release, &[]);
    assert!(ok, "{report}");
    assert_eq!(report["health"]["settled"], true, "{report}");
    assert_eq!(installed(&mac).0, fs::read(&release).unwrap());
}

// Only OPPA's own agents count as a pair: another program's agent whose label
// ends in .work, with no instance named, does not block the owner's install.
// A pair installed before the check (written here by hand) blocks `start`.
#[test]
fn only_oppa_agents_count_and_start_refuses_a_second_pair_too() {
    let mac = Mac::new();
    let agents = mac.home.join("Library/LaunchAgents");
    fs::create_dir_all(&agents).unwrap();
    fs::write(
        agents.join("com.example.work.plist"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><plist version=\"1.0\"><dict>\
         <key>Label</key><string>com.example.work</string>\
         <key>ProgramArguments</key><array><string>/usr/bin/true</string></array></dict></plist>",
    )
    .unwrap();
    json(&mac.oppa(&["service", "install"]));
    json(&mac.oppa(&["service", "start"]));
    json(&mac.oppa(&["service", "stop"]));
    // A trial pair's plists beside the owner's, as an older build could leave them.
    for agent in ["serve", "work"] {
        let trial = agents.join(format!("ai.oppa.trial.{agent}.plist"));
        fs::copy(mac.plist(&format!("ai.oppa.{agent}")), &trial).unwrap();
        let relabeled = Command::new("/usr/bin/plutil")
            .args([
                "-replace",
                "Label",
                "-string",
                &format!("ai.oppa.trial.{agent}"),
            ])
            .arg(&trial)
            .output()
            .unwrap();
        assert!(relabeled.status.success(), "{}", stderr(&relabeled));
    }
    let start = mac.oppa(&["service", "start"]);
    assert!(!start.status.success());
    assert!(
        stderr(&start).contains("already serves this instance"),
        "{}",
        stderr(&start)
    );
    // An upgrade refuses the same way, before its canary.
    let release = mac.release("release-blocked", &[]);
    let blocked = mac.oppa(&["service", "upgrade", "--from", release.to_str().unwrap()]);
    assert!(!blocked.status.success());
    assert!(
        stderr(&blocked).contains("already serves this instance"),
        "{}",
        stderr(&blocked)
    );
    assert!(mac.builds().is_empty());
}
