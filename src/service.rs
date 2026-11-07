//! The instance as a macOS login service: two launch agents that start
//! `oppa-launch`, which holds the owner's privacy grants and runs the `oppa`
//! beside it (src/bin/oppa-launch). Installing copies this build into
//! `<runtime>/bin`; the launcher is copied and signed once and kept after
//! that, because a new launcher is a new program to macOS and loses every
//! grant. `check` runs `oppa connectors test --prompt` as a one-shot job
//! through the same launcher, so macOS asks for, and reports, the access the
//! service itself holds rather than a terminal's.
use crate::{
    config::{ConnectorKind, Paths},
    connectors::PROMPT_TIMEOUT_SECONDS,
};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::{
    fs,
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{Duration, Instant},
};

/// Each launch agent's label is this base and the `oppa` subcommand it runs.
/// OPPA_SERVICE_LABEL names another base for a trial service, so that it never
/// meets the owner's agents; the launcher's signing identifier stays the same.
const LABEL: &str = "ai.oppa";
const SUBCOMMANDS: [&str; 2] = ["serve", "work"];
pub const LAUNCHER_IDENTIFIER: &str = "ai.oppa.launch";
/// launchd's stop deadline for the agents, above the launcher's grace so the
/// launcher, not launchd, ends a draining `oppa`.
const EXIT_TIMEOUT: u64 = 30;
const GRACE: u64 = 25;
/// An upgrade's canary turn may take this long; then both agents must keep the
/// same process for SETTLE seconds, within START seconds of being loaded.
const CANARY: Duration = Duration::from_secs(300);
const SETTLE: Duration = Duration::from_secs(5);
const START: Duration = Duration::from_secs(30);
const BASE_PATH: [&str; 4] = ["/usr/bin", "/bin", "/usr/sbin", "/sbin"];

#[derive(clap::Subcommand)]
pub enum ServiceCommand {
    /// Copy this build into <runtime>/bin, the launcher only the first time,
    /// and write the launch agents. Starts nothing.
    Install {
        /// Replace and re-sign the launcher; macOS then needs every grant again.
        #[arg(long)]
        replace_launcher: bool,
    },
    /// Load both launch agents, restarting any that run.
    Start,
    /// Stop both launch agents; they start again at the next login.
    Stop,
    /// Show the installed build, the launcher and each agent's state and log.
    Status,
    /// Check connector access as the service's launcher, letting macOS ask.
    Check {
        /// Defaults to the time each script connector may wait for an answer
        /// to macOS, so the check never abandons one that is still asking.
        #[arg(long)]
        timeout_seconds: Option<u64>,
    },
    /// Stop the agents and remove them. Binaries and records stay.
    Uninstall,
    /// Install a new build in place of the service's: a canary turn on a copy
    /// of this instance first, then a swap that keeps the previous build as
    /// <runtime>/bin/oppa.previous and puts it back if the agents do not keep
    /// running. The canary writes only to its copy of the instance.
    Upgrade {
        /// The build to install; this executable when not given.
        #[arg(long)]
        from: Option<PathBuf>,
        /// Install a release that moves the database to a newer version; once it
        /// has moved the live database the previous build cannot open it, so it is
        /// not put back.
        #[arg(long)]
        migrate: bool,
    },
}

pub fn run(paths: Paths, command: ServiceCommand) -> Result<Value> {
    let service = Service::new(paths)?;
    match command {
        ServiceCommand::Install { replace_launcher } => {
            service.install(&std::env::current_exe()?, replace_launcher)
        }
        ServiceCommand::Start => service.start(),
        ServiceCommand::Stop => service.stop(),
        ServiceCommand::Status => service.status(),
        ServiceCommand::Check { timeout_seconds } => {
            // Each script connector may wait for macOS to ask, then describe itself.
            let longest: u64 = service
                .paths
                .load()?
                .connectors
                .iter()
                .filter(|c| c.kind == ConnectorKind::Script)
                .map(|c| c.timeout_seconds.max(PROMPT_TIMEOUT_SECONDS) + c.timeout_seconds)
                .sum();
            let timeout = timeout_seconds.unwrap_or(60 + longest);
            service.check(Duration::from_secs(timeout))
        }
        ServiceCommand::Uninstall => service.uninstall(),
        ServiceCommand::Upgrade { from, migrate } => {
            let build = from.map_or_else(std::env::current_exe, Ok)?;
            service.upgrade(&build, migrate)
        }
    }
}

pub struct Service {
    paths: Paths,
    agents_dir: PathBuf,
    domain: String,
    label: String,
}

impl Service {
    pub fn new(paths: Paths) -> Result<Self> {
        let home = std::env::var_os("HOME").context("HOME is unavailable")?;
        let label = match std::env::var("OPPA_SERVICE_LABEL") {
            Err(std::env::VarError::NotPresent) => LABEL.to_owned(),
            Err(std::env::VarError::NotUnicode(_)) => {
                bail!("OPPA_SERVICE_LABEL is not a label base such as ai.oppa.trial")
            }
            Ok(label)
                if !label.is_empty()
                    && label.len() <= 64
                    && !label.starts_with('.')
                    && !label.ends_with('.')
                    && !label.contains("..")
                    && label
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-') =>
            {
                label
            }
            Ok(label) => {
                bail!("OPPA_SERVICE_LABEL {label:?} is not a label base such as ai.oppa.trial")
            }
        };
        Ok(Self {
            paths,
            agents_dir: PathBuf::from(home).join("Library/LaunchAgents"),
            domain: format!("gui/{}", unsafe { libc::getuid() }),
            label,
        })
    }

    /// Each launch agent's label and the `oppa` subcommand it runs.
    fn agents(&self) -> [(String, &'static str); 2] {
        SUBCOMMANDS.map(|subcommand| (format!("{}.{subcommand}", self.label), subcommand))
    }

    fn labels(&self) -> [String; 2] {
        self.agents().map(|(label, _)| label)
    }

    fn bin(&self) -> PathBuf {
        self.paths.runtime.join("bin")
    }

    fn launcher(&self) -> PathBuf {
        self.bin().join("oppa-launch")
    }

    fn plist(&self, label: &str) -> PathBuf {
        self.agents_dir.join(format!("{label}.plist"))
    }

    fn log(&self, name: &str) -> PathBuf {
        self.paths.runtime.join("logs").join(name)
    }

    /// Copy `executable` (and, the first time, the launcher beside it) into
    /// `<runtime>/bin`, and write both launch agents. Nothing is started.
    pub fn install(&self, executable: &Path, replace_launcher: bool) -> Result<Value> {
        self.paths
            .load()
            .context("no instance here; run oppa init first")?;
        self.owned()?;
        self.alone()?;
        // An upgrade keeps the PATH the owner installed with, so running it
        // from a shell with a shorter PATH cannot hide the harness CLIs.
        let path = self
            .plist_value(&self.plist(&self.labels()[0]), "EnvironmentVariables.PATH")
            .unwrap_or_else(|| self.search_path());
        fs::create_dir_all(self.bin())?;
        fs::create_dir_all(self.log(""))?;
        let executable = executable.canonicalize()?;
        let installed = self.bin().join("oppa");
        let copied = if installed.canonicalize().ok().as_deref() == Some(executable.as_path()) {
            false
        } else {
            replace_file(&executable, &installed)?;
            true
        };
        let launcher = self.launcher();
        let launcher_action =
            if !replace_launcher && self.signed_as().as_deref() == Some(LAUNCHER_IDENTIFIER) {
                "kept"
            } else {
                let built = executable.with_file_name("oppa-launch");
                if !built.is_file() {
                    bail!(
                        "{} is missing; build with `cargo build --release` so both binaries exist",
                        built.display()
                    );
                }
                // Signed before it takes the launcher's name, so a failed signature
                // never leaves an unsigned launcher that a later install keeps.
                let staged = launcher.with_extension("new");
                fs::copy(&built, &staged)?;
                fs::set_permissions(&staged, fs::Permissions::from_mode(0o755))?;
                checked(
                    Command::new("codesign")
                        .args([
                            "--force",
                            "--sign",
                            "-",
                            "--identifier",
                            LAUNCHER_IDENTIFIER,
                        ])
                        .arg(&staged),
                )
                .context("could not sign the launcher")?;
                fs::rename(&staged, &launcher)?;
                "installed"
            };
        fs::create_dir_all(&self.agents_dir)?;
        for (label, subcommand) in self.agents() {
            let args = [launcher.to_string_lossy().into_owned(), subcommand.into()];
            let log = self.log(&format!("{subcommand}.log"));
            fs::write(
                self.plist(&label),
                self.render(&label, &args, true, &log, &log, &path),
            )?;
        }
        Ok(json!({
            "oppa": {"path": installed, "copied": copied},
            "launcher": {"path": launcher, "action": launcher_action,
                "grant_again": launcher_action == "installed",
                "full_disk_access": "grant to this file in System Settings, Privacy & Security, Full Disk Access"},
            "launch_agents": self.labels().iter().map(|label| self.plist(label)).collect::<Vec<_>>(),
            "next": "oppa service start",
        }))
    }

    /// (Re)load both agents from their plists; a loaded agent is stopped first
    /// so an edited plist takes effect.
    pub fn start(&self) -> Result<Value> {
        self.owned()?;
        self.alone()?;
        for label in self.labels() {
            if !self.plist(&label).is_file() {
                bail!("{label} is not installed; run oppa service install");
            }
        }
        self.release(&self.labels())?;
        for label in self.labels() {
            self.bootstrap(&self.plist(&label))?;
        }
        self.status()
    }

    pub fn stop(&self) -> Result<Value> {
        self.owned()?;
        self.release(&self.labels())?;
        self.status()
    }

    /// Stop the agents and remove their plists. Binaries, logs and every
    /// record stay; the launcher's grants stay with the launcher.
    pub fn uninstall(&self) -> Result<Value> {
        self.owned()?;
        self.release(&self.labels())?;
        for label in self.labels() {
            match fs::remove_file(self.plist(&label)) {
                Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error)?,
                _ => {}
            }
        }
        self.status()
    }

    pub fn status(&self) -> Result<Value> {
        let installed = self.bin().join("oppa");
        let this_build = std::env::current_exe()
            .ok()
            .and_then(|exe| fs::read(exe).ok());
        let current = this_build.is_some() && fs::read(&installed).ok() == this_build;
        let identifier = self.signed_as();
        let mine = self.paths.runtime.canonicalize().ok();
        let agents = self
            .agents()
            .iter()
            .map(|(label, subcommand)| -> Result<Value> {
                let job = self.job(label)?;
                let log = self.log(&format!("{subcommand}.log"));
                // The labels are shared by every instance on this Mac.
                let instances = self.instances_of(label)?;
                let instance = instances.first().cloned();
                Ok(json!({
                    "label": label,
                    "installed": self.plist(label).is_file(),
                    "instance": instance,
                    "this_instance": !instances.is_empty()
                        && instances.iter().all(|i| Some(i) == mine.as_ref()),
                    "loaded": job.is_some(),
                    "state": job.as_ref().and_then(|j| j.get("state").cloned()),
                    "pid": job.as_ref().and_then(|j| j.get("pid").cloned()),
                    "runs": job.as_ref().and_then(|j| j.get("runs").cloned()),
                    "last_exit": job.as_ref().and_then(|j| j.get("last exit code").cloned()),
                    "log": log,
                    "log_tail": tail(&log, 5),
                }))
            })
            .collect::<Result<Vec<Value>>>()?;
        let running = agents
            .iter()
            .all(|a| a["state"] == "running" && a["this_instance"] == true);
        Ok(json!({
            "runtime": self.paths.runtime,
            "oppa": {"path": installed, "present": installed.is_file(), "is_this_build": current},
            "launcher": {"path": self.launcher(), "present": self.launcher().is_file(),
                "signed_as": identifier, "expected": LAUNCHER_IDENTIFIER},
            "agents": agents,
            "running": running,
        }))
    }

    /// Install `build` as the service's `oppa`. Its canary turn on a copy of this
    /// instance must answer first, so a release that cannot never reaches the
    /// service. Then the agents stop, the build takes `oppa`'s place with the
    /// previous one kept as `oppa.previous`, and both agents must start and
    /// keep running, or the previous build goes back and they start again.
    pub fn upgrade(&self, build: &Path, migrate: bool) -> Result<Value> {
        if std::env::var_os(crate::harness::RUN_MARKER).is_some() {
            bail!("an upgrade stops the service and every run in it, this one too; run it from a terminal");
        }
        self.paths
            .load()
            .context("no instance here; run oppa init first")?;
        self.owned()?;
        self.alone()?;
        let installed = self.bin().join("oppa");
        for label in self.labels() {
            if !self.plist(&label).is_file() || !installed.is_file() {
                bail!("{label} is not installed; run oppa service install first");
            }
        }
        let build = build
            .canonicalize()
            .with_context(|| format!("{} does not exist", build.display()))?;
        if fs::read(&installed)? == fs::read(&build)? {
            bail!("{} is already the service's build", build.display());
        }
        let canary = self.canary(&build)?;
        let unchanged = |problem: String| {
            Ok(
                json!({"ready": false, "upgraded": false, "problem": problem, "canary": canary,
                "service": "unchanged"}),
            )
        };
        if canary["answered"] != true {
            return unchanged("the release did not answer its canary turn".into());
        }
        if canary["migrates"] == true && !migrate {
            return unchanged(format!(
                "the release moves the database from version {} to {}, after which the previous build \
                 cannot open it; run again with --migrate to install it without a way back",
                canary["schema_before"], canary["schema_after"]
            ));
        }
        // Staged beside `oppa`, so the swap is two renames within one folder.
        let staged = self.bin().join("oppa.next");
        replace_file(&build, &staged)?;
        let previous = self.bin().join("oppa.previous");
        // Until the release is in place, a failure leaves the build as it was, running.
        let swapped = self
            .release(&self.labels())
            .and_then(|()| Ok(fs::rename(&installed, &previous)?))
            .and_then(|()| {
                fs::rename(&staged, &installed).or_else(|error| {
                    fs::rename(&previous, &installed)?;
                    Err(error)
                })?;
                Ok(())
            });
        if let Err(error) = swapped {
            let _ = fs::remove_file(&staged);
            let restarted = self.start_agents();
            return Err(error).context(match restarted {
                Ok(()) => {
                    "could not put the release in place; the service runs its build as before"
                }
                Err(_) => "could not put the release in place, nor start the service again",
            });
        }
        // From here an error is a failure like any other: the previous build goes back.
        let health = self
            .start_agents()
            .and_then(|()| self.settled())
            .unwrap_or_else(|error| json!({"settled": false, "error": format!("{error:#}")}));
        if health["settled"] == true {
            return Ok(
                json!({"ready": true, "upgraded": true, "build": build, "previous": previous,
                "canary": canary, "health": health}),
            );
        }
        // The release stops first, so it cannot move the database on while this looks.
        // Only a release that has moved it stays: the previous build would refuse it. One
        // that failed before that goes back like any other; a database that cannot be
        // read keeps the release, which is said.
        if let Err(error) = self.release(&self.labels()) {
            return Ok(
                json!({"ready": false, "upgraded": true, "problem": format!("the release did \
                not keep running as the service, and it could not be stopped to put the previous \
                build back: {error:#}"), "canary": canary, "health": health}),
            );
        }
        let kept = match self.schema_version() {
            Ok(now) => match (now, canary["schema_before"].as_i64()) {
                (Some(now), Some(before)) if now > before => {
                    Some("it moved the database to a version the previous build cannot open")
                }
                _ => None,
            },
            Err(_) => Some("its database's version could not be read"),
        };
        if let Some(why) = kept {
            // Loaded again, as it was, for launchd to keep trying.
            let _ = self.start_agents();
            return Ok(
                json!({"ready": false, "upgraded": true, "problem": format!("the release did \
                not keep running as the service, and {why}, so it stays"), "canary": canary,
                "health": health}),
            );
        }
        let failed = self.bin().join("oppa.failed");
        let restored = fs::rename(&installed, &failed)
            .map_err(anyhow::Error::from)
            .and_then(|()| Ok(fs::rename(&previous, &installed)?))
            .and_then(|()| self.start_agents())
            .and_then(|()| self.settled());
        Ok(match restored {
            Ok(after) => json!({"ready": false, "upgraded": false, "rolled_back": true,
                "problem": "the release did not keep running as the service, so the previous build is back",
                "failed_build": failed, "canary": canary, "health": health, "after_rollback": after}),
            Err(error) => json!({"ready": false, "upgraded": false, "rolled_back": false,
                "problem": format!("the release did not keep running as the service, and putting the \
                previous build back failed: {error:#}"), "canary": canary, "health": health}),
        })
    }

    /// The live database's version, read without writing; None when there is none.
    fn schema_version(&self) -> Result<Option<i64>> {
        version_of(&self.paths.database())
    }

    fn start_agents(&self) -> Result<()> {
        for label in self.labels() {
            self.bootstrap(&self.plist(&label))?;
        }
        Ok(())
    }

    /// One turn by `build` on a copy of this instance: its private folder, at read
    /// access and with every path into the instance pointed at the copy, and an
    /// empty database with the live one's schema and version. The turn has a fresh
    /// conversation, so no conversation or native session of the owner's reaches
    /// it. The copy goes when it ends.
    fn canary(&self, build: &Path) -> Result<Value> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .subsec_nanos();
        // A copy an interrupted upgrade left behind goes first: its process is gone.
        for entry in fs::read_dir(std::env::temp_dir())?.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let pid = name
                .strip_prefix("oppa-canary-")
                .and_then(|rest| rest.split('-').next())
                .and_then(|pid| pid.parse::<i32>().ok());
            let gone = |pid: i32| {
                (unsafe { libc::kill(pid, 0) }) != 0
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            };
            if pid.is_some_and(gone) {
                let _ = fs::remove_dir_all(entry.path());
            }
        }
        let root = Scratch(
            std::env::temp_dir().join(format!("oppa-canary-{}-{nanos:x}", std::process::id())),
        );
        fs::create_dir(&root.0)?;
        fs::set_permissions(&root.0, fs::Permissions::from_mode(0o700))?;
        let copy = Paths::new(root.0.join("runtime"), Some(root.0.join("private")))?;
        fs::create_dir_all(&copy.runtime)?;
        checked(
            Command::new("/bin/cp")
                .arg("-cR")
                .arg(&self.paths.private)
                .arg(&copy.private),
        )?;
        copy.prepare()?;
        let mut config = fs::read_to_string(copy.config_file())?;
        // Each folder as resolved (/private/var/...) and as given (/var/...),
        // the longest first; boundaries keep ~/.oppa from matching in ~/.oppa-private.
        let mut moves = Vec::new();
        for (from, to) in [
            (&self.paths.runtime, &copy.runtime),
            (&self.paths.private, &copy.private),
        ] {
            let resolved = from.canonicalize().unwrap_or_else(|_| from.clone());
            for from in [resolved, from.clone()] {
                moves.push((
                    from.to_string_lossy().into_owned(),
                    to.to_string_lossy().into_owned(),
                ));
            }
        }
        moves.sort_by_key(|(from, _)| std::cmp::Reverse(from.len()));
        for (from, to) in moves {
            config = replace_path(&config, &from, &to);
        }
        // Read access and no connector: the canary cannot act and has no script
        // connector; as any read run on Claude, it may read the owner's Gmail,
        // Calendar and Drive, and waits for those connectors to load. Channels stay, since people's handles name them;
        // `oppa chat` never starts one, and with an empty database and no serve
        // on the copy, a turn's channel scopes find nothing and send nothing.
        let mut config: crate::config::Config = toml::from_str(&config)?;
        config.access = crate::harness::Access::Read;
        config.connectors.clear();
        let config = toml::to_string(&config)?;
        fs::write(copy.config_file(), config)?;
        let before = self.schema_version()?;
        if before.is_some() {
            clone_schema(&self.paths.database(), &copy.database())?;
        }
        let word = format!("canary-{:x}", nanos ^ std::process::id().rotate_left(16));
        let prompt = format!(
            "This turn checks a new OPPA build before it is installed. Reply with exactly this word \
             and nothing else: {word}"
        );
        let (code, out, err) = bounded(
            Command::new(build)
                .args([
                    "chat",
                    "--conversation",
                    &word,
                    "--message",
                    &prompt,
                    "--home",
                ])
                .arg(&copy.runtime)
                .arg("--private-dir")
                .arg(&copy.private)
                .env_remove("OPPA_HOME")
                .env_remove("OPPA_PRIVATE_DIR"),
            CANARY,
        )?;
        let after = match before {
            Some(_) => version_of(&copy.database())?,
            None => None,
        };
        let line = |text: &str| text.chars().take(400).collect::<String>();
        Ok(
            json!({"answered": code == Some(0) && out.contains(&word), "exit": code,
            "reply": line(out.trim()), "error": line(err.trim()),
            "schema_before": before, "schema_after": after,
            "migrates": before.is_some() && after > before}),
        )
    }

    /// Whether both agents run and keep the same process for SETTLE seconds,
    /// watched for up to START seconds. A release that exits is restarted by
    /// launchd, which counts another run, so it fails at once.
    fn settled(&self) -> Result<Value> {
        let deadline = Instant::now() + START;
        let mut steady: Option<(Instant, Vec<String>)> = None;
        loop {
            let jobs: Vec<_> = self
                .labels()
                .iter()
                .map(|label| Ok(self.job(label)?.unwrap_or_default()))
                .collect::<Result<_>>()?;
            // Each job was just loaded, so a second run is the release exiting and
            // launchd starting it again; launchd may still say 0 before the first.
            let restarted = jobs.iter().any(|job| {
                job.get("runs")
                    .and_then(|runs| runs.parse::<u32>().ok())
                    .is_some_and(|runs| runs >= 2)
            });
            let pids: Vec<String> = jobs
                .iter()
                .filter(|job| job.get("state").is_some_and(|state| state == "running"))
                .filter_map(|job| job.get("pid").cloned())
                .collect();
            let now = Instant::now();
            match &steady {
                _ if pids.len() < jobs.len() => steady = None,
                Some((since, seen)) if *seen == pids => {
                    if now.duration_since(*since) >= SETTLE {
                        return Ok(json!({"settled": true, "pids": pids}));
                    }
                }
                _ => steady = Some((now, pids)),
            }
            if restarted || now > deadline {
                let agents: Vec<Value> = self
                    .labels()
                    .iter()
                    .zip(jobs)
                    .map(|(label, job)| json!({"label": label, "job": job}))
                    .collect();
                return Ok(json!({"settled": false, "restarted": restarted, "agents": agents}));
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    /// Run `oppa connectors test --prompt` through the installed launcher as a
    /// one-shot launch agent and return its report. macOS attributes the
    /// access, and any prompt, to the launcher, as it does for the service.
    pub fn check(&self, timeout: Duration) -> Result<Value> {
        if !self.launcher().is_file() || !self.bin().join("oppa").is_file() {
            bail!("the service is not installed; run oppa service install");
        }
        // One check per instance at a time, under a label no other instance uses.
        fs::create_dir_all(self.paths.runtime.join("locks"))?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(self.paths.runtime.join("locks").join("check.lock"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                bail!("another service check is running for this instance")
            }
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(error).context("could not lock the service check")
            }
        }
        let (out, err) = (self.log("check.json"), self.log("check.err"));
        for file in [&out, &err] {
            let _ = fs::remove_file(file);
        }
        let label = self.check_label();
        let plist = self.paths.runtime.join("check.plist");
        let args = ["connectors", "test", "--prompt"].map(String::from);
        let args: Vec<String> = std::iter::once(self.launcher().to_string_lossy().into_owned())
            .chain(args)
            .collect();
        let path = self
            .plist_value(&self.plist(&self.labels()[0]), "EnvironmentVariables.PATH")
            .unwrap_or_else(|| self.search_path());
        fs::write(&plist, self.render(&label, &args, false, &out, &err, &path))?;
        self.release(&[&label])?;
        self.bootstrap(&plist)?;
        let deadline = Instant::now() + timeout;
        let exit = loop {
            let job = self.job(&label)?.unwrap_or_default();
            if job.get("state").is_some_and(|s| s == "not running")
                && job.get("runs").is_some_and(|r| r != "0")
            {
                break job.get("last exit code").cloned();
            }
            if Instant::now() > deadline {
                let _ = self.release(&[&label]);
                bail!(
                    "the check did not finish in time; answer any macOS prompt, then run it again"
                );
            }
            std::thread::sleep(Duration::from_millis(250));
        };
        self.release(&[&label])?;
        let _ = fs::remove_file(&plist);
        let report = fs::read_to_string(&out).unwrap_or_default();
        let connectors: Value = serde_json::from_str(&report).unwrap_or(Value::Null);
        let ready = exit.as_deref() == Some("0") && connectors.is_array();
        Ok(json!({
            "as": "the service's launcher",
            "deadline_seconds": timeout.as_secs(),
            "ready": ready,
            "exit": exit,
            "connectors": connectors,
            "error": tail(&err, 5),
        }))
    }

    /// Refuse when an installed agent runs another existing instance, so a trial
    /// instance can never replace, stop or remove the owner's service.
    fn owned(&self) -> Result<()> {
        let mine = self.paths.runtime.canonicalize().ok();
        for label in self.labels() {
            // What runs now and what the next start loads must both be ours.
            if self
                .instances_of(&label)?
                .iter()
                .any(|theirs| Some(theirs) != mine.as_ref())
            {
                bail!(
                    "{label} belongs to another instance; run `oppa service uninstall` with that instance's --home first"
                );
            }
        }
        Ok(())
    }

    /// Refuse when agents under another label base serve this instance: one
    /// instance has one pair, so a trial label never runs the owner's instance
    /// and the owner's never runs a trial's. Stopping and removing stay possible.
    fn alone(&self) -> Result<()> {
        let mine = self.paths.runtime.canonicalize().ok();
        let ours = self.labels();
        for entry in fs::read_dir(&self.agents_dir)
            .into_iter()
            .flatten()
            .flatten()
        {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(label) = name.strip_suffix(".plist") else {
                continue;
            };
            if ours.iter().any(|own| own == label) {
                continue;
            }
            // Only an OPPA agent, one that runs an oppa-launch; read from its plist, so
            // another program's agent is never asked about.
            let plist = entry.path();
            let arguments = self.plist_arguments(&plist);
            let launcher = arguments.first().is_some_and(|program| {
                Path::new(program).file_name() == Some("oppa-launch".as_ref())
            });
            if !launcher {
                continue;
            }
            let home = self.plist_value(&plist, "EnvironmentVariables.OPPA_HOME");
            let user = self.plist_value(&plist, "EnvironmentVariables.HOME");
            if mine.is_some() && serving(&arguments, home, user).as_ref() == mine.as_ref() {
                bail!(
                    "{label} already serves this instance; one instance has one pair of agents, so a trial label is for another instance (--home or OPPA_HOME)"
                );
            }
        }
        Ok(())
    }

    fn plist_arguments(&self, plist: &Path) -> Vec<String> {
        crate::process::output(
            Command::new("/usr/bin/plutil")
                .args(["-extract", "ProgramArguments", "json", "-o", "-"])
                .arg(plist),
        )
        .ok()
        .and_then(|output| serde_json::from_slice(&output.stdout).ok())
        .unwrap_or_default()
    }

    fn check_label(&self) -> String {
        use sha2::{Digest, Sha256};
        let runtime = self
            .paths
            .runtime
            .canonicalize()
            .unwrap_or(self.paths.runtime.clone());
        let digest = Sha256::digest(runtime.as_os_str().as_encoded_bytes());
        format!(
            "{}.check.{:x}",
            self.label,
            u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]])
        )
    }

    /// The instances an agent's label serves: the job launchd has loaded, which
    /// is what runs now, then the plist on disk, which is what the next start
    /// loads. Either may be missing, and an instance that no longer exists is
    /// nobody's, so it is left out.
    fn instances_of(&self, label: &str) -> Result<Vec<PathBuf>> {
        let mut found = Vec::new();
        if let Some(text) = self.print(label)? {
            let environment = block(&text, "environment");
            let variable = |name: &str| {
                environment
                    .iter()
                    .find_map(|line| line.strip_prefix(&format!("{name} => ")).map(str::to_owned))
            };
            found.extend(serving(
                &block(&text, "arguments"),
                variable("OPPA_HOME"),
                variable("HOME"),
            ));
        }
        let plist = self.plist(label);
        if plist.exists() {
            let arguments = self.plist_arguments(&plist);
            let home = self.plist_value(&plist, "EnvironmentVariables.OPPA_HOME");
            let user = self.plist_value(&plist, "EnvironmentVariables.HOME");
            found.extend(serving(&arguments, home, user));
        }
        Ok(found)
    }

    fn plist_value(&self, plist: &Path, key: &str) -> Option<String> {
        let output = crate::process::output(
            Command::new("/usr/bin/plutil")
                .args(["-extract", key, "raw", "-o", "-"])
                .arg(plist),
        )
        .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
    }

    fn signed_as(&self) -> Option<String> {
        let output =
            crate::process::output(Command::new("codesign").arg("-dv").arg(self.launcher()))
                .ok()?;
        String::from_utf8_lossy(&output.stderr)
            .lines()
            .find_map(|line| line.strip_prefix("Identifier=").map(str::to_owned))
    }

    fn render(
        &self,
        label: &str,
        args: &[String],
        keep_alive: bool,
        out: &Path,
        err: &Path,
        path: &str,
    ) -> String {
        let string = |value: &str| format!("<string>{}</string>", xml(value));
        let program: String = args.iter().map(|a| string(a)).collect();
        let env = [
            ("HOME", std::env::var("HOME").unwrap_or_default()),
            (
                "OPPA_HOME",
                self.paths.runtime.to_string_lossy().into_owned(),
            ),
            (
                "OPPA_PRIVATE_DIR",
                self.paths.private.to_string_lossy().into_owned(),
            ),
            ("OPPA_LAUNCH_GRACE_SECONDS", GRACE.to_string()),
            ("PATH", path.to_owned()),
        ];
        let env: String = env
            .iter()
            .map(|(key, value)| format!("<key>{key}</key>{}", string(value)))
            .collect();
        let (out, err) = (xml(out), xml(err));
        let runtime = xml(&self.paths.runtime);
        let keep = if keep_alive { "true" } else { "false" };
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
<key>Label</key><string>{label}</string>
<key>ProgramArguments</key><array>{program}</array>
<key>EnvironmentVariables</key><dict>{env}</dict>
<key>WorkingDirectory</key><string>{runtime}</string>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><{keep}/>
<key>ProcessType</key><string>Interactive</string>
<key>ThrottleInterval</key><integer>10</integer>
<key>ExitTimeOut</key><integer>{EXIT_TIMEOUT}</integer>
<key>StandardOutPath</key><string>{out}</string>
<key>StandardErrorPath</key><string>{err}</string>
</dict>
</plist>
"#
        )
    }

    /// The first install's PATH: `<runtime>/bin`, then the system
    /// directories, so `python3` is the system interpreter the adapters are
    /// accepted with, then the installer's own PATH, which is where its
    /// signed-in harness CLIs were found. Temporary directories are left out:
    /// they do not outlive the session that made them. Everything on it runs
    /// under the launcher's grants.
    fn search_path(&self) -> String {
        let mut path = vec![self.bin().to_string_lossy().into_owned()];
        path.extend(BASE_PATH.map(String::from));
        let inherited = std::env::var_os("PATH").unwrap_or_default();
        for dir in std::env::split_paths(&inherited) {
            let text = dir.to_string_lossy().into_owned();
            let temporary = [
                "/tmp/",
                "/private/tmp/",
                "/var/folders/",
                "/private/var/folders/",
            ]
            .iter()
            .any(|prefix| text.starts_with(prefix));
            if dir.is_absolute() && dir.is_dir() && !temporary && !path.contains(&text) {
                path.push(text);
            }
        }
        path.join(":")
    }

    fn bootstrap(&self, plist: &Path) -> Result<()> {
        // A job just booted out can still be tearing down; launchd refuses
        // the new bootstrap until it is gone.
        let mut attempt = 0;
        loop {
            let result = checked(
                Command::new("launchctl")
                    .arg("bootstrap")
                    .arg(&self.domain)
                    .arg(plist),
            );
            match result {
                Ok(_) => return Ok(()),
                Err(_) if attempt < 10 => {
                    attempt += 1;
                    std::thread::sleep(Duration::from_millis(500));
                }
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("could not load {}", plist.display()))
                }
            }
        }
    }

    /// Unload the jobs and wait until launchd has let them go. `bootout`
    /// returns while a process still drains, for up to the launcher's grace,
    /// and a bootstrap before then fails. All drain at once. Harnesses and
    /// connectors run in process groups of their own, which launchd does not
    /// stop with a job, so whatever of them outlives it is stopped then.
    fn release(&self, labels: &[impl AsRef<str>]) -> Result<()> {
        let labels: Vec<&str> = labels.iter().map(AsRef::as_ref).collect();
        let mut seen = Vec::new();
        for &label in &labels {
            note_below(&mut seen, self.job(label)?.as_ref());
            let output = crate::process::output(
                Command::new("launchctl")
                    .arg("bootout")
                    .arg(format!("{}/{label}", self.domain)),
            )
            .context("could not run launchctl")?;
            // 3: nothing loaded; 36: already being unloaded.
            if !matches!(output.status.code(), Some(0 | 3 | 36)) {
                bail!(
                    "launchctl could not unload {label}: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                );
            }
        }
        // Every agent drains at once, and a draining one can still start a
        // harness or a connector, so each pass looks below all that remain.
        let deadline = Instant::now() + Duration::from_secs(EXIT_TIMEOUT + 10);
        let mut loaded = labels;
        loop {
            let mut remaining = Vec::new();
            for label in loaded {
                if let Some(job) = self.job(label)? {
                    note_below(&mut seen, Some(&job));
                    remaining.push(label);
                }
            }
            loaded = remaining;
            if loaded.is_empty() || Instant::now() > deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        stop_left(&seen);
        if let Some(label) = loaded.first() {
            bail!("{label} did not stop within {EXIT_TIMEOUT} seconds");
        }
        Ok(())
    }

    /// What `launchctl print` says of a loaded job, or None when it is not loaded.
    fn print(&self, label: &str) -> Result<Option<String>> {
        let output = crate::process::output(
            Command::new("launchctl")
                .arg("print")
                .arg(format!("{}/{label}", self.domain)),
        )
        .context("could not run launchctl")?;
        // 113 is launchd having no such job; anything else must not pass for it.
        match output.status.code() {
            Some(0) => Ok(Some(String::from_utf8_lossy(&output.stdout).into_owned())),
            Some(113) => Ok(None),
            _ => bail!(
                "launchctl could not read {label}: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ),
        }
    }

    fn job(&self, label: &str) -> Result<Option<std::collections::HashMap<String, String>>> {
        Ok(self.print(label)?.map(|text| {
            text.lines()
                .filter_map(|line| line.strip_prefix('\t'))
                .filter(|line| !line.starts_with('\t'))
                .filter_map(|line| line.split_once(" = "))
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect()
        }))
    }
}

/// The instance an agent's arguments and environment name: `--home` outranks
/// OPPA_HOME, as it does for oppa, and neither means `.oppa` in the agent's
/// HOME, which is this process's unless the agent sets its own.
fn serving(arguments: &[String], home: Option<String>, user: Option<String>) -> Option<PathBuf> {
    let flag = arguments
        .iter()
        .position(|a| a == "--home")
        .and_then(|i| arguments.get(i + 1).cloned())
        .or_else(|| {
            arguments
                .iter()
                .find_map(|a| a.strip_prefix("--home=").map(str::to_owned))
        });
    let home = flag.or(home).map(PathBuf::from).or_else(|| {
        user.map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
            .map(|home| home.join(".oppa"))
    })?;
    // A relative path names a place relative to the job, not to this process,
    // so it is kept as written and never matches this instance.
    if home.is_relative() {
        return Some(home);
    }
    home.canonicalize().ok()
}

/// A process as `ps` shows it: its id, its group, and when it started, which
/// tells it from a later process given the same id.
#[derive(Clone, PartialEq)]
struct Seen {
    pid: i32,
    group: i32,
    started: String,
}

/// Each process with its parent's id, from `ps`.
fn processes() -> Vec<(Seen, i32)> {
    let Ok(output) = crate::process::output(Command::new("/bin/ps").args([
        "-A",
        "-o",
        "pid=,ppid=,pgid=,lstart=",
    ])) else {
        return Vec::new();
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            let mut number = || fields.next()?.parse::<i32>().ok();
            let (pid, parent, group) = (number()?, number()?, number()?);
            let started = fields.collect::<Vec<_>>().join(" ");
            Some((
                Seen {
                    pid,
                    group,
                    started,
                },
                parent,
            ))
        })
        .collect()
}

/// Add a job's process and everything below it to `seen`, read while it runs.
fn note_below(seen: &mut Vec<Seen>, job: Option<&std::collections::HashMap<String, String>>) {
    let Some(pid) = job.and_then(|job| job.get("pid")?.parse::<i32>().ok()) else {
        return;
    };
    let all = processes();
    let mut tree = vec![pid];
    let mut next = 0;
    while next < tree.len() {
        let parent = tree[next];
        tree.extend(all.iter().filter(|p| p.1 == parent).map(|p| p.0.pid));
        next += 1;
    }
    for (process, _) in all.into_iter().filter(|p| tree.contains(&p.0.pid)) {
        if !seen.contains(&process) {
            seen.push(process);
        }
    }
}

/// Stop the process groups that outlived their job: only a group in which a
/// process seen below the job still runs, the same process by its start time.
/// Never this process's own group.
fn stop_left(seen: &[Seen]) {
    let own = unsafe { libc::getpgrp() };
    let mut groups: Vec<i32> = processes()
        .into_iter()
        .map(|p| p.0)
        .filter(|p| seen.contains(p) && p.group > 1 && p.group != own)
        .map(|p| p.group)
        .collect();
    groups.sort_unstable();
    groups.dedup();
    for group in groups {
        unsafe { libc::kill(-group, libc::SIGKILL) };
    }
}

/// The lines of a `name = { ... }` block in `launchctl print` output.
fn block(text: &str, name: &str) -> Vec<String> {
    let opening = format!("\t{name} = {{");
    text.lines()
        .skip_while(|line| *line != opening)
        .skip(1)
        .take_while(|line| line.trim() != "}")
        .map(|line| line.trim().to_owned())
        .collect()
}

/// Write beside the target, then rename over it, so a running copy keeps its
/// file and nothing ever runs a half-written binary.
fn replace_file(from: &Path, to: &Path) -> Result<()> {
    let staged = to.with_extension("new");
    fs::copy(from, &staged).with_context(|| format!("could not copy {}", from.display()))?;
    fs::set_permissions(&staged, fs::Permissions::from_mode(0o755))?;
    fs::rename(&staged, to)?;
    Ok(())
}

/// A database's version, read without writing; None when there is none.
fn version_of(path: &Path) -> Result<Option<i64>> {
    if !path.is_file() {
        return Ok(None);
    }
    let database =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    database.busy_timeout(Duration::from_secs(5))?;
    Ok(Some(database.query_row(
        "PRAGMA user_version",
        [],
        |r| r.get(0),
    )?))
}

/// An empty database at `to` with the tables, indexes, triggers and version of the
/// one at `from`, which is only read: a database like the live one with no
/// conversation in it.
fn clone_schema(from: &Path, to: &Path) -> Result<()> {
    let live =
        rusqlite::Connection::open_with_flags(from, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    live.busy_timeout(Duration::from_secs(5))?;
    let objects: Vec<(String, String, String)> = live
        .prepare(
            "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL \
             AND name NOT LIKE 'sqlite_%' ORDER BY rowid",
        )?
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let version: i64 = live.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    let empty = rusqlite::Connection::open(to)?;
    let virtuals: Vec<&String> = objects
        .iter()
        .filter(|(_, _, sql)| sql.starts_with("CREATE VIRTUAL TABLE"))
        .map(|(_, name, _)| name)
        .collect();
    for (kind, name, sql) in &objects {
        // A virtual table makes its own shadow tables, which are tables named after
        // it; its triggers are made like any other object.
        let shadow = kind == "table" && virtuals.iter().any(|v| name.starts_with(&format!("{v}_")));
        if !shadow {
            empty.execute_batch(sql)?;
        }
    }
    empty.execute_batch(&format!("PRAGMA user_version={version}"))?;
    Ok(())
}

/// `text` with each whole path `from` replaced by `to`: preceded by the text's
/// start, a quote, `=` or whitespace, and followed by its end, `/` or a quote.
fn replace_path(text: &str, from: &str, to: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(from) {
        let before = rest[..at].chars().last().or_else(|| out.chars().last());
        let after = rest[at + from.len()..].chars().next();
        let whole = before.is_none_or(|c| matches!(c, '"' | '\'' | '=') || c.is_whitespace())
            && after.is_none_or(|c| matches!(c, '/' | '"' | '\''));
        out.push_str(&rest[..at]);
        out.push_str(if whole { to } else { from });
        rest = &rest[at + from.len()..];
    }
    out.push_str(rest);
    out
}

/// A private folder removed with everything in it when dropped.
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Run a command to its end or `limit`, whichever comes first, and return its
/// exit code (None when it was stopped), stdout and stderr.
fn bounded(command: &mut Command, limit: Duration) -> Result<(Option<i32>, String, String)> {
    use std::io::Read;
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0);
    let mut child =
        crate::process::spawning(|| command.spawn()).context("could not start the build")?;
    // Read on threads that report through a channel, so a harness the build left
    // holding its pipes cannot keep this waiting.
    let (sender, receiver) = std::sync::mpsc::channel();
    let pipes: [Box<dyn Read + Send>; 2] = [
        Box::new(child.stdout.take().context("stdout")?),
        Box::new(child.stderr.take().context("stderr")?),
    ];
    for (index, mut pipe) in pipes.into_iter().enumerate() {
        let sender = sender.clone();
        std::thread::spawn(move || {
            let mut text = String::new();
            let _ = pipe.read_to_string(&mut text);
            let _ = sender.send((index, text));
        });
    }
    drop(sender);
    let deadline = Instant::now() + limit;
    let code = loop {
        if let Some(status) = child.try_wait()? {
            break status.code();
        }
        if Instant::now() > deadline {
            // The harness and connectors run in process groups of their own, so they
            // are found below the build and stopped with it.
            let mut below = Vec::new();
            let pid = std::collections::HashMap::from([("pid".to_owned(), child.id().to_string())]);
            note_below(&mut below, Some(&pid));
            unsafe { libc::killpg(child.id() as i32, libc::SIGKILL) };
            child.wait()?;
            stop_left(&below);
            break None;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let mut texts = [String::new(), String::new()];
    let until = Instant::now() + Duration::from_secs(5);
    while let Ok((index, text)) =
        receiver.recv_timeout(until.saturating_duration_since(Instant::now()))
    {
        texts[index] = text;
    }
    let [out, err] = texts;
    Ok((code, out, err))
}

fn checked(command: &mut Command) -> Result<Output> {
    let output = crate::process::output(command)?;
    if !output.status.success() {
        bail!(
            "{}",
            String::from_utf8_lossy(&output.stderr).trim().to_owned()
        );
    }
    Ok(output)
}

/// The last lines of a log that is never rotated, read from its end.
fn tail(path: &Path, lines: usize) -> Vec<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut bytes = Vec::new();
    if let Ok(mut file) = fs::File::open(path) {
        let start = file
            .metadata()
            .map_or(0, |m| m.len().saturating_sub(64 * 1024));
        let _ = file
            .seek(SeekFrom::Start(start))
            .and_then(|_| file.read_to_end(&mut bytes));
    }
    let text = String::from_utf8_lossy(&bytes);
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..]
        .iter()
        .map(|line| line.chars().take(300).collect())
        .collect()
}

fn xml(value: impl AsRef<std::ffi::OsStr>) -> String {
    value
        .as_ref()
        .to_string_lossy()
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
