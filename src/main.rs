use anyhow::{bail, ensure, Context, Result};
use chrono::Timelike;
use clap::{Parser, Subcommand};
use oppa::{
    broker::Broker,
    config::{self, Config, ConnectorKind, ConnectorManifest, Paths},
    connectors,
    harness::{self, AgentManifest},
    launch, mcp,
    runtime::{self, ConsoleDelivery, Runtime},
    schedule::{self, Schedule, ScheduleStore},
    store::Store,
    tasks::{TaskState, TaskStore},
    vault::{Edit, Provenance, Vault},
    worker::{WorkResult, Worker},
};
use std::{
    fs,
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    process::Command,
};

#[derive(Parser)]
#[command(
    name = "oppa",
    version,
    about = "Open Personal Assistant: a local-first chief of staff"
)]
struct Cli {
    /// Private runtime directory; must be outside any source checkout.
    #[arg(long, global = true, env = "OPPA_HOME")]
    home: Option<PathBuf>,
    /// Private configuration directory. Defaults to a sibling of runtime.
    #[arg(long, global = true, env = "OPPA_PRIVATE_DIR")]
    private_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a separate instance. Existing storage is never overwritten.
    Init {
        #[arg(long)]
        agent: Option<String>,
        #[arg(long, default_value = "owner")]
        owner: String,
        /// How far conversation turns may act, from the first one: read,
        /// connectors (the harness's accounts, web and browser, no shell) or
        /// full (every tool and file). Asked at a terminal; connectors otherwise.
        #[arg(long, value_enum)]
        access: Option<harness::Access>,
        /// The model the harness runs; its own default when absent.
        #[arg(long)]
        model: Option<String>,
        /// How hard the model thinks: low, medium, high, xhigh or max.
        #[arg(long, default_value = "medium")]
        effort: String,
        /// Keep the harness's own browser (Claude in Chrome, Codex browser use)
        /// out of the assistant's runs.
        #[arg(long)]
        no_browser: bool,
        /// A 1Password vault the assistant reads keys and passwords from, with
        /// a service account token in the Keychain under oppa-1password.
        #[arg(long)]
        vault: Option<String>,
        /// A knowledge wiki the owner already keeps (a Git repository of
        /// Markdown notes with an AGENTS.md) to link at full access; otherwise
        /// a new one is started.
        #[arg(long)]
        knowledge: Option<PathBuf>,
        /// Answer in web chat on this Mac, on this loopback port (8765 if none
        /// is given; 0 picks a free one).
        #[arg(long, num_args = 0..=1, default_missing_value = "8765")]
        web: Option<u16>,
        /// Answer on iMessage, from the assistant's own Apple ID, to these
        /// phone numbers (+15555550100) or email addresses of the owner.
        #[arg(long, value_delimiter = ',')]
        imessage: Vec<String>,
    },
    /// Converse through an installed subscription harness.
    Chat {
        #[arg(long, default_value = "console")]
        conversation: String,
        #[arg(long)]
        message: Option<String>,
        /// A file to attach to --message; repeat for more.
        #[arg(long, requires = "message")]
        attach: Vec<PathBuf>,
    },
    /// Internal owner-local MCP stdio endpoint for a host-issued run grant.
    #[command(hide = true)]
    Mcp {
        #[arg(long)]
        grant: PathBuf,
    },
    /// Run queued tasks in a separate process, with the owner's access; never reclaims active work.
    Work {
        #[arg(long)]
        once: bool,
    },
    /// Inspect tasks, request a stop, or reconcile an existing receipt.
    Tasks {
        #[command(subcommand)]
        command: TaskCommand,
    },
    /// Run configured channel adapters and answer owner messages until stopped.
    Serve,
    /// Inspect local readiness without sending a model request.
    Doctor,
    /// List connectors and check each script connector's access, with a plain fix.
    Connectors {
        #[command(subcommand)]
        command: Option<ConnectorCommand>,
    },
    /// Inspect the complete canonical conversation, including delivery states.
    History {
        #[arg(long, default_value = "console")]
        conversation: String,
        #[arg(long)]
        query: Option<String>,
        /// Only events after this sequence number.
        #[arg(long, default_value_t = 0)]
        after: i64,
    },
    /// Inspect queued or uncertain messages. Reading never resends.
    Outbox,
    /// List stored conversations, whose ids a schedule's conversation names.
    Conversations,
    /// List schedules with their next and last fired slots, or add, cancel or run one.
    Schedules {
        /// Include cancelled schedules.
        #[arg(long)]
        all: bool,
        #[command(subcommand)]
        command: Option<ScheduleCommand>,
    },
    /// Show each channel and whether serve runs it, or add WeChat, Slack or iMessage.
    Channels {
        #[command(subcommand)]
        command: Option<ChannelsCommand>,
    },
    /// Show the instance's settings, or change how far the assistant may act.
    Settings {
        #[command(subcommand)]
        command: Option<SettingsCommand>,
    },
    /// Start the knowledge wiki, or link one the owner already keeps.
    Knowledge {
        #[arg(long)]
        link: Option<PathBuf>,
    },
    /// Inspect and revise the private Markdown vault.
    Memory {
        #[command(subcommand)]
        command: MemoryCommand,
    },
    /// Run this instance as a macOS login service through launchd.
    Service {
        #[command(subcommand)]
        command: oppa::service::ServiceCommand,
    },
}

#[derive(Subcommand)]
enum ConnectorCommand {
    /// Check access as this process. With --prompt the operating system may ask
    /// for access, so run it at the Mac, as the process that will use it.
    Test {
        id: Option<String>,
        #[arg(long)]
        prompt: bool,
    },
}

#[derive(Subcommand)]
enum TaskCommand {
    List {
        #[arg(long)]
        conversation: Option<String>,
    },
    Read {
        id: String,
    },
    Stop {
        id: String,
    },
    /// Apply persisted evidence; a running task whose attempt lock is free is
    /// recorded as interrupted or stopped. Never starts or repeats a worker.
    Reconcile {
        id: String,
    },
    /// Queue a new attempt for an interrupted or failed task, only when its
    /// released attempt lock proves no worker process remains.
    Resume {
        id: String,
    },
    /// Present one pending result through the conversation model.
    Present {
        #[arg(long, default_value = "console")]
        conversation: String,
    },
}

#[derive(Subcommand)]
enum ScheduleCommand {
    /// Add a schedule: its prompt fires into the conversation at a local time,
    /// on one date or on repeating days (every day when neither is given).
    Add {
        /// A stored conversation id (see `oppa conversations`), or "console".
        #[arg(long)]
        conversation: String,
        /// Local time, HH:MM.
        #[arg(long)]
        at: String,
        /// A one-off's local date, YYYY-MM-DD.
        #[arg(long, conflicts_with = "days")]
        date: Option<String>,
        /// Weekdays such as mon,wed,fri.
        #[arg(long, value_delimiter = ',')]
        days: Vec<String>,
        #[arg(long)]
        prompt: String,
        /// Letters, digits, '-' or '_'; generated when omitted.
        #[arg(long)]
        id: Option<String>,
        /// After `--`, an argv run without a shell before the turn.
        #[arg(last = true)]
        command: Vec<String>,
    },
    /// Cancel a schedule so it never fires again.
    Cancel { id: String },
    /// Fire a console schedule for the current minute and print the reply.
    /// `oppa serve` runs channel schedules at their time.
    Run { id: String },
}

#[derive(Subcommand)]
enum ChannelsCommand {
    /// Add a channel and register the owner on it; a running serve starts it
    /// within seconds.
    Add {
        #[command(subcommand)]
        channel: AddChannel,
    },
    /// Make the iMessage handles held by add imessage the owner's, once the
    /// owner has sent the code it gave.
    Confirm,
}

#[derive(Subcommand)]
enum AddChannel {
    /// WeChat through its ClawBot plugin. Prints a link to show as a QR code;
    /// after the owner scans it, run this again with --finish.
    Wechat {
        /// Wait for the scan, then add the channel and the owner's handle.
        #[arg(long)]
        finish: bool,
        /// Seconds --finish waits for the scan.
        #[arg(long, default_value_t = 60)]
        wait: u64,
        /// Another iLink address, for a synthetic server.
        #[arg(long, hide = true)]
        base_url: Option<String>,
    },
    /// Slack through an app the owner creates from the link this prints.
    /// After each token the owner copies, run this again with --token.
    Slack {
        /// Keep the token on this Mac's clipboard; once all three are kept,
        /// add the channel and the owner's handle.
        #[arg(long)]
        token: bool,
        /// The app's name in Slack: the assistant's own.
        #[arg(long, default_value = "OPPA")]
        name: String,
        /// Another Web API address, for a synthetic server.
        #[arg(long, hide = true)]
        api_url: Option<String>,
        /// Another command that prints the clipboard, for a test.
        #[arg(long, hide = true)]
        clipboard: Option<String>,
    },
    /// iMessage from the assistant's own Apple ID, answering the owner's
    /// phone numbers (+15555550100) and email addresses once the owner
    /// confirms them with the code this prints.
    Imessage {
        #[arg(long, value_delimiter = ',', required = true)]
        handles: Vec<String>,
    },
}

#[derive(Subcommand)]
enum SettingsCommand {
    /// Set how far conversation turns and their tasks may act. Protected
    /// policy: only the owner runs this. It applies from the next turn.
    Access {
        #[arg(value_enum)]
        level: harness::Access,
    },
}

#[derive(Subcommand)]
enum MemoryCommand {
    Search {
        query: String,
    },
    Read {
        path: String,
    },
    Put {
        path: String,
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        expected_revision: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        source: String,
    },
    Delete {
        path: String,
        #[arg(long)]
        expected_revision: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        source: String,
    },
    Revert {
        commit: String,
        #[arg(long)]
        expected_revision: String,
        #[arg(long)]
        reason: String,
    },
    /// Copy a folder of Markdown notes into the vault as one managed commit.
    ///
    /// Symlinks are never followed. Hidden, linked, non-Markdown, non-UTF-8 and
    /// oversized files, and names with a colon, backslash or line break, are
    /// skipped and listed. If any target already exists or differs only in
    /// case from another path, the import is refused and nothing is written.
    Import {
        /// Folder to read. Nothing in it is changed.
        dir: PathBuf,
        /// Vault folder for the notes, such as imported/<source>. Required so an
        /// import never lands at the vault root.
        #[arg(long)]
        into: String,
        /// Where the notes came from, recorded in the commit.
        #[arg(long)]
        source: String,
        /// Why they are imported, recorded in the commit.
        #[arg(long)]
        reason: String,
        /// Print the plan and write nothing.
        #[arg(long)]
        dry_run: bool,
    },
    /// Take in what changed in the owner's Claude Code and Codex memory now,
    /// as the daily memory sync does: an owner turn at the console that brings
    /// what should last into OPPA memory. The first run takes in everything.
    Sync,
    /// Review the conversations since the last finished review now, as
    /// the nightly review does: an owner turn at the console that keeps what
    /// they taught as skills and memory.
    Review,
    /// Forget a text: without --confirm, list where it is with a digest; with
    /// that digest, remove it where it can be and list where it is still found.
    Forget {
        text: String,
        #[arg(long)]
        confirm: Option<String>,
    },
    Log,
    Revision,
    /// Reconcile an interrupted Git transaction without replaying its edits.
    Recover,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let home = match cli.home {
        Some(home) => home,
        None => std::env::var_os("HOME")
            .map(PathBuf::from)
            .context("HOME is unavailable; supply --home")?
            .join(".oppa"),
    };
    let paths = Paths::new(home, cli.private_dir)?;
    match cli.command {
        Commands::Init {
            agent,
            owner,
            access,
            model,
            effort,
            no_browser,
            vault,
            knowledge,
            web,
            imessage,
        } => initialize(
            &paths,
            agent,
            owner,
            Choices {
                access,
                model,
                effort,
                browser: (!no_browser).then_some(true),
                vault,
                knowledge,
            },
            web,
            imessage,
        )?,
        Commands::Mcp { grant } => {
            let broker = Broker::open(launch::load(&paths, &grant)?)?;
            mcp::serve(io::stdin().lock(), io::stdout().lock(), &broker)?;
        }
        Commands::Doctor => doctor(&paths)?,
        Commands::Connectors { command } => {
            let config = paths.load()?;
            harness::install_connector_adapters(&paths.runtime)?;
            let (only, prompt) = match command {
                Some(ConnectorCommand::Test { id, prompt }) => (id, prompt),
                None => (None, false),
            };
            let chosen: Vec<_> = config
                .connectors
                .iter()
                .filter(|c| only.as_ref().is_none_or(|id| &c.id == id))
                .collect();
            if only.is_some() && chosen.is_empty() {
                bail!("no connector with that id");
            }
            let reports: Vec<_> = chosen
                .into_iter()
                .map(|c| connector_report(c, prompt))
                .collect();
            println!("{}", serde_json::to_string_pretty(&reports)?);
            if reports.iter().any(|r| r["ready"] == false) {
                bail!("one or more connectors are not ready");
            }
        }
        Commands::Serve => {
            let shutdown = async {
                let mut terminate =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
                eprintln!("serve: stopping after in-flight turns");
            };
            oppa::channel::Server::new(paths).run(shutdown).await?;
        }
        Commands::Work { once } => {
            let worker = Worker::open(paths, std::env::current_exe()?)?;
            loop {
                let result = worker.run_once().await?;
                if !result.is_idle() || once {
                    println!("{}", serde_json::to_string_pretty(&result)?);
                }
                if once {
                    if matches!(result, WorkResult::Interrupted { .. })
                        || result
                            .task()
                            .is_some_and(|task| task.state == TaskState::Failed)
                    {
                        bail!("task did not complete; inspect its persisted evidence");
                    }
                    break;
                }
                if result.is_idle() {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        Commands::Tasks { command } => {
            let config = paths.load()?;
            let tasks = TaskStore::open(&paths, &config.owner)?;
            match command {
                TaskCommand::List { conversation } => println!(
                    "{}",
                    serde_json::to_string_pretty(&tasks.list(conversation.as_deref(), 100)?)?
                ),
                TaskCommand::Read { id } => println!(
                    "{}",
                    serde_json::to_string_pretty(
                        &serde_json::json!({"task":tasks.get(&id)?,"receipt":tasks.receipt(&id)?})
                    )?
                ),
                TaskCommand::Stop { id } => println!(
                    "{}",
                    serde_json::to_string_pretty(&tasks.request_stop(&id)?)?
                ),
                TaskCommand::Reconcile { id } => {
                    println!("{}", serde_json::to_string_pretty(&tasks.reconcile(&id)?)?)
                }
                TaskCommand::Resume { id } => {
                    tasks.reconcile(&id)?;
                    let evidence = tasks.observe_released_attempt(&id)?.context(
                        "the attempt lock is still held, so a worker process may be alive; nothing was resumed",
                    )?;
                    if !config.access.is_read() {
                        eprintln!("tasks run with {:?} access: the new attempt starts over and may repeat changes the earlier one made; read its board and evidence first", config.access);
                    }
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&tasks.resume(&id, evidence)?)?
                    )
                }
                TaskCommand::Present { conversation } => {
                    harness::install_bundled_adapters(&paths.runtime)?;
                    Runtime::open(paths)?
                        .present_pending(&conversation, deliver_console)
                        .await?;
                }
            }
        }
        Commands::Chat {
            conversation,
            message,
            attach,
        } => {
            harness::install_bundled_adapters(&paths.runtime)?;
            let runtime = Runtime::open(paths)?;
            if let Some(message) = message {
                chat_once(&runtime, &conversation, &message, &attach).await?;
            } else {
                chat_interactive(&runtime, &conversation).await?;
            }
        }
        Commands::History {
            conversation,
            query,
            after,
        } => {
            let runtime = Runtime::open(paths)?;
            if let Some(query) = query {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&runtime.store.search(
                        &conversation,
                        &query,
                        2
                    )?)?
                );
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&runtime::read_all_events(
                        &runtime.store,
                        &conversation,
                        after
                    )?)?
                );
            }
        }
        Commands::Outbox => println!(
            "{}",
            serde_json::to_string_pretty(&Runtime::open(paths)?.store.pending_outbox()?)?
        ),
        Commands::Conversations => println!(
            "{}",
            serde_json::to_string_pretty(&list_conversations(&Runtime::open(paths)?.store)?)?
        ),
        Commands::Schedules { all, command: None } => {
            let (schedules, store) = (
                ScheduleStore::open(&paths)?,
                Store::open(&paths.database())?,
            );
            let listed = schedules
                .list(all)?
                .iter()
                .map(|stored| schedule::report(&paths, &store, stored))
                .collect::<Result<Vec<_>>>()?;
            println!("{}", serde_json::to_string_pretty(&listed)?);
        }
        Commands::Schedules {
            command:
                Some(ScheduleCommand::Add {
                    conversation,
                    at,
                    date,
                    days,
                    prompt,
                    id,
                    command,
                }),
            ..
        } => {
            let schedule = Schedule {
                id: match id {
                    Some(id) => id,
                    None => schedule::new_id(None)?,
                },
                at,
                date,
                days: schedule::parse_days(&days)?,
                conversation,
                prompt,
                command,
                timeout_seconds: 300,
                window_minutes: 60,
                enabled: true,
                wake: false,
            };
            schedule::ensure_future(&schedule)?;
            let (stored, new) = ScheduleStore::open(&paths)?.add(&schedule, "owner", None)?;
            ensure!(new, "schedule {} already exists", schedule.id);
            let report = schedule::report(&paths, &Store::open(&paths.database())?, &stored)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::Schedules {
            command: Some(ScheduleCommand::Cancel { id }),
            ..
        } => {
            let cancelled = ScheduleStore::open(&paths)?.cancel(&id)?;
            let report = schedule::report(&paths, &Store::open(&paths.database())?, &cancelled)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::Channels { command } => {
            let report = match command {
                None => oppa::channels::report(&paths)?,
                Some(ChannelsCommand::Add { channel }) => add_channel(&paths, channel)?,
                Some(ChannelsCommand::Confirm) => confirm_channel(&paths)?,
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::Settings { command } => {
            let report = match command {
                None => settings(&paths, &paths.load()?),
                Some(SettingsCommand::Access { level }) => {
                    // Earlier uncommitted edits to the file would be committed
                    // under this change's name, so then nothing is committed.
                    let tracked = paths.private.join(".git").is_dir();
                    let clean = tracked
                        && git(&paths, &["diff", "--quiet", "HEAD", "--", "config.toml"]).is_ok();
                    let previous = config::set_access(&paths, level)?;
                    // `init` keeps the private configuration in Git; record the
                    // change there. The new access holds even if Git refuses.
                    let name = serde_json::to_value(level)?;
                    let message = format!("Set access to {}", name.as_str().unwrap_or("?"));
                    let committed = clean
                        && git(&paths, &["add", "--", "config.toml"])
                            .and_then(|()| {
                                git(
                                    &paths,
                                    &[
                                        "-c",
                                        "user.name=Local operator",
                                        "-c",
                                        "user.email=operator@example.test",
                                        "commit",
                                        "--allow-empty",
                                        "-m",
                                        &message,
                                    ],
                                )
                            })
                            .is_ok();
                    if tracked && !committed {
                        eprintln!("access changed; the private Git repository did not record it, since config.toml had other uncommitted edits or Git refused");
                    }
                    serde_json::json!({"access":level,"previous":previous,"committed":committed})
                }
            };
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Commands::Schedules {
            command: Some(ScheduleCommand::Run { id }),
            ..
        } => {
            let stored = ScheduleStore::open(&paths)?.get(&id)?;
            ensure!(
                stored.cancelled_at.is_none() && stored.schedule.enabled,
                "schedule {id} is cancelled or disabled; it never fires"
            );
            let schedule = &stored.schedule;
            if schedule.channel_destination().is_some() {
                bail!(
                    "schedule {id} is delivered in {}; oppa serve runs it at its time",
                    schedule.conversation
                );
            }
            harness::install_bundled_adapters(&paths.runtime)?;
            let runtime = Runtime::open(paths)?;
            let slot = chrono::Local::now()
                .with_second(0)
                .and_then(|now| now.with_nanosecond(0))
                .context("the local clock has no current minute")?;
            let turn = runtime
                .fire_schedule(schedule, &slot, &mut ConsoleDelivery(deliver_console))
                .await?;
            if turn.is_none() {
                eprintln!(
                    "schedule {id} already fired for {}; nothing ran",
                    slot.to_rfc3339()
                );
            }
        }
        Commands::Service { command } => {
            let report = oppa::service::run(paths, command)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if report.get("ready") == Some(&serde_json::Value::Bool(false)) {
                match report.get("problem").and_then(|problem| problem.as_str()) {
                    Some(problem) => bail!("{problem}"),
                    None => bail!("not ready as the service"),
                }
            }
        }
        Commands::Knowledge { link } => {
            let config = paths.load()?;
            ensure!(
                config.access == harness::Access::Computer,
                "the knowledge wiki is read and written with files, which needs full access; run oppa settings access full"
            );
            if let Some(link) = &link {
                oppa::knowledge::resolve(&paths, link)?;
            }
            // Ignored first, so a wiki that fails halfway never lands in the
            // instance's own repository.
            let ignore = paths.private.join(".gitignore");
            let ignored = fs::read_to_string(&ignore).unwrap_or_default();
            if !ignored.lines().any(|line| line == "/knowledge") {
                fs::write(&ignore, format!("{ignored}/knowledge\n"))?;
                git(
                    &paths,
                    &[
                        "-c",
                        "user.name=Local operator",
                        "-c",
                        "user.email=operator@example.test",
                        "commit",
                        "-m",
                        "Keep the knowledge wiki out of the instance configuration",
                        "--",
                        ".gitignore",
                    ],
                )?;
            }
            oppa::knowledge::prepare(&paths, link.as_deref())?;
            println!("Knowledge wiki: {}", paths.knowledge().display());
        }
        Commands::Memory {
            command: command @ (MemoryCommand::Sync | MemoryCommand::Review),
        } => {
            let config = paths.load()?;
            ensure!(
                config.access == harness::Access::Computer,
                "the memory sync and review need full access: they open files, mark progress and write skills with a shell; run oppa settings access full"
            );
            let sync = config
                .memory_sync
                .clone()
                .unwrap_or(oppa::memory_sync::MemorySync {
                    at: "00:00".into(),
                    sources: oppa::memory_sync::default_sources(),
                    conversation: None,
                });
            harness::install_bundled_adapters(&paths.runtime)?;
            let schedule = match command {
                MemoryCommand::Review => {
                    let latest = oppa::store::Store::open(&paths.database())?.last_seq()?;
                    let Some(review) = sync.review(&paths, latest, "console", chrono::Local::now())
                    else {
                        println!("Nothing new since the last review.");
                        return Ok(());
                    };
                    review
                }
                _ => sync.schedule(&paths, "console"),
            };
            let runtime = Runtime::open(paths)?;
            let slot = chrono::Local::now()
                .with_second(0)
                .and_then(|now| now.with_nanosecond(0))
                .context("the local clock has no current minute")?;
            if runtime
                .fire_schedule(&schedule, &slot, &mut ConsoleDelivery(deliver_console))
                .await?
                .is_none()
            {
                eprintln!("{} already ran this minute; nothing ran", schedule.id);
            }
        }
        Commands::Memory { command } => {
            let config = paths.load()?;
            let vault = Vault::open(&paths.vault())?;
            let provenance = |reason, source| Provenance {
                author: config.owner.clone(),
                source,
                reason,
                run_id: "owner-console".into(),
            };
            match command {
                MemoryCommand::Search { query } => {
                    println!("{}", serde_json::to_string_pretty(&vault.search(&query)?)?)
                }
                MemoryCommand::Read { path } => print!("{}", vault.read(&path)?),
                MemoryCommand::Revision => println!("{}", vault.revision()?),
                MemoryCommand::Recover => {
                    println!("{}", serde_json::to_string_pretty(&vault.recover()?)?)
                }
                MemoryCommand::Log => {
                    println!("{}", serde_json::to_string_pretty(&vault.history(30)?)?)
                }
                MemoryCommand::Put {
                    path,
                    file,
                    expected_revision,
                    reason,
                    source,
                } => {
                    let receipt = vault.commit(
                        &expected_revision,
                        &[Edit::Write {
                            path,
                            content: fs::read_to_string(file)?,
                        }],
                        &provenance(reason, source),
                    )?;
                    println!("{}", serde_json::to_string_pretty(&receipt)?);
                }
                MemoryCommand::Delete {
                    path,
                    expected_revision,
                    reason,
                    source,
                } => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&vault.commit(
                            &expected_revision,
                            &[Edit::Delete { path }],
                            &provenance(reason, source)
                        )?)?
                    );
                }
                MemoryCommand::Revert {
                    commit,
                    expected_revision,
                    reason,
                } => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&vault.revert(
                            &expected_revision,
                            &commit,
                            &provenance(reason, "owner-console".into())
                        )?)?
                    );
                }
                MemoryCommand::Forget { text, confirm } => {
                    let report = match confirm {
                        Some(digest) => {
                            oppa::forget::forget(&paths, &text, &digest, &[], || Ok(()))?
                        }
                        None => oppa::forget::preview(&paths, &text, &[])?,
                    };
                    println!("{}", serde_json::to_string_pretty(&report)?);
                }
                MemoryCommand::Import {
                    dir,
                    into,
                    source,
                    reason,
                    dry_run,
                } => oppa::vault::import::run(
                    &vault,
                    &dir,
                    &into,
                    &provenance(reason, source),
                    dry_run,
                )?,
                MemoryCommand::Sync | MemoryCommand::Review => unreachable!("handled above"),
            }
        }
    }
    Ok(())
}

/// What the owner chose for a new instance, by flag or at the terminal.
struct Choices {
    access: Option<harness::Access>,
    model: Option<String>,
    effort: String,
    /// `None` when the owner passed --no-browser.
    browser: Option<bool>,
    vault: Option<String>,
    knowledge: Option<PathBuf>,
}

/// Each stored conversation and when its newest event was recorded.
fn list_conversations(store: &Store) -> Result<Vec<serde_json::Value>> {
    let conversations = store.conversations()?;
    conversations
        .into_iter()
        .map(|conversation| {
            let newest = store.recent_transcript(&conversation.id, 1)?.pop();
            Ok(
                serde_json::json!({"id":conversation.id,"channel":conversation.channel,
                "account":conversation.account,"kind":conversation.kind,"title":conversation.title,
                "last_event_at":newest.map(|event| event.created_at)}),
            )
        })
        .collect()
}

/// What `oppa settings` shows, and the dashboard reads: no credential or
/// command line, only what the owner chose.
fn settings(paths: &Paths, config: &Config) -> serde_json::Value {
    let knowledge = paths.knowledge();
    serde_json::json!({"owner":config.owner,"access":config.access,
        "agents":config.agents.iter().map(|a| serde_json::json!({"id":a.id,"family":a.family,"model":a.model,"effort":a.effort})).collect::<Vec<_>>(),
        "channels":config.channels.iter().map(|c| serde_json::json!({"id":c.id,"account":c.account})).collect::<Vec<_>>(),
        "connectors":config.connectors.iter().map(|c| serde_json::json!({"id":c.id,"kind":c.kind})).collect::<Vec<_>>(),
        "browser":config.browser,
        "memory_sync":config.memory_sync.as_ref().map(|sync| &sync.at),
        "knowledge":knowledge.is_dir().then(|| knowledge.canonicalize().unwrap_or(knowledge)),
        "skills":oppa::prompt::skill_index(&paths.skills()),
        "source":config.source,
        "notes":{"persona":"assistant/persona.md","rules":"assistant/rules.md"}})
}

async fn chat_once(
    runtime: &Runtime,
    conversation: &str,
    message: &str,
    attach: &[PathBuf],
) -> Result<()> {
    let attachments = attach
        .iter()
        .map(|path| {
            let path = path
                .canonicalize()
                .with_context(|| format!("cannot attach {}", path.display()))?;
            Ok(oppa::store::Attachment {
                name: path.file_name().map(|n| n.to_string_lossy().into_owned()),
                path: path.to_string_lossy().into_owned(),
                mime_type: None,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let native_id = format!(
        "console-{}-{}",
        std::process::id(),
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .context("clock out of range")?
    );
    runtime
        .turn_with(
            conversation,
            &native_id,
            message,
            attachments,
            deliver_console,
        )
        .await?;
    Ok(())
}

fn deliver_console(answer: &str) -> Result<()> {
    let mut out = io::stdout().lock();
    out.write_all(answer.as_bytes())?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}

async fn chat_interactive(runtime: &Runtime, conversation: &str) -> Result<()> {
    // A dedicated blocking reader allows pending task results to be presented
    // while the console is waiting for input. EOF closes this console only;
    // the separate worker and its durable task remain independent.
    let interactive = io::stdin().is_terminal();
    let (sender, receiver) = std::sync::mpsc::sync_channel(8);
    std::thread::spawn(move || loop {
        let mut line = String::new();
        match io::stdin().read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if sender.send(Ok(line)).is_err() {
                    break;
                }
            }
            Err(error) => {
                let _ = sender.send(Err(error));
                break;
            }
        }
    });
    let mut prompt_due = true;
    loop {
        if prompt_due && interactive {
            print!("You: ");
            io::stdout().flush()?;
        }
        prompt_due = false;
        match receiver.try_recv() {
            Ok(Ok(line)) => {
                if !line.trim().is_empty() {
                    chat_once(runtime, conversation, line.trim_end(), &[]).await?;
                }
                prompt_due = true;
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if runtime
                    .present_pending(conversation, deliver_console)
                    .await?
                    .is_some()
                {
                    prompt_due = true;
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
    Ok(())
}

fn initialize(
    paths: &Paths,
    agent: Option<String>,
    owner: String,
    choices: Choices,
    mut web: Option<u16>,
    mut imessage: Vec<String>,
) -> Result<()> {
    if paths.runtime.exists() || paths.private.exists() {
        bail!("initialization requires new runtime and private directories; existing data was preserved");
    }
    let mut manifests: Vec<AgentManifest> = [
        include_str!("../resources/agents/codex.toml"),
        include_str!("../resources/agents/claude.toml"),
    ]
    .into_iter()
    .map(toml::from_str)
    .collect::<std::result::Result<_, _>>()?;
    let selected = match agent {
        Some(agent) => agent,
        None if io::stdin().is_terminal() => {
            match ask("Which signed-in harness should answer? [codex/claude, default codex]: ")? {
                input if input.is_empty() => "codex".into(),
                input => input,
            }
        }
        None => "codex".into(),
    };
    manifests.retain(|a| a.id == selected);
    if manifests.is_empty() {
        bail!("unknown bundled harness; choose codex or claude");
    }
    for manifest in &mut manifests {
        manifest.model = choices.model.clone();
        manifest.effort = Some(choices.effort.clone());
    }
    let terminal = io::stdin().is_terminal();
    let access = match choices.access {
        Some(access) => access,
        None if terminal => match ask("How much may it do? full: every tool and file on this Mac; connectors: \
            the accounts and MCP servers in your harness, the web and your browser, no built-in shell; read: read and answer \
            [full/connectors/read, default full]: ")?
        .to_lowercase()
        .as_str()
        {
            "" | "full" | "computer" => harness::Access::Computer,
            "connectors" => harness::Access::Connectors,
            "read" => harness::Access::Read,
            other => bail!("unknown access {other}; choose full, connectors or read"),
        },
        None => harness::Access::Connectors,
    };
    // A browser the owner turned off with --no-browser stays off; otherwise a
    // person at a terminal may turn it off.
    let browser = choices.browser.is_some()
        && !(terminal
            && ask("Let it use your browser (Claude in Chrome, Codex browser use)? [Y/n]: ")?
                .to_lowercase()
                .starts_with('n'));
    // Where to talk is asked only of a person at a terminal; a script that
    // names neither gets the console alone, as before.
    if web.is_none() && imessage.is_empty() && io::stdin().is_terminal() {
        if !ask("Talk in web chat on this Mac? [Y/n]: ")?
            .to_lowercase()
            .starts_with('n')
        {
            web = Some(8765);
        }
        imessage = ask("On iMessage too? It needs the assistant's own Apple ID signed in to Messages here. \
            Your phone numbers (+15555550100) and email addresses, comma-separated, or blank for no: ")?
            .split(',')
            .map(str::trim)
            .filter(|handle| !handle.is_empty())
            .map(String::from)
            .collect();
    }
    let vault = match choices.vault {
        Some(vault) => Some(vault),
        None if terminal => Some(ask(
            "A 1Password vault it may read for keys and passwords, with its service account token \
            in the Keychain under oppa-1password (blank for none): ",
        )?)
        .filter(|vault| !vault.is_empty()),
        None => None,
    };
    // The wiki is read and written with files, so only full access gets one.
    // A path is checked now, before anything is written.
    let knowledge = match choices.knowledge {
        _ if access != harness::Access::Computer => None,
        Some(existing) => Some(Some(oppa::knowledge::resolve(paths, &existing)?)),
        None if terminal => loop {
            let answer = ask(
                "Do you already keep a knowledge wiki, a Git repository of Markdown notes on what you \
                read and research, with an AGENTS.md? Its path to use it, or blank to start one: ",
            )?;
            if answer.is_empty() {
                break Some(None);
            }
            match oppa::knowledge::resolve(paths, Path::new(&answer)) {
                Ok(existing) => break Some(Some(existing)),
                Err(error) => println!("{error:#}"),
            }
        },
        None => Some(None),
    };
    let (channels, people) = talk(paths, &owner, web, &imessage)?;
    let connectors = vault
        .iter()
        .map(|vault| oppa::config::ConnectorManifest {
            id: "secrets".into(),
            kind: oppa::config::ConnectorKind::Script,
            command: vec![
                "python3".into(),
                paths
                    .runtime
                    .join("adapters/onepassword.py")
                    .display()
                    .to_string(),
                "--vault".into(),
                vault.clone(),
            ],
            timeout_seconds: 60,
            owner_only: true,
        })
        .collect();
    let mut config = Config {
        version: 1,
        connectors,
        sessions: oppa::config::SessionPolicy {
            gather_seconds: 3,
            ..Default::default()
        },
        owner,
        access,
        source: None,
        agents: manifests,
        channels,
        people,
        groups: vec![],
        heartbeat: None,
        browser,
        // The sync needs a shell, so only full access gets it.
        memory_sync: (access == harness::Access::Computer).then(|| oppa::memory_sync::MemorySync {
            at: "03:30".into(),
            sources: oppa::memory_sync::default_sources(),
            conversation: None,
        }),
    };
    config.validate()?;
    paths.prepare()?;
    harness::install_bundled_adapters(&paths.runtime)?;
    for manifest in &mut config.agents {
        for arg in &mut manifest.command {
            if arg.starts_with("resources/adapters/") {
                *arg = paths
                    .runtime
                    .join("adapters")
                    .join(
                        PathBuf::from(&arg)
                            .file_name()
                            .context("invalid adapter path")?,
                    )
                    .to_string_lossy()
                    .into_owned();
            }
        }
    }
    fs::write(paths.config_file(), toml::to_string_pretty(&config)?)?;
    fs::write(
        paths.private.join(".gitignore"),
        "/vault/\n/knowledge\n.env*\n*.key\n*.pem\n",
    )?;
    fs::create_dir(paths.private.join("evals"))?;
    git(paths, &["init", "--template=", "-b", "main"])?;
    git(paths, &["add", "--", "config.toml", ".gitignore"])?;
    git(
        paths,
        &[
            "-c",
            "user.name=Local operator",
            "-c",
            "user.email=operator@example.test",
            "commit",
            "-m",
            "Initialize private instance configuration",
        ],
    )?;
    Vault::init(&paths.vault())?;
    if let Some(existing) = &knowledge {
        oppa::knowledge::prepare(paths, existing.as_deref())?;
    }
    Store::open(&paths.database())?;
    println!("Instance created. Run oppa doctor, then oppa chat. Personal records have not been imported.");
    if selected == "codex" {
        println!("Using Codex: run codex login status to check your sign-in. If needed, run codex login and choose Sign in with ChatGPT for subscription access; no API key is needed.");
    }
    println!("Connect accounts in your chosen harness: Codex apps/plugins or codex mcp add; Claude Settings, Connectors or claude mcp add. An actual chat verifies each connection.");
    match &knowledge {
        Some(Some(existing)) => println!("Your knowledge wiki at {} is linked as {}; the assistant follows its AGENTS.md.", existing.display(), paths.knowledge().display()),
        Some(None) => println!("A new knowledge wiki is at {}: ask the assistant to digest an article or a filing and it is kept there with its sources.", paths.knowledge().display()),
        None => {}
    }
    if config.memory_sync.is_some() {
        println!("The assistant offers in chat to take in what Claude Code and Codex already remember about you; after you say yes, it does so every day at 03:30.");
    }
    if web.is_some() {
        println!(
            "Web chat starts with oppa serve; open the link it writes to {} in a browser on this Mac.",
            paths.runtime.join("channels/web/login-url").display()
        );
    }
    if !imessage.is_empty() {
        println!(
            "iMessage needs the imsg bridge (0.15 or later) at {} before oppa serve starts; serve stops if it cannot start a channel.",
            paths.runtime.join("channels/imsg-bridge/imsg").display()
        );
    }
    Ok(())
}

fn ask(question: &str) -> Result<String> {
    print!("{question}");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_owned())
}

/// The channels the owner chose at init, with the owner registered on each.
fn talk(
    paths: &Paths,
    owner: &str,
    web: Option<u16>,
    imessage: &[String],
) -> Result<(Vec<config::ChannelManifest>, Vec<config::Person>)> {
    let (mut channels, mut handles) = (vec![], vec![]);
    if !imessage.is_empty() {
        for handle in imessage {
            handles.push(oppa::channels::imessage_handle(handle)?);
        }
        channels.push(oppa::channels::imessage(paths));
    }
    if let Some(port) = web {
        handles.push("web:owner".into());
        channels.push(oppa::channels::web(paths, port));
    }
    let owner = config::Person {
        id: owner.into(),
        role: config::Role::Owner,
        handles,
    };
    let people = (!owner.handles.is_empty()).then_some(owner);
    Ok((channels, people.into_iter().collect()))
}

/// `oppa channels add`: what was added and what the owner does next. Each
/// step's text is for the assistant to put in its own words.
fn add_channel(paths: &Paths, channel: AddChannel) -> Result<serde_json::Value> {
    use serde_json::json;
    Ok(match channel {
        AddChannel::Wechat {
            finish: false,
            base_url,
            ..
        } => {
            let started = oppa::channels::wechat_login(
                paths,
                oppa::channels::Login::Start,
                base_url.as_deref(),
            )?;
            json!({"status": "scan", "link": started["link"],
                "next": "In WeChat (iOS 8.0.70 or later, Android 8.0.69), turn on ClawBot under Me, Settings, Plugins. \
                    Then open the link on this Mac and scan the QR code it shows with WeChat, or open the link inside WeChat \
                    on the phone, and confirm on the phone within five minutes. Then run oppa channels add wechat --finish."})
        }
        AddChannel::Wechat {
            finish: true,
            wait,
            base_url,
        } => {
            let finished = oppa::channels::wechat_login(
                paths,
                oppa::channels::Login::Finish(wait),
                base_url.as_deref(),
            )?;
            match finished["status"].as_str() {
                Some("bound") => {
                    let handle = finished["handle"].as_str().unwrap_or_default().to_owned();
                    let manifest = oppa::channels::wechat(paths, base_url.as_deref());
                    let added = oppa::channels::add(paths, &manifest, &[handle.clone()], true)?;
                    oppa::channels::wechat_login(
                        paths,
                        oppa::channels::Login::Activate,
                        base_url.as_deref(),
                    )?;
                    json!({"status": "added", "handle": handle,
                        "channel": if added { "added" } else { "kept as configured" },
                        "next": "Serve starts WeChat within seconds (check with oppa channels). Then send the bot a first \
                            message in its ClawBot chat in WeChat: it can answer only after a message from you there."})
                }
                Some("scanned") => json!({"status": "scanned",
                    "next": "Scanned but not yet confirmed: confirm on the phone, then run oppa channels add wechat --finish again."}),
                Some("waiting") => json!({"status": "waiting",
                    "next": "Not scanned yet. Once it is, run oppa channels add wechat --finish again."}),
                Some("expired") => json!({"status": "expired",
                    "next": "The QR code expired. Run oppa channels add wechat for a new one."}),
                _ => json!({"status": "no_login",
                    "next": "No QR code is waiting. Run oppa channels add wechat first."}),
            }
        }
        AddChannel::Slack {
            token: false,
            name,
            api_url,
            ..
        } => {
            let made = oppa::channels::slack_login(
                paths,
                oppa::channels::SlackLogin::Link(&name),
                api_url.as_deref(),
            )?;
            json!({"status": "create", "link": made["link"],
                "next": "1. Open the link on this Mac, signed in to Slack in the browser, pick the workspace and Create. \
                    2. Under Install App, Install to the workspace and Allow. \
                    3. On the same page copy the Bot User OAuth Token, then run oppa channels add slack --token. \
                    4. Copy the User OAuth Token, then run it again. \
                    5. Under Basic Information, App-Level Tokens, Generate Token and Scopes: any name, add the scope \
                    connections:write, Generate, copy the token, then run it a third time. \
                    Each run reads the token from the clipboard, so it never goes into a chat."})
        }
        AddChannel::Slack {
            token: true,
            api_url,
            clipboard,
            ..
        } => {
            let kept = oppa::channels::slack_login(
                paths,
                oppa::channels::SlackLogin::Token(clipboard.as_deref()),
                api_url.as_deref(),
            )?;
            let still = |kept: &serde_json::Value| {
                format!(
                    "Still needed: {}. Copy the next one, then run oppa channels add slack --token again.",
                    kept["missing"]
                        .as_array()
                        .map(|m| m.iter().filter_map(|k| k.as_str()).map(|k| match k {
                            "bot" => "the Bot User OAuth Token (xoxb-) under Install App",
                            "user" => "the User OAuth Token (xoxp-) under Install App",
                            _ => "an app-level token (xapp-) with connections:write under Basic Information",
                        }).collect::<Vec<_>>().join("; "))
                        .unwrap_or_default()
                )
            };
            match kept["status"].as_str() {
                Some("ready") => {
                    let handle = kept["handle"].as_str().unwrap_or_default().to_owned();
                    let manifest = oppa::channels::slack(paths, api_url.as_deref());
                    let added = oppa::channels::add(paths, &manifest, &[handle.clone()], true)?;
                    oppa::channels::slack_login(
                        paths,
                        oppa::channels::SlackLogin::Activate,
                        api_url.as_deref(),
                    )?;
                    json!({"status": "added", "handle": handle, "team": kept["team"],
                        "channel": if added { "added" } else { "kept as configured" },
                        "next": "Serve starts Slack within seconds (check with oppa channels). In Slack, open the app under \
                            Apps and send it a message: that direct chat is the owner's. To share a room with other \
                            agents, type /invite and the app's name in that channel; the assistant reads everything said \
                            there and answers the owner's own messages."})
                }
                Some("kept") => {
                    json!({"status": "kept", "kind": kept["kind"], "next": still(&kept)})
                }
                Some("invalid") => {
                    json!({"status": "invalid", "kind": kept["kind"], "error": kept["error"],
                    "next": format!("Slack refused that token. Copy it again, whole, then run oppa channels add slack --token again. {}", still(&kept))})
                }
                Some("mismatch") => json!({"status": "mismatch",
                    "next": "The bot and user tokens are from different Slack workspaces. Copy both from the same app's Install App page and run add slack --token for each."}),
                _ => json!({"status": "no_token",
                    "next": format!("The clipboard holds no Slack token. {}", still(&kept))}),
            }
        }
        AddChannel::Imessage { handles } => {
            let handles = handles
                .iter()
                .map(|h| oppa::channels::imessage_handle(h))
                .collect::<Result<Vec<_>>>()?;
            let config = paths.load()?;
            for handle in &handles {
                let bare = handle.strip_prefix("imessage:").unwrap_or(handle);
                if let Some(person) = config.person("imessage", bare) {
                    anyhow::ensure!(
                        person.id == config.owner,
                        "{bare} is already {}'s",
                        person.id
                    );
                }
            }
            let added = oppa::channels::add(paths, &oppa::channels::imessage(paths), &[], false)?;
            let held = oppa::channels::hold(paths, handles)?;
            json!({"status": "confirm", "handles": held.handles, "code": held.code,
                "channel": if added { "added" } else { "kept as configured" },
                "next": format!("These count as the owner only after the owner confirms them. In the owner's own direct chat, \
                    tell them that whoever writes from these handles gets everything the assistant does for them, and that if one \
                    belongs to someone else, such as a family member, they should not confirm it. If all are theirs, they reply \
                    with the code {} in this chat; then run oppa channels confirm. Only the owner may send it: never send it \
                    yourself or in their place, by oppa chat or any other way. The code lasts an hour.", held.code)})
        }
    })
}

fn confirm_channel(paths: &Paths) -> Result<serde_json::Value> {
    use oppa::channels::Confirmed;
    use serde_json::json;
    Ok(match oppa::channels::confirm(paths)? {
        Confirmed::None => {
            json!({"status": "none", "next": "No handles are waiting. Run oppa channels add imessage first."})
        }
        Confirmed::Expired(held) => json!({"status": "expired", "handles": held.handles,
            "next": "The code is over an hour old. Run oppa channels add imessage again for a new one."}),
        Confirmed::Waiting(held) => json!({"status": "waiting", "handles": held.handles,
            "next": "No message from the owner, written as themselves after the code was made, carries it yet. Only the \
                owner may send it: never send or repeat it in their place."}),
        Confirmed::Added(held) => {
            let imsg = paths.runtime.join("channels/imsg-bridge/imsg");
            let launcher = paths.runtime.join("bin/oppa-launch");
            json!({"status": "added", "handles": held.handles, "imsg_bridge": imsg, "imsg_present": imsg.exists(),
                "launcher": launcher, "launcher_present": launcher.exists(),
                "next": "Serve starts iMessage once three things hold: Messages on this Mac is signed in to the assistant's \
                    own Apple ID, imsg 0.15 or later (github.com/openclaw/imsg) is unpacked at imsg_bridge with its helper \
                    files, and the service's launcher has Full Disk Access in System Settings, Privacy & Security. It tries \
                    again every 30 seconds; check with oppa channels."})
        }
    })
}

fn git(paths: &Paths, args: &[&str]) -> Result<()> {
    let mut command = Command::new("git");
    for (key, _) in std::env::vars_os() {
        if key.to_string_lossy().starts_with("GIT_") {
            command.env_remove(key);
        }
    }
    command
        .args([
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
        ])
        .arg("-C")
        .arg(&paths.private)
        .args(args);
    let output = oppa::process::output(&mut command)?;
    if !output.status.success() {
        bail!("private configuration Git operation failed");
    }
    Ok(())
}

/// Health as this process sees it. The service may hold different operating
/// system permissions than a terminal, so the report says whose view it is.
fn connector_report(connector: &ConnectorManifest, prompt: bool) -> serde_json::Value {
    let mut report = match connectors::health(connector, prompt) {
        Ok(health) => health,
        Err(error) => serde_json::json!({"ready":false,"problem":format!("{error:#}")}),
    };
    report["id"] = connector.id.clone().into();
    report["kind"] = "script".into();
    report["as"] = "this process; the service may hold different permissions".into();
    if report["ready"] == true {
        match connectors::describe(connector) {
            Ok(operations) => {
                let (reads, writes): (Vec<_>, Vec<_>) =
                    operations.into_iter().partition(|o| o.tier == "read");
                report["read_operations"] = reads.into_iter().map(|o| o.name).collect();
                report["write_operations"] = writes.into_iter().map(|o| o.name).collect();
            }
            Err(error) => {
                report["ready"] = false.into();
                report["problem"] = format!("{error:#}").into();
            }
        }
    }
    report
}

fn executable_on_path(program: &str) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let executable = |path: &Path| {
        fs::metadata(path)
            .map(|meta| meta.is_file() && meta.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    };
    if Path::new(program).is_absolute() {
        return executable(Path::new(program));
    }
    std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).any(|dir| executable(&dir.join(program))))
        .unwrap_or(false)
}

fn doctor(paths: &Paths) -> Result<()> {
    let config = paths.load()?;
    let mut checks = vec![serde_json::json!({"check":"configuration","ready":true})];
    checks.push(serde_json::json!({"check":"canonical_state","ready":paths.database().is_file() && Store::open(&paths.database()).is_ok()}));
    checks.push(serde_json::json!({"check":"vault","ready":Vault::open(&paths.vault()).and_then(|v|v.check_ready()).is_ok()}));
    for agent in &config.agents {
        let program = agent.command.first().context("missing agent command")?;
        let exists = executable_on_path(program);
        checks.push(serde_json::json!({"check":"harness_launcher","agent":agent.id,"ready":exists,"authentication":"not probed; an actual chat verifies provider access"}));
        // Only bundled adapters imply a native CLI. Custom adapters may use
        // the same agent id with a different launcher.
        for cli in ["codex", "claude"] {
            let adapter = paths.runtime.join("adapters").join(format!("{cli}.py"));
            if agent.command.get(1).map(Path::new) == Some(adapter.as_path()) {
                checks.push(serde_json::json!({"check":"harness_cli","agent":agent.id,
                    "program":cli,"ready":executable_on_path(cli),
                    "authentication":"not probed; an actual chat verifies subscription access",
                    "repair":if cli == "codex" {
                        "Install Codex CLI on PATH, run codex login and choose Sign in with ChatGPT, then run oppa chat --message hello. No API key is needed."
                    } else {
                        "Install Claude Code on PATH, sign in with your subscription, then run oppa chat --message hello."
                    }}));
            }
        }
    }
    for connector in config
        .connectors
        .iter()
        .filter(|c| c.kind == ConnectorKind::Script)
    {
        let mut report = connector_report(connector, false);
        report["check"] = "connector".into();
        checks.push(report);
    }
    // A running service installed the adapters of the build it started from;
    // drift here means it runs a different build than this command.
    let stale = harness::stale_bundled_adapters(&paths.runtime);
    checks.push(serde_json::json!({"check":"bundled_adapters","ready":stale.is_empty(),"stale":stale,
        "repair":"restart oppa serve and oppa work from this build; they reinstall the bundled adapters"}));
    // Serve starts no heartbeat in a chat this refuses, and says so only in
    // its log. This reads config.toml as it is now; serve reads [heartbeat]
    // when it starts. A missing database is reported, not created.
    // The default heartbeat waits, ready, for the owner's first direct message.
    if let Some(heartbeat) = oppa::heartbeat::Heartbeat::of(&config) {
        let chat = if paths.database().is_file() {
            Store::open(&paths.database()).and_then(|store| {
                let running = oppa::heartbeat::Heartbeat::serving(&config, &store, paths)?;
                let chat = running.map(|running| {
                    oppa::heartbeat::owners_chat(&store, &running.conversation, &config.owner)
                        .map(|_| (running.conversation, running.every_minutes))
                });
                chat.transpose()
            })
        } else if heartbeat.conversation.is_empty() {
            Ok(None)
        } else {
            Err(anyhow::anyhow!("there is no canonical state yet"))
        };
        checks.push(match chat {
            Ok(Some((chat, every))) => serde_json::json!({"check":"heartbeat","conversation":chat,"every_minutes":every,"ready":true}),
            Ok(None) => serde_json::json!({"check":"heartbeat","conversation":null,"ready":true,
                "waiting":"it starts in the owner's direct chat with the assistant once the owner writes there"}),
            Err(error) => serde_json::json!({"check":"heartbeat","conversation":heartbeat.conversation,"ready":false,
                "problem":format!("{error:#}"),
                "repair":"name the owner's direct chat from `oppa conversations` once the owner has written there, then restart serve"}),
        });
    }
    let left = oppa::runtime::undeleted_forgetting_sessions(paths)?;
    checks.push(serde_json::json!({"check":"forgotten_sessions","ready":left.is_empty(),"sessions":left,
        "repair":"a run that forgot something could not delete these native sessions, which still hold the text; turns in their conversation fail until OPPA can delete them, so make their files deletable"}));
    println!("{}", serde_json::to_string_pretty(&checks)?);
    if checks
        .iter()
        .any(|c| c.get("ready") == Some(&serde_json::Value::Bool(false)))
    {
        bail!("one or more readiness checks failed");
    }
    Ok(())
}
