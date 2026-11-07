//! Private, short-lived launch context for owner-local broker processes.
use crate::{
    broker::{validate_input, BrokerLaunch, ConversationScope, RunGrant, TaskAccess},
    config::{private_directory, reject_directory_links, Paths, Role},
    harness::{Access, McpServer},
    runtime::atomic_json,
    store::Store,
};
use anyhow::{ensure, Context, Result};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

/// Host-selected authority, never parsed from model output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunCapabilities {
    Conversation,
    Worker,
    Presentation,
    /// The owner's heartbeat: it reads and may speak in its conversation, and
    /// changes nothing.
    Heartbeat,
}

pub struct BrokerRun {
    pub launch: BrokerLaunch,
    pub server: McpServer,
    grant_file: PathBuf,
}

impl BrokerRun {
    pub fn create(
        paths: &Paths,
        requester: &str,
        input_seq: i64,
        conversations: Vec<String>,
        timeout_seconds: u64,
        executable: &Path,
    ) -> Result<Self> {
        Self::create_with_capabilities(
            paths,
            requester,
            input_seq,
            conversations,
            timeout_seconds,
            executable,
            RunCapabilities::Conversation,
        )
    }

    pub fn create_with_capabilities(
        paths: &Paths,
        requester: &str,
        input_seq: i64,
        conversations: Vec<String>,
        timeout_seconds: u64,
        executable: &Path,
        capabilities: RunCapabilities,
    ) -> Result<Self> {
        let config = paths.load()?;
        let role = config
            .turn_role(requester)
            .context("grant requester is not the owner, a registered adult or a child")?;
        let owner = role == Role::Owner;
        ensure!(
            owner || capabilities == RunCapabilities::Conversation,
            "only the owner's turns start tasks"
        );
        validate_input(&paths.database(), requester, input_seq, &conversations)?;
        let store = Store::open(&paths.database())?;
        // A heartbeat's input gets the heartbeat's grant and nothing else.
        ensure!(
            (store.event(input_seq)?.kind == "heartbeat")
                == (capabilities == RunCapabilities::Heartbeat),
            "a heartbeat runs only with its read-only grant"
        );
        // A family member's turn (the owner's, an adult's or a child's) and
        // the background work it starts get the owner's chosen reach: what a
        // family member may do is the instructions' to guard, never the
        // host's. A presentation only reports and
        // a heartbeat only looks, so they stay read-only.
        let read_only = matches!(
            capabilities,
            RunCapabilities::Presentation | RunCapabilities::Heartbeat
        );
        let access = if read_only {
            Access::Read
        } else {
            config.access
        };
        let task_access = match capabilities {
            RunCapabilities::Conversation => TaskAccess::Manage,
            RunCapabilities::Worker => TaskAccess::None,
            RunCapabilities::Presentation | RunCapabilities::Heartbeat => TaskAccess::Read,
        };
        // The owner's turns and the heartbeat's looks set wakes, while a
        // heartbeat runs with the owner's direct chat to look in.
        let wakes = owner
            && matches!(
                capabilities,
                RunCapabilities::Conversation | RunCapabilities::Heartbeat
            )
            && crate::heartbeat::Heartbeat::running(&config, &store)?.is_some_and(|heartbeat| {
                crate::heartbeat::owners_chat(&store, &heartbeat.conversation, &config.owner)
                    .is_ok()
            });
        // A family member's turn may discover the conversations of every
        // channel account of the instance, the owner's own accounts included,
        // so a topic continues on a second channel and a message or schedule
        // can go to another chat; a worker stays inside its originating
        // conversation.
        let conversation_scopes = if capabilities == RunCapabilities::Worker {
            vec![]
        } else {
            let origin = store.conversation(&store.event(input_seq)?.conversation)?;
            let mut scopes = vec![ConversationScope {
                owners_account: config.owners_account(&origin.channel),
                channel: origin.channel,
                account: origin.account,
            }];
            let console = ("console".to_string(), "local".to_string());
            let channels = config
                .channels
                .iter()
                .map(|c| (c.id.clone(), c.account.clone()));
            for (channel, account) in std::iter::once(console).chain(channels) {
                let scope = ConversationScope {
                    owners_account: config.owners_account(&channel),
                    channel,
                    account,
                };
                if !scopes.contains(&scope) {
                    scopes.push(scope);
                }
            }
            scopes
        };
        private_directory(&paths.runtime.join("grants"))?;
        let mut bytes = [0u8; 24];
        File::open("/dev/urandom")?.read_exact(&mut bytes)?;
        let context = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        let run_id = format!("{input_seq}-{context}");
        let grant_file = paths.runtime.join("grants").join(format!("{run_id}.json"));
        let journal = paths.runtime.join("runs").join(&run_id);
        private_directory(&journal)?;
        let launch = BrokerLaunch {
            grant: RunGrant {
                context,
                requester: requester.into(),
                run_id,
                input_seq,
                conversations,
                conversation_scopes,
                read_scopes: vec!["*".into()],
                write_scopes: if capabilities == RunCapabilities::Conversation {
                    vec!["*".into()]
                } else {
                    vec![]
                },
                task_access,
                eligible_agents: if owner && task_access == TaskAccess::Manage {
                    config.agents.into_iter().map(|agent| agent.id).collect()
                } else {
                    vec![]
                },
                expires_at: chrono::Utc::now().timestamp() + timeout_seconds as i64 + 30,
                access,
                role,
                wakes,
            },
            private_dir: paths.private.clone(),
            database: paths.database(),
            vault: paths.vault(),
            journal,
        };
        atomic_json(&grant_file, &launch)?;
        let server = McpServer {
            command: executable
                .to_str()
                .context("broker executable path must be UTF-8")?
                .into(),
            args: vec![
                "--home".into(),
                paths.runtime.to_string_lossy().into(),
                "--private-dir".into(),
                paths.private.to_string_lossy().into(),
                "mcp".into(),
                "--grant".into(),
                grant_file.to_string_lossy().into(),
            ],
            read_tools: vec![],
            write_tools: vec![],
        };
        Ok(Self {
            launch,
            server,
            grant_file,
        })
    }
}

impl Drop for BrokerRun {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.grant_file);
    }
}

/// This interface is for the authenticated local operator's child processes.
/// It does not authenticate a remote actor or provide child sandboxing.
pub fn load(paths: &Paths, file: &Path) -> Result<BrokerLaunch> {
    let metadata = fs::symlink_metadata(file)?;
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "grant must be a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        ensure!(
            metadata.uid() == unsafe { libc::geteuid() }
                && metadata.nlink() == 1
                && metadata.permissions().mode() & 0o077 == 0,
            "grant must be owned by the local operator and private"
        );
    }
    ensure!(metadata.len() <= 65536, "grant file exceeds size limit");
    reject_directory_links(&paths.runtime.join("grants"))?;
    reject_directory_links(&paths.runtime.join("runs"))?;
    let grants = paths.runtime.join("grants").canonicalize()?;
    let path = file.canonicalize()?;
    ensure!(
        path.parent() == Some(grants.as_path()),
        "grant is outside this instance"
    );
    let mut content = Vec::new();
    File::open(&path)?.take(65537).read_to_end(&mut content)?;
    ensure!(content.len() <= 65536, "grant file exceeds size limit");
    let launch: BrokerLaunch = serde_json::from_slice(&content)?;
    ensure!(
        launch.grant.context.len() == 48
            && launch.grant.context.bytes().all(|b| b.is_ascii_hexdigit())
            && launch.grant.run_id
                == format!("{}-{}", launch.grant.input_seq, launch.grant.context),
        "grant run identity does not match its host context"
    );
    ensure!(
        path.file_name().and_then(|name| name.to_str())
            == Some(format!("{}.json", launch.grant.run_id).as_str()),
        "grant filename does not match its run"
    );
    ensure!(
        launch.private_dir.canonicalize()? == paths.private.canonicalize()?
            && launch.database.canonicalize()? == paths.database().canonicalize()?
            && launch.vault.canonicalize()? == paths.vault().canonicalize()?,
        "grant storage does not match this instance"
    );
    reject_directory_links(&launch.journal)?;
    ensure!(
        launch.journal.canonicalize()?
            == paths
                .runtime
                .join("runs")
                .join(&launch.grant.run_id)
                .canonicalize()?,
        "grant journal is outside this instance"
    );
    crate::broker::admits(&paths.load()?, &launch.grant)?;
    ensure!(
        launch.grant.expires_at >= chrono::Utc::now().timestamp(),
        "grant expired"
    );
    validate_input(
        &launch.database,
        &launch.grant.requester,
        launch.grant.input_seq,
        &launch.grant.conversations,
    )?;
    Ok(launch)
}
