//! Channels the owner adds after init. `oppa channels add` writes a
//! `[[channels]]` entry and the owner's handles on it, keeping the rest of
//! config.toml as the owner wrote it. Serve starts a configured channel it is
//! not running within seconds and records what it runs in
//! `<runtime>/channels/serving.json`, which `oppa channels` shows, so the
//! assistant can guide the owner through a setup in conversation. An
//! iMessage handle counts as the owner's only once the owner confirms it.
//! Logins that need the owner's hands (a WeChat scan, Slack's tokens) are
//! steps the bundled adapter runs.

use crate::config::{ChannelManifest, Config, Paths};
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::PathBuf, process::Command};

const PYTHON: &str = "/usr/bin/python3";

fn manifest(id: &str, account: &str, timeout_seconds: u64, args: Vec<String>) -> ChannelManifest {
    ChannelManifest {
        id: id.into(),
        account: account.into(),
        command: [vec![PYTHON.into()], args].concat(),
        timeout_seconds,
        account_holder: Default::default(),
    }
}

fn runtime(paths: &Paths, relative: &str) -> String {
    paths.runtime.join(relative).to_string_lossy().into_owned()
}

/// iMessage from the assistant's own Apple ID, through the imsg bridge.
pub fn imessage(paths: &Paths) -> ChannelManifest {
    let args = vec![
        runtime(paths, "adapters/imessage.py"),
        "--state".into(),
        runtime(paths, "channels/imessage"),
        "--imsg".into(),
        runtime(paths, "channels/imsg-bridge/imsg"),
    ];
    manifest("imessage", "assistant", 45, args)
}

/// Web chat on this Mac. Serve hands the adapter its own executable, so
/// nothing here goes stale on an upgrade.
pub fn web(paths: &Paths, port: u16) -> ChannelManifest {
    let args = vec![
        runtime(paths, "adapters/web.py"),
        "--state".into(),
        runtime(paths, "channels/web"),
        "--home".into(),
        paths.runtime.to_string_lossy().into_owned(),
        "--private-dir".into(),
        paths.private.to_string_lossy().into_owned(),
        "--port".into(),
        port.to_string(),
    ];
    manifest("web", "local", 30, args)
}

/// WeChat through its ClawBot plugin. `base_url` replaces Tencent's iLink
/// address, for a synthetic server.
pub fn wechat(paths: &Paths, base_url: Option<&str>) -> ChannelManifest {
    let mut args = vec![
        runtime(paths, "adapters/wechat.py"),
        "--state".into(),
        runtime(paths, "channels/wechat"),
    ];
    if let Some(url) = base_url {
        args.extend(["--base-url".into(), url.into()]);
    }
    manifest("wechat", "clawbot", 45, args)
}

/// An owner's iMessage handle: a phone number as +15555550100, or an email
/// address.
pub fn imessage_handle(handle: &str) -> Result<String> {
    let handle = handle.trim();
    let phone = handle.strip_prefix('+').is_some_and(|digits| {
        (7..=15).contains(&digits.len()) && digits.bytes().all(|b| b.is_ascii_digit())
    });
    let email = !handle.contains(char::is_whitespace)
        && handle.split_once('@').is_some_and(|(local, domain)| {
            !local.is_empty()
                && !domain.contains('@')
                && domain.split('.').count() > 1
                && domain.split('.').all(|part| !part.is_empty())
        });
    ensure!(
        phone || email,
        "{handle:?} is neither a phone number written as +15555550100 nor an email address"
    );
    Ok(format!("imessage:{handle}"))
}

/// Add `channel` unless one with its id is configured, and register
/// `handles` as the owner's on it. With `replace`, the owner's other handles
/// on that channel go: a WeChat login binds one account at a time. Returns
/// whether the channel was new; an existing one is kept as it is.
pub fn add(
    paths: &Paths,
    channel: &ChannelManifest,
    handles: &[String],
    replace: bool,
) -> Result<bool> {
    #[derive(Serialize)]
    struct Entry<'a> {
        channels: [&'a ChannelManifest; 1],
    }
    let entry: toml_edit::DocumentMut = toml::to_string(&Entry {
        channels: [channel],
    })?
    .parse()?;
    let table = entry["channels"]
        .as_array_of_tables()
        .and_then(|tables| tables.get(0))
        .context("channel entry")?
        .clone();
    let prefix = format!("{}:", channel.id);
    let (_, added) = crate::config::rewrite(
        paths,
        |current: &Config, document| {
            let added = !current.channels.iter().any(|c| c.id == channel.id);
            if added {
                if document.get("channels").is_none() {
                    document.insert(
                        "channels",
                        toml_edit::Item::ArrayOfTables(Default::default()),
                    );
                }
                document["channels"]
                    .as_array_of_tables_mut()
                    .context("channels is not a list of tables")?
                    .push(table);
            }
            if document.get("people").is_none() {
                document.insert("people", toml_edit::Item::ArrayOfTables(Default::default()));
            }
            let people = document["people"]
                .as_array_of_tables_mut()
                .context("people is not a list of tables")?;
            let found = people.iter().position(|p| {
                p.get("id").and_then(|id| id.as_str()) == Some(current.owner.as_str())
            });
            let index = match found {
                Some(index) => index,
                None => {
                    let mut table = toml_edit::Table::new();
                    table.insert("id", toml_edit::value(current.owner.as_str()));
                    table.insert("role", toml_edit::value("owner"));
                    people.push(table);
                    people.len() - 1
                }
            };
            let owner = people.get_mut(index).context("owner entry")?;
            let mut list: Vec<String> = owner
                .get("handles")
                .and_then(|h| h.as_array())
                .map(|array| {
                    array
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default();
            // Compared as the registry compares them: an email in any case is one handle.
            let key = crate::config::handle_key;
            if replace {
                list.retain(|h| {
                    !h.starts_with(&prefix) || handles.iter().any(|n| key(n) == key(h))
                });
            }
            for handle in handles {
                if !list.iter().any(|h| key(h) == key(handle)) {
                    list.push(handle.clone());
                }
            }
            crate::people::set_if_changed(
                owner,
                "handles",
                toml_edit::value(list.into_iter().collect::<toml_edit::Array>()),
            );
            Ok(added)
        },
        |config, _| {
            ensure!(
                handles.iter().all(|h| config
                    .person(&channel.id, h.strip_prefix(&prefix).unwrap_or(h))
                    .is_some_and(|p| p.id == config.owner)),
                "the handles were not registered as the owner's"
            );
            Ok(())
        },
    )?;
    Ok(added)
}

/// Handles waiting for the owner's word before they count as the owner's.
/// Anything the assistant reads could ask it to add a number, so a handle
/// added through `oppa channels add imessage` waits until the owner, writing
/// as themselves, sends the code it was given. A code lasts an hour.
#[derive(Serialize, serde::Deserialize)]
pub struct Pending {
    pub handles: Vec<String>,
    pub code: String,
    /// The newest event when the code was made: only a later message counts.
    after: i64,
    made: u64,
}

const PENDING_SECONDS: u64 = 3600;

fn pending_file(paths: &Paths) -> PathBuf {
    paths.runtime.join("channels").join("pending-owner.json")
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Hold iMessage `handles` until the owner sends the returned code,
/// replacing any held before.
pub fn hold(paths: &Paths, handles: Vec<String>) -> Result<Pending> {
    let mut random = [0u8; 4];
    std::io::Read::read_exact(&mut fs::File::open("/dev/urandom")?, &mut random)?;
    let pending = Pending {
        handles,
        code: format!("{:06}", u32::from_le_bytes(random) % 1_000_000),
        after: crate::store::Store::open(&paths.database())?.last_seq()?,
        made: now(),
    };
    let path = pending_file(paths);
    fs::create_dir_all(path.parent().context("channels directory")?)?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&pending)?)?;
    fs::rename(temporary, path)?;
    Ok(pending)
}

/// What became of the held handles.
pub enum Confirmed {
    /// Nothing is held.
    None,
    /// The code is over an hour old; the handles were dropped.
    Expired(Pending),
    /// The owner has not sent the code yet.
    Waiting(Pending),
    /// The owner sent it: the handles are the owner's now.
    Added(Pending),
}

/// Register the held handles once a message the owner wrote after they were
/// held carries the code.
pub fn confirm(paths: &Paths) -> Result<Confirmed> {
    let path = pending_file(paths);
    let Ok(bytes) = fs::read(&path) else {
        return Ok(Confirmed::None);
    };
    let pending: Pending = serde_json::from_slice(&bytes)?;
    if now().saturating_sub(pending.made) > PENDING_SECONDS {
        fs::remove_file(&path)?;
        return Ok(Confirmed::Expired(pending));
    }
    let owner = paths.load()?.owner;
    let store = crate::store::Store::open(&paths.database())?;
    if !store.wrote_after(&owner, pending.after, &pending.code)? {
        return Ok(Confirmed::Waiting(pending));
    }
    add(paths, &imessage(paths), &pending.handles, false)?;
    // A hold made meanwhile stays for its own code.
    let current: Option<Pending> = fs::read(&path)
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok());
    if current.is_some_and(|current| current.code == pending.code) {
        fs::remove_file(&path)?;
    }
    Ok(Confirmed::Added(pending))
}

/// A step of the WeChat login, run by the bundled adapter.
pub enum Login {
    /// Get a QR code link.
    Start,
    /// Wait up to this many seconds for its scan.
    Finish(u64),
    /// Put the scanned login in use, once the registry has its handle.
    Activate,
}

/// Run one step of the WeChat login and return what the adapter printed.
pub fn wechat_login(paths: &Paths, step: Login, base_url: Option<&str>) -> Result<Value> {
    let wait;
    let args: &[&str] = match step {
        Login::Start => &["--login-start"],
        Login::Finish(seconds) => {
            wait = seconds.to_string();
            &["--login-finish", "--wait", &wait]
        }
        Login::Activate => &["--login-activate"],
    };
    login(paths, &wechat(paths, base_url), args)
}

/// Slack through an app the owner makes from a manifest link: its bot
/// answers, and `ask` posts as it to hand the owner's other agents a request.
/// `api_url` replaces Slack's Web API, for a synthetic server.
pub fn slack(paths: &Paths, api_url: Option<&str>) -> ChannelManifest {
    let mut args = vec![
        runtime(paths, "adapters/slack.py"),
        "--state".into(),
        runtime(paths, "channels/slack"),
    ];
    if let Some(url) = api_url {
        args.extend(["--api-url".into(), url.into()]);
    }
    manifest("slack", "bot", 45, args)
}

/// A step of the Slack login, run by the bundled adapter.
pub enum SlackLogin<'a> {
    /// The link that creates the app, named `name` in Slack.
    Link(&'a str),
    /// Check and keep the token this command prints, the clipboard's by default.
    Token(Option<&'a str>),
    /// Put the checked tokens in use, once the registry has the owner's handle.
    Activate,
}

/// Run one step of the Slack login and return what the adapter printed.
pub fn slack_login(paths: &Paths, step: SlackLogin, api_url: Option<&str>) -> Result<Value> {
    let args: Vec<&str> = match step {
        SlackLogin::Link(name) => vec!["--login-link", "--name", name],
        SlackLogin::Token(None) => vec!["--login-token"],
        SlackLogin::Token(Some(clipboard)) => vec!["--login-token", "--clipboard", clipboard],
        SlackLogin::Activate => vec!["--login-activate"],
    };
    login(paths, &slack(paths, api_url), &args)
}

/// Run a channel adapter's login step and return the JSON it printed last.
fn login(paths: &Paths, channel: &ChannelManifest, args: &[&str]) -> Result<Value> {
    crate::harness::install_bundled_adapters(&paths.runtime)?;
    let mut command = Command::new(&channel.command[0]);
    command.args(&channel.command[1..]).args(args);
    let output = crate::process::output(&mut command)?;
    let last = |bytes: &[u8]| {
        String::from_utf8_lossy(bytes)
            .lines()
            .last()
            .unwrap_or_default()
            .to_owned()
    };
    ensure!(
        output.status.success(),
        "the {} login failed: {}",
        channel.id,
        last(&output.stderr)
    );
    serde_json::from_str(&last(&output.stdout))
        .with_context(|| format!("the {} adapter gave no answer", channel.id))
}

fn serving_file(paths: &Paths) -> PathBuf {
    paths.runtime.join("channels").join("serving.json")
}

/// What serve runs, by channel id: `running`, or why it could not start.
/// Written by serve whenever it changes.
pub fn record_serving(paths: &Paths, states: &BTreeMap<String, Value>) -> Result<()> {
    let path = serving_file(paths);
    fs::create_dir_all(path.parent().context("channels directory")?)?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(states)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

/// Each configured channel with what serve last recorded for it, and
/// whether serve runs now.
pub fn report(paths: &Paths) -> Result<Value> {
    let config = paths.load()?;
    let serving: BTreeMap<String, Value> = fs::read(serving_file(paths))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default();
    let running = crate::channel::serve_running(paths);
    let channels: Vec<Value> = config
        .channels
        .iter()
        .map(|channel| {
            let handles: Vec<&String> = config
                .people
                .iter()
                .find(|p| p.id == config.owner)
                .map(|owner| {
                    owner
                        .handles
                        .iter()
                        .filter(|h| h.starts_with(&format!("{}:", channel.id)))
                        .collect()
                })
                .unwrap_or_default();
            let state = match serving.get(&channel.id) {
                Some(state) if running => state.clone(),
                _ if running => json!({"state": "starting"}),
                _ => json!({"state": "serve is not running"}),
            };
            json!({"id": channel.id, "account": channel.account, "owner_handles": handles, "serving": state})
        })
        .collect();
    Ok(json!({"serve_running": running, "channels": channels}))
}
