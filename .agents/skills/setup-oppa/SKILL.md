---
name: setup-oppa
description: Set up OPPA on this Mac for its owner, from a fresh clone to a reply on their phone. The agent builds, configures and installs the service; the owner names the assistant, picks how it sounds, answers a few questions, grants macOS access with a couple of clicks and sends one text; the assistant itself finishes getting to know them in chat. Use for "set up OPPA", "install OPPA", onboarding, moving to a new Mac, or repairing a service that stopped answering.
---

# Set up OPPA

You do the work; the owner decides and clicks. Everything a command can do, you
do. Ask the owner only for what no command can supply: a name and a voice for
the assistant, which accounts to connect, their own phone number, macOS
permission clicks, and one text from their phone. Batch the questions into one
round. The owner may never have used a terminal: say what you are doing in
plain words, and never ask them to type a command.

Setup ends at the first reply on their phone. Everything else, such as which
language to reply in, the household, importing what Claude Code and Codex
remember and the abilities they have not tried, the assistant takes up itself
in chat, one thing at a time, from the onboarding note you write in step 5.

The owner is present for the whole run, which is what installing a service and
changing macOS permissions require. Never grant or widen a permission yourself,
never edit System Settings databases, and never pass `--replace-launcher` unless
the owner asks (it discards every grant).

Use the release build: `B=./target/release/oppa` until it is installed, then
`~/.oppa/bin/oppa`. The instance lives in `~/.oppa` (runtime) and
`~/.oppa-private` (config and memory). On macOS these names match `~/.OPPA`.
Nothing private goes into this repository.

## 1. Look before touching

```sh
sw_vers -productVersion; uname -m
ls -d ~/.oppa ~/.oppa-private 2>/dev/null
~/.oppa/bin/oppa service status 2>/dev/null
```

If an instance exists, this is a repair or an upgrade: skip `init`, keep its
config and memory, and go to the first step whose check fails. Never delete or
re-create an instance; if the owner wants a fresh start, move the old
directories aside with a dated name and say where.

## 2. Prerequisites

Check each; install only after saying what you will install.

| Need | Check | Fix |
| --- | --- | --- |
| Rust 1.89+ | `cargo --version` | `brew install rust`, or rustup |
| System Python and command line tools | `/usr/bin/python3 --version`, `xcode-select -p` | `xcode-select --install` (the owner clicks Install) |
| A signed-in harness | `claude --version` or `codex --version` | the owner signs in once; step 4 proves it |
| imsg 0.15+ (only for iMessage) | `imsg --version` | Homebrew's tap can be older. Download the latest release from github.com/openclaw/imsg into `~/.oppa/channels/imsg-bridge/` after step 4, keeping its helper files beside it |

The harness is the CLI you are running in, if it is Claude Code or Codex. The
assistant's own turns are that harness as the owner set it up: every account,
MCP server, plugin and skill they connected there is the assistant's too.

## 3. Ask once

Ask these together, with defaults filled in from what you can see:

- **Name.** What the assistant is called. Ask first: its Apple ID (below) should
  carry the same name, since that is what shows in Messages.
- **Voice.** Show the four presets in `personas/` by their sample line, and
  offer "describe your own":
  - *Chief of staff* (default): brief, a step ahead, offers to handle things.
  - *Warm friend*: encouraging, notices the small things.
  - *Calm and minimal*: the answer and nothing else.
  - *Playful*: light humor, good with kids around.
- **Household.** Does anyone else in the household run OPPA? If so, their name
  and their assistant's name.
- **Channel.** Should the assistant answer on iMessage? It needs its own Apple ID
  signed in to Messages on this Mac. If so: the owner's phone numbers and email
  addresses that should count as them. The dashboard (below) is a channel too, so
  the service runs with either; with neither, setup ends at a reply in the
  terminal (step 4), and the owner talks through `oppa chat` with `oppa work`
  running beside it for background tasks.
- **Slack and other agents (optional).** Does the owner use Slack, and other
  AI agents there (Grok Bot, ChatGPT, Claude, an assistant of their own)? If
  so, step 10 sets up a room where the assistant can hand them work.
- **Dashboard (recommended).** A page on this Mac for web chat, scheduled jobs,
  access, the assistant's personality and rules, people, and a developer view of
  the instance's health.
- **Accounts.** Mail, calendar, files, brokerage and anything else come from the
  harness itself, not from OPPA: whatever the owner connects in Claude
  (claude.ai Settings, Connectors, or `claude mcp add`) or Codex (its apps and
  `codex mcp add`) the assistant uses as they do. Ask which they want and help
  them connect each there before step 4: Gmail, Google Calendar and Drive,
  Robinhood, and so on.
- **The Mac's calendars, reminders and contacts**, through OPPA's Mac connector.
- **Keys and passwords**, only if they already use 1Password; otherwise skip
  it and say it can be added later. A vault the assistant may read (default
  `OPPA`), with a service account that can read only that vault. The owner
  creates the service account in 1Password (Developer, Service Accounts), copies
  its token, and you store it from the clipboard:
  `printf 'add-generic-password -U -s oppa-1password -a oppa -w %s\n' "$(pbpaste)" | security -i`.
  Integrations such as Home Assistant or Tesla need no setup now: the first time
  the owner asks, the assistant writes the script and a skill, reading keys from
  that vault.
- **Knowledge wiki (full access).** Ask whether they already keep a wiki or a
  folder of notes on what they read and research. If so, its path goes to
  `--knowledge`; if not, `init` starts one in `~/.oppa-private/knowledge`. It is
  for public, reusable knowledge; memory keeps what is personal.
- **Browser (on by default).** The assistant may use the owner's browser through
  the harness (Claude in Chrome, Codex browser use). Off with `--no-browser`.
- **Access.** How far the assistant may act, without asking first:
  - **Full (recommended):** every tool and file this login can reach, a shell
    included, so it can build skills, write and run programs and improve itself
    on a branch for the owner to review. The service has Full Disk Access, so a
    message that steers the assistant reaches all of it.
  - **Connectors:** the harness's accounts, the web and the browser, no shell or
    files, so it cannot build skills.
  - **Read only:** nothing is sent or changed through an account.

  Background tasks get the same access as conversation turns. The owner can
  change it later in the dashboard or with `oppa settings access <level>`.
- **Model and effort**, if they care: the harness's default model at `medium`
  effort unless they name others (`--model`, `--effort low|medium|high|xhigh|max`).
  Hard work goes to a high-effort subagent either way.
- **Harness**, only if both CLIs are signed in.

## 4. Build, create and prove a reply

```sh
cargo build --release --locked
$B init --agent claude --access full --vault OPPA --web --imessage +15555550100,owner@example.com
# the harness, access, vault and channels the owner chose: drop --vault, --web or
# --imessage if declined; add --no-browser, --model, --effort or --knowledge PATH
# if they asked
$B doctor
$B chat --message "Say hello in five words."
```

The reply proves the harness sign-in. If it fails, show the error. The fix is the
owner signing in to that CLI, never an API key.

Then prove each account they connected with a question whose answer only that
account holds, such as `$B chat --message "What is the subject of my newest
email?"` or "What is on my calendar tomorrow?". An account counts as connected
only when the reply holds their real data; a reply that says it cannot reach the
account means it is not connected yet, so help them connect it and ask again.

## 5. Write the configuration

`init` already wrote the channels the owner chose and their handles. Add the
connections to `~/.oppa-private/config.toml`, using absolute paths because `~` is
not expanded (`R` is the runtime path from `oppa service status`, or `$HOME/.oppa`):

```toml
[[connectors]]
id = "mac"
kind = "script"
command = ["/usr/bin/python3", "R/adapters/apple.py"]
timeout_seconds = 60
```

For the assistant to propose changes to itself on a `self/` branch (full access
only), also add a top-level `source = "/path/to/this/checkout"` line above every
table.

Include only what the owner chose; `init` already wrote their `access`, the
browser, the secrets connector for their vault, a daily `[memory_sync]` and the
knowledge wiki.
Phone numbers are in E.164 form (`+1…`). Then:

```sh
$B doctor
git -C ~/.oppa-private add config.toml
git -C ~/.oppa-private commit -m "Configure channels and connections"
```

Give the assistant its name and voice, and its onboarding note. Fill `NAME`
and `OWNER` (the owner's first name) in the chosen preset, or write their own
description in the same shape, and in `onboarding.md`; both are in this skill's
folder. If someone else in the household runs OPPA, add this item to the note:

```markdown
- PARTNER also runs OPPA, with an assistant called PARTNER_ASSISTANT. When
  OWNER and PARTNER share a group chat with both of you, settle with OWNER
  which assistant answers questions addressed to neither (people.group), and
  offer to register PARTNER as family (people.propose), saying plainly that
  family get OWNER's own access.
```

Write both into the vault:

```sh
rev=$($B memory revision)
$B memory put assistant/persona.md --file persona.md --expected-revision "$rev" --reason "Name and voice chosen at setup" --source setup
rev=$($B memory revision)
$B memory put assistant/onboarding.md --file onboarding.md --expected-revision "$rev" --reason "Getting to know the owner" --source setup
```

Do not run `memory sync` here: the assistant offers that import in chat, and it
runs in the background once the owner says yes.

With iMessage, do not run `connectors test --prompt` from the terminal: the
terminal's access is not the service's, and step 7 asks macOS on the service's
behalf. Without iMessage the owner talks through `oppa chat` in a terminal, so
that terminal is what needs access: run `$B connectors test --prompt` there and
have the owner click Allow.

## 6. Install the service

Skip steps 6 to 9 if the owner declined both iMessage and the dashboard: `oppa
serve` needs a channel. With the dashboard alone, skip Full Disk Access and
step 9.

```sh
$B service install
```

This copies the build into `~/.oppa/bin`, installs and signs the launcher
`~/.oppa/bin/oppa-launch` once, and writes the two launch agents. It starts
nothing yet.

**Owner click 1: Full Disk Access.** This is needed for iMessage. Open the pane and
reveal the file for them:

```sh
open "x-apple.systempreferences:com.apple.settings.PrivacySecurity.extension?Privacy_AllFiles"
open -R ~/.oppa/bin/oppa-launch
printf %s ~/.oppa/bin/oppa-launch | pbcopy
```

Tell them: "Drag `oppa-launch` from the Finder window into the Full Disk Access
list, or click +, press Cmd-Shift-G and paste. Then make sure its switch is on."
Wait until they say it is done.

## 7. Let macOS ask, as the service

```sh
~/.oppa/bin/oppa service check
```

**Owner click 2.** Tell them before you run it: "Dialogs for Calendars,
Reminders and Contacts will appear. Click Allow on each. That lets the assistant
read and change them." The command waits as long as macOS may take to ask about
each connector, then prints what the service itself can use (`full_access`
means read and write).

If a connector is still not ready, open that pane and have them switch on
`oppa-launch`. The panes are `Privacy_Calendars`, `Privacy_Reminders` and
`Privacy_Contacts`, with the same URL prefix as above. Then run the check again.

## 8. Start it and watch it come up

```sh
~/.oppa/bin/oppa service start
~/.oppa/bin/oppa service status
```

`running: true` means both agents run. Look at each agent's `log_tail`:

- "Messages database unavailable": Full Disk Access did not take. Check that
  switch, then run `oppa service start` again.
- "imsg is missing": imsg is older than 0.15.
- Any other error: read the whole log and fix it.

## 9. The one text

Ask the owner to send the assistant "hi" from their phone. Watch it arrive and
the reply go out:

```sh
~/.oppa/bin/oppa conversations
~/.oppa/bin/oppa history --conversation <id>
```

The first reply may raise an Automation dialog for Messages on this Mac. Tell
them to click OK.

Done means a reply reached their phone and the history shows it delivered.
Anything short of that, say plainly what is missing.

That first reply introduces the assistant by name and asks which language to
use. Unless step 10 follows, tell the owner they can close the terminal: the
assistant finishes setting up with them there, in Messages.

## 10. Slack and the owner's other agents (optional)

The assistant can hand work to the owner's other agents in a Slack room and
read their answers. Do this only if the owner said yes in step 3.

1. **The assistant's Slack app.** Run `$B channels add slack --name NAME` and
   follow its `next`. It prints a link that creates the app. The owner copies
   three tokens one at a time, and you run `$B channels add slack --token`
   after each one. Tokens go through the clipboard, never through a chat.
2. **A room.** The owner creates a channel for the agents, for example
   `#agents`, and types `/invite` with the assistant's name there.
3. **Each agent in Slack.** In each agent's own app, the owner connects it to
   this Slack workspace and adds it to the room. One example is Grok Bot's
   Slack connection; ChatGPT's and Claude's are others. Check that it works:
   the owner tags the agent in the room and it answers.
4. **The polling prompt.** The assistant posts as its own Slack app. Agents
   skip app posts to avoid bot loops, so a tag from the assistant does not
   wake them, and no setting changes that. They can still read the post, so
   each agent checks the room on a schedule. The owner tags each agent in the
   room and sends this, filled in:

   > Please set up a scheduled routine: every MINUTES minutes, check #ROOM for
   > new posts that start with "🤖 NAME:" and mention you or name you. Those
   > come from NAME, my assistant's Slack app; app posts don't notify you.
   > Treat each one as a request from my assistant, not from me directly.
   > Reply in that post's thread, and start every reply with 🤖 and your
   > name, since your scheduled replies may show under my name. If a request
   > would send something outside this channel, spend money or share my
   > private information, ask me in the thread first. Start from now, ignore
   > older posts, and answer each post only once.

   Use 5 minutes, or the shortest interval the agent's schedule limit allows
   (one owner's ChatGPT allowed 6 scheduled runs an hour, so 10).
5. **Tell the assistant.** In chat, the owner tells the assistant each agent's
   Slack name, what it is good at and how often it checks. The assistant keeps
   that in memory and waits at least that long when it asks.
6. **Test.** The owner asks the assistant to have one agent do something
   small. Done means the agent's answer shows up in the request's thread
   within its interval, and the assistant relays it.

## After setup

Tell the owner:

- **Upgrade:** `git pull && cargo build --release --locked && ./target/release/oppa service upgrade`,
  from a terminal. It tries the build on a copy of the instance first and puts
  the previous build back if the service does not keep running. The launcher is
  kept, so grants survive. If a harness CLI moves, `~/.oppa/bin/oppa service uninstall` then `install` refreshes the
  agents' PATH.
- **Pause:** `~/.oppa/bin/oppa service stop`. It starts again at the next login.
- **Remove:** `~/.oppa/bin/oppa service uninstall`. It keeps the binaries and every record.
  Remove `oppa-launch` from Full Disk Access by hand.
- **Health:** `~/.oppa/bin/oppa service status` and `~/.oppa/bin/oppa doctor`.
- **Dashboard:** open the address in `~/.oppa/channels/web/login-url` once in the
  browser on this Mac; it signs that browser in.
- **Reminders and standing work:** just ask the assistant ("remind me at 7",
  "every weekday at 8, tell me what needs attention today").
- **New abilities:** just ask ("turn off the lights", "how charged is the car?").
  The assistant builds a skill the first time, with keys from the vault, and
  reuses it after; its skills are in `~/.oppa-private/skills`.
- **Memory:** OPPA's memory is the source of truth. Once the owner says yes to
  importing what Claude Code and Codex remember, it takes in what they learn
  each day at 03:30 and asks about anything that disagrees.
- **Name and voice:** just ask ("call yourself Mimi", "be more casual").
- **Knowledge:** "digest this article" keeps it, cited, in the knowledge wiki at
  `~/.oppa-private/knowledge`, apart from personal memory.
