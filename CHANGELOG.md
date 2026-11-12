# Changelog

## Unreleased

First public release.

- Slack replies are sent as Markdown that Slack renders: tables, code blocks with their language, lists and
  links, where a reply used to be rewritten into Slack's older markup and showed tables and code as plain text.
  A long reply still goes in parts of at most 3,900 characters, and a code block cut between two parts is now
  closed in the first and opened again in the next. @here, @channel and @everyone in a reply no longer notify
  anyone. The formatting lives in `adapters/slack_format.py`, a single file with no OPPA imports, so another
  tool that posts to Slack can copy it. One real message on 2026-11-12 confirmed the path: Slack took
  `markdown_text` from the app, with `&`, `<` and `>` unescaped, and stored it as a table block, a code block
  and a list.

- A background task chooses its own model and effort. `tasks.delegate` takes an optional `model` and `effort`
  beside the agent, which stays the choice of harness and provider; what a task leaves out is the agent's own
  from `config.toml`. The assistant is shown each registered agent's family, model and effort, and the manual
  has it fit the choice to the work and follow what the owner said in the rules note. A model the harness
  cannot run fails the task with its reason. Existing instances gain two columns on the tasks table in place; a build put back
  after an upgrade still runs, but cannot read the receipt of a task that chose a model or effort.

- Watches: the assistant can watch something all day without a model turn for every look. A schedule with
  `watch` (`every_minutes` 1 to 60, `until`, `cooldown_minutes` 5 to 1440, 30 by default) runs its command every
  few minutes from `at` until `until` on its days. Serve runs it on a task of its own, so no turn waits for it,
  one run at a time, and starts the schedule's turn only when the command prints a line `{"trigger": true}` or
  fails; after a turn none starts for the cooldown. An ask is owed a turn until one answers it: one made in the
  cooldown is held and reaches the next turn under `held` (five at most, the oldest dropped), one whose turn had
  not started when oppa stopped fires after the restart, and a run cut off mid-command reports that instead of
  staying silent. An ask counts as answered once its event is logged, as a schedule slot does. `runs/watch-<id>.json` keeps the latest run and the asks still owed, which
  `oppa schedules` shows as `last_run`. `oppa schedules add` takes `--every`, `--until` and `--cooldown`; at
  `computer` access `schedules.create` takes `watch` and `command`. The policy hash changes (the manual and the
  schedule tools), so sessions start afresh after an install. An earlier build cannot read a stored watch:
  cancel watches before installing one.
- The assistant has a soul and a face. Owners see two things: the Soul (`assistant/soul.md`: name, look,
  languages and personality, started from one of four presets in `resources/souls/`) and Memory. Every session
  reads three notes, soul first: the soul, the owner's own profile `people/<owner>/profile.md` (who they are and
  how they like things done) and `assistant/rules.md`. The owner profile used to be read from
  `people/<owner>/private/profile.md`, a path nothing wrote, so standing preferences drifted into the persona
  note. `household/profile.md` is no longer read, and `assistant/persona.md` stands in only for a soul not yet
  written, so an upgrade keeps the name and voice (the setup guide's "Upgrading to the soul note" moves it, and the
  assistant writes the soul from it at the owner's next change). The manual is now `resources/manual/MANUAL.md`
  and defers to the soul.
  Only the owner's own turn changes the soul or the rules. The face is a picture in `avatars/` beside the vault,
  which ignores itself in the private repository: `soul.avatar` (owner turns; below full access only a picture sent
  in the chat or an earlier face) and `oppa soul avatar <picture>` square it at 1024 pixels, one change at a time,
  and keep earlier faces.
  `messages.contact_card` sends the assistant's contact card in a direct chat, once per name and face: the
  iMessage adapter writes a vCard with the address Messages records for that chat and the face as its photo,
  with no bridge needed; other channels refuse it as not sent. The dashboard's tabs are Chat, Soul (face,
  presets, the soul note), Memory (about you, rules, people), Schedules, Access and Health. Setup asks for a
  name, a personality and a look, draws four faces with Codex on the owner's ChatGPT plan while it builds (an
  image key is the paid fallback, a picture or initials the free one), registers Codex beside a Claude driver,
  and asks about importing Claude Code and Codex memory up front. The import's closing message now says what it
  left out, and onboarding offers a paste-in prompt for other assistants.
- The OPPA mark no longer shows a sliver of blue between the hair and the right ear: the brand kit's new
  temple path is in `site/assets` (icon, face, lockups, social card) and in the dashboard's inline mark and favicon.
- iMessage tapbacks and the typing bubble, optional: both need imsg's bridge, which needs SIP off; without it
  text works as before and a tapback is refused as not sent. A turn reacts with `messages.react` (love, like, dislike,
  laugh, emphasize, question); a message takes one reaction, the first is final, and it is sent when the turn ends
  and counts as delivered only once Messages history shows it on the message. The bubble shows while a turn
  answering someone in a direct chat runs, without ever holding the turn up. The adapter's `--launch-bridge`
  (new installs) restarts Messages with the helper when it is down and SIP is off, at most once per boot.
  Setup has an optional "Tapbacks and typing" guide, also for later.
- The dashboard has one tab bar: the User/Developer switch is gone and Health is its last tab.
- Slack: `ask` posts as the assistant's own app, not as the owner. Agents in Slack skip every app post,
  including one made with the owner's token, so they find requests by checking the room on a schedule.
  Setup step 10 walks the owner through connecting each agent and pasting the polling prompt.

- First setup explicitly selects the coding agent performing it: Claude Code
  selects Claude, Codex selects Codex; repairs preserve the existing driver.
  The setup agent leaves choices and pending steps for OPPA to finish in chat.
  Codex setup checks CLI availability and ChatGPT subscription sign-in,
  and either driver verifies a second turn before
  service installation. API keys and Claude are not required for Codex setup.
  `oppa doctor` now detects missing or non-executable bundled harness CLIs;
  install the selected CLI on PATH, sign in, and retry an actual chat.
  Codex CLI 0.164.0 with ChatGPT sign-in passed disposable console turns at read
  and full access (same native session across each pair), a memory broker read,
  and web-chat init, login and reply through the page's HTTP API. The web drive
  now answers the browser and vault prompts too. Fresh-account browser sign-in,
  launchd credentials and phone delivery still require the owner's laptop test.
  Validation: `cargo test --offline`, `python3 scripts/audit.py` and
  `git diff --check` passed; the separate diff review found no policy changes.
- Documented switching an existing instance through the external coding agent,
  preserving OPPA memory and history and verifying the target subscription.
  This adds guidance, not an in-chat switch or a new settings command.
