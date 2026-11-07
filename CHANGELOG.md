# Changelog

## Unreleased

First public release.

- The assistant has a soul and a face. Owners see two things: the Soul (`assistant/soul.md`: name, look,
  languages and personality, started from one of four presets in `resources/souls/`) and Memory. Every session
  reads three notes, soul first: the soul, the owner's own profile `people/<owner>/profile.md` (who they are and
  how they like things done) and `assistant/rules.md`. The owner profile used to be read from
  `people/<owner>/private/profile.md`, a path nothing wrote, so standing preferences drifted into the persona
  note; `assistant/persona.md` and `household/profile.md` are no longer read (see the setup guide's "Upgrading to
  the soul note"). The manual that ships with OPPA is now `resources/manual/MANUAL.md` and defers to the soul.
  Only the owner's own turn changes the soul or the rules. The face is a picture kept beside the vault:
  `soul.avatar` (owner turns) and `oppa soul avatar <picture>` square it at 1024 pixels and keep earlier ones.
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
