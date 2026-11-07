# Changelog

## Unreleased

First public release.

- iMessage tapbacks and the typing bubble, optional: both need imsg's bridge, which needs SIP off; without it
  text works as before and the channel claims neither. A turn reacts with `messages.react` (love, like, dislike,
  laugh, emphasize, question); a message takes one reaction, the first is final, and it is sent when the turn ends
  and counts as delivered only once Messages history shows it on the message. The bubble shows while a turn
  answering someone runs. The adapter's `--launch-bridge` (new installs) restarts Messages with the helper when
  it is down and SIP is off. Setup has an optional "Tapbacks and typing" guide, also for later.
- The dashboard has one tab bar: the User/Developer switch is gone and Health is its last tab.
- Slack: `ask` posts as the assistant's own app, not as the owner. Agents in Slack skip every app post,
  including one made with the owner's token, so they find requests by checking the room on a schedule.
  Setup step 10 walks the owner through connecting each agent and pasting the polling prompt.
