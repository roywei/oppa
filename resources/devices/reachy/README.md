# OPPA on a Reachy Mini

A Reachy Mini (the wireless model) becomes a room device for OPPA: you talk to the assistant through
the robot, and the assistant can wake it, speak through it, look through its camera and put it to sleep.

## How it works

```
person in the room <-> robot (oppa_body) <== audio ==> voice provider (OpenAI Realtime)
                              |
                              | requests, replies, wake, sleep, look   (HTTP on the home network)
                              v
                 reachy.py on the Mac  <-- channel protocol -->  oppa serve  -->  Claude or Codex
```

- The voice model only talks. It answers small talk itself and hands everything else to the assistant,
  which runs on your Claude or Codex plan. Only the voice is billed by the voice provider.
- Asleep, the robot holds no voice session, records nothing and costs nothing. Awake, it listens until
  someone says goodbye or tells it to sleep, the assistant puts it to sleep, or the room has been quiet for
  a minute (`--idle-seconds`).
- It wakes when the assistant wakes it or has words for the room, and when someone moves an antenna.
- The provider's key stays on the Mac. The robot gets a key that opens one session and lasts ten minutes.

## Set up

1. Add the channel to `config.toml` and give the room a person. Whoever speaks in the room is the one
   handle `reachy:room`; the person you give it decides what the room may ask for.

   ```toml
   [[channels]]
   id = "reachy"
   account = "home"
   command = ["/usr/bin/python3", "<runtime>/adapters/reachy.py", "--state", "<runtime>/channels/reachy",
              "--voice-key", "<a command that prints your OpenAI API key>"]
   timeout_seconds = 45

   [[people]]
   id = "home"
   role = "adult"
   handles = ["reachy:room"]
   ```

2. Restart `oppa serve`, so the adapter starts and writes its token.
3. Let this Mac log in to the robot over ssh without a prompt, then install the body app:

   ```sh
   resources/devices/reachy/install.sh reachy-mini.local http://<this-mac>.local:8767 <runtime>/channels/reachy
   ```

   The app starts now and at every boot of the robot, asleep.
4. Tell the assistant how to steer the robot, in a skill of its own:

   ```sh
   /usr/bin/python3 <runtime>/adapters/reachy.py ctl --state <runtime>/channels/reachy status
   /usr/bin/python3 <runtime>/adapters/reachy.py ctl --state <runtime>/channels/reachy wake "Dinner is ready."
   /usr/bin/python3 <runtime>/adapters/reachy.py ctl --state <runtime>/channels/reachy sleep
   /usr/bin/python3 <runtime>/adapters/reachy.py ctl --state <runtime>/channels/reachy look
   ```

   `look` prints the path of one camera picture and opens no voice session.

## What is enforced and what is not

- Every request to the adapter needs its token, and `ctl` is answered on this Mac only.
- The robot sends the token over plain HTTP. Anyone who can read traffic on the home network can speak
  as the room and open voice sessions on your key.
- Anyone in the room speaks as `reachy:room`. The robot does not know who is talking.
- `"debug": true` in the robot's `~/.config/oppa-body.json` logs what was heard and said on the robot.
