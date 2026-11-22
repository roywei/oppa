"""OPPA's body on a Reachy Mini: the microphone, speaker, camera and motors, and
nothing that thinks.

Asleep, the app holds no voice session and records nothing. It waits on a long
poll to the OPPA channel adapter on the owner's Mac (resources/adapters/reachy.py)
and watches its own antennas: the adapter can wake it, ask for one camera frame,
or give it words to say, and a person can wake it by moving an antenna.

Awake, it keeps one full-duplex session open with the voice provider, with a
short-lived key and a session set-up that both come from the adapter. The voice
model's tools run here: `delegate` hands a request to the adapter, `look`
shows the model a camera frame, `sleep` ends the session, and the body's own
tools (Body.tools) turn the head and base, play a dance or a feeling, and set
the volume. The robot also sleeps by itself after a stretch of silence, so a
forgotten session stops costing.

Configuration is one file, ~/.config/oppa-body.json:
    {"oppa": "http://the-mac.local:8767", "token": "<the adapter's token>"}
"""

import asyncio
import base64
import json
import logging
import math
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import numpy as np
import websockets
from reachy_mini import ReachyMini, ReachyMiniApp
from scipy.signal import resample_poly

CONFIG = Path.home() / ".config" / "oppa-body.json"
RATE = 24000            # the provider's audio: 16-bit mono
POLL_TIMEOUT = 40       # above the adapter's own long poll
TOUCH = 0.35            # radians an antenna must move within a second to wake the robot
EMOTIONS = "pollen-robotics/reachy-mini-emotions-library"
HEAD = {"left": (35, 0), "right": (-35, 0), "up": (0, -20), "down": (0, 20), "front": (0, 0)}   # yaw, pitch in degrees

log = logging.getLogger("oppa_body")


class Link:
    """The adapter on the owner's Mac. Calls block, so they run in threads."""

    def __init__(self, base, token):
        self.base = base.rstrip("/")
        self.token = token

    def call(self, path, body=None, timeout=20):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base + path, data, {
            "Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            try:
                message = json.loads(error.read()).get("error")
            except ValueError:
                message = None
            raise OSError(message or "the adapter answered %d" % error.code) from None


class Body:
    """The robot's hardware, in the provider's audio format."""

    def __init__(self, mini):
        self.mini = mini
        self.awake = False
        self.antennas = None
        self.antennas_at = 0.0
        self.following = True       # the head follows the face it sees, unless told to look elsewhere
        self.moves = {}             # name -> a dance or a recorded feeling the robot can play
        try:
            from reachy_mini.motion.recorded_move import RecordedMoves
            from reachy_mini_dances_library.collection.dance import AVAILABLE_MOVES
            from reachy_mini_dances_library.dance_move import DanceMove
            self.moves = {name: (lambda name=name: DanceMove(name)) for name in AVAILABLE_MOVES}
            feelings = RecordedMoves(EMOTIONS)
            self.moves.update({name: (lambda name=name: feelings.get(name)) for name in feelings.list_moves()})
        except Exception as error:      # the libraries or their data are missing: the robot still talks
            log.warning("no dances or feelings: %s", error)
        self.tools = [
            {"type": "function", "name": "look_toward",
             "description": "Turn the robot's head. `person` follows the face it sees, which is how it looks unless told.",
             "parameters": {"type": "object", "required": ["direction"], "properties": {
                 "direction": {"type": "string", "enum": [*HEAD, "person"]}}}},
            {"type": "function", "name": "turn_body",
             "description": "Turn the robot on its base to an angle in degrees: 0 is forward, positive is to its left.",
             "parameters": {"type": "object", "required": ["degrees"], "properties": {
                 "degrees": {"type": "number", "minimum": -150, "maximum": 150}}}},
            {"type": "function", "name": "set_volume",
             "description": "Set how loud the robot speaks, from 0 to 100.",
             "parameters": {"type": "object", "required": ["percent"], "properties": {
                 "percent": {"type": "number", "minimum": 0, "maximum": 100}}}},
        ]
        if self.moves:
            self.tools.append(
                {"type": "function", "name": "express",
                 "description": "Play a dance or show a feeling with the robot's body.",
                 "parameters": {"type": "object", "required": ["move"], "properties": {
                     "move": {"type": "string", "enum": sorted(self.moves)}}}})

    def act(self, name, arguments):
        """Run one of the body's own tools. Blocks while the body moves."""
        mini = self.mini
        if name == "look_toward":
            self.following = arguments.get("direction") == "person"
            if self.following:
                mini.start_head_tracking()
            else:
                from reachy_mini.utils import create_head_pose
                yaw, pitch = HEAD[arguments["direction"]]
                mini.stop_head_tracking()
                mini.goto_target(head=create_head_pose(yaw=yaw, pitch=pitch, degrees=True), duration=0.6, body_yaw=None)
        elif name == "turn_body":
            degrees = max(-150.0, min(150.0, float(arguments["degrees"])))
            mini.goto_target(body_yaw=math.radians(degrees), duration=1.0)
        elif name == "set_volume":
            percent = int(max(0, min(100, float(arguments["percent"]))))
            request = urllib.request.Request("http://127.0.0.1:8000/api/volume/set", json.dumps({"volume": percent}).encode(),
                                             {"Content-Type": "application/json"})
            urllib.request.urlopen(request, timeout=5).close()
        elif name == "express":
            # Without the move's own sound, which would play over the voice.
            asyncio.run(mini.async_play_move(self.moves[arguments["move"]](), initial_goto_duration=0.5, sound=False))

    def wake(self):
        mini = self.mini
        mini.enable_motors()
        mini.wake_up()
        mini.media.start_recording()
        mini.media.start_playing()
        self.in_rate = mini.media.get_input_audio_samplerate()
        self.out_rate = mini.media.get_output_audio_samplerate()
        mini.enable_wobbling()          # the head moves with the voice
        mini.start_head_tracking()      # and turns to the face it sees
        self.following = True
        # The waking move's own sound and motors are not someone speaking: on 2026-11-22 they
        # were heard as a word and the robot went straight back to sleep.
        settled = time.monotonic() + 1.0
        while time.monotonic() < settled:
            if mini.media.get_audio_sample() is None:
                time.sleep(0.01)
        self.awake = True

    def sleep(self):
        mini = self.mini
        self.awake = False
        mini.stop_head_tracking()
        mini.media.stop_recording()     # asleep, nothing is recorded
        mini.media.stop_playing()       # before the wobbling it feeds is taken away
        mini.disable_wobbling()
        mini.goto_sleep()
        mini.disable_motors()
        self.antennas = None

    def hear(self):
        """The next piece of microphone audio, or None when there is none yet."""
        sample = self.mini.media.get_audio_sample()
        if sample is None or not len(sample):
            return None
        if sample.ndim == 2:
            sample = sample[:, 0]
        if self.in_rate != RATE:
            sample = resample_poly(sample, RATE, self.in_rate)
        return (np.clip(sample, -1.0, 1.0) * 32767).astype("<i2").tobytes()

    def say(self, pcm):
        sample = np.frombuffer(pcm, dtype="<i2").astype(np.float32) / 32768
        if self.out_rate != RATE:
            sample = resample_poly(sample, self.out_rate, RATE).astype(np.float32)
        self.mini.media.push_audio_sample(sample)

    def hush(self):
        """Drop speech that is queued but not yet played: the person cut in."""
        self.mini.media.audio.clear_player()

    def photo(self):
        """One camera frame, taken with the head still: a head that follows a face or moves with
        the voice blurs the picture. Asleep, the head is down, so it lifts for the picture."""
        mini, lifted = self.mini, not self.awake
        if lifted:
            mini.enable_motors()
            mini.wake_up()
        else:
            mini.stop_head_tracking()
            mini.disable_wobbling()
        try:
            time.sleep(1.0 if lifted else 0.6)      # the head comes to rest
            frames = []
            for _ in range(20):
                jpeg = mini.media.get_frame_jpeg()
                if jpeg:
                    frames.append(bytes(jpeg))
                    if len(frames) == 5:
                        break
                time.sleep(0.1)
            if not frames:
                raise OSError("the camera gave no picture")
            return max(frames, key=len)     # the sharpest of a few: detail makes a JPEG larger
        finally:
            if lifted:
                mini.goto_sleep()
                mini.disable_motors()
                self.antennas = None
            else:
                mini.enable_wobbling()
                if self.following:
                    mini.start_head_tracking()

    def touched(self):
        """Asleep, the motors are off and an antenna moves only when someone moves it."""
        now = time.monotonic()
        position = np.array(self.mini.get_present_antenna_joint_positions())
        if self.antennas is None or now - self.antennas_at > 1.0:
            moved = self.antennas is not None and float(np.max(np.abs(position - self.antennas))) > TOUCH
            self.antennas, self.antennas_at = position, now
            return moved
        return False


class Voice:
    """One voice session, from waking to sleeping. One response runs at a time.
    `telling` is true while the response that runs is one we asked for to say
    the assistant's words. `owed` holds a response we asked for that was
    refused because another was running: it is asked for again, with its own
    `telling`, when that one ends."""

    def __init__(self, body, link, inbox, grant):
        self.body, self.link, self.inbox, self.grant = body, link, inbox, grant
        self.ws = None
        self.busy = self.telling = self.mute = False
        self.owed = None            # None, or the `telling` of the response that was refused
        self.results = []           # words from the assistant, waiting to be said
        self.waiting_until = 0.0    # an answer was promised to the room: stay awake for it until then
        self.active = time.monotonic()
        self.playing_until = 0.0
        self.reason = None          # why the session ends
        self.acts = set(tool["name"] for tool in getattr(body, "tools", ()))
        self.acting = None          # the body's move in progress; one at a time
        self.spoken = 0.0           # seconds of speech played, for the log
        self.handed = 0

    async def send(self, event):
        await self.ws.send(json.dumps(event))

    async def run(self, first):
        loop = asyncio.get_running_loop()
        self.ws = await websockets.connect(
            self.grant["url"], additional_headers={"Authorization": "Bearer " + self.grant["secret"]}, max_size=None)
        try:
            session = json.loads(json.dumps(self.grant["session"]))
            session["tools"] = session["tools"] + self.body.tools
            if log.isEnabledFor(logging.DEBUG):     # what was heard, written out for the log only
                session["audio"]["input"]["transcription"] = {"model": "gpt-4o-mini-transcribe"}
            await self.send({"type": "session.update", "session": session})
            await asyncio.to_thread(self.body.wake)
            if first.get("kind") == "say":
                self.results.append(first["text"])
                await self.flush()
            tasks = [asyncio.create_task(job) for job in (self.listen(loop), self.events(), self.orders(), self.watch())]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                if task.exception() is not None and self.reason is None:
                    self.reason = "the voice line dropped: %s" % task.exception()
        finally:
            await self.ws.close()
            if self.reason == "told to sleep":
                self.body.hush()    # the assistant's stop is at once, mid-sentence if need be
            else:                   # a goodbye already queued plays out before the head goes down
                await asyncio.sleep(max(0.0, min(8.0, self.playing_until - time.monotonic())))
            await asyncio.to_thread(self.body.sleep)
        return self.reason or "the voice line closed"

    async def listen(self, loop):
        """Microphone to provider. The read polls, so it runs in a thread."""
        stop = threading.Event()

        def pump():
            while not stop.is_set():
                pcm = self.body.hear()
                if pcm is None:
                    time.sleep(0.01)
                    continue
                frame = json.dumps({"type": "input_audio_buffer.append", "audio": base64.b64encode(pcm).decode()})
                asyncio.run_coroutine_threadsafe(self.ws.send(frame), loop).result()

        try:
            await asyncio.to_thread(pump)
        finally:
            stop.set()

    async def events(self):
        async for raw in self.ws:
            event = json.loads(raw)
            kind = event["type"]
            if kind == "response.output_audio.delta":
                if self.mute:
                    continue
                pcm = base64.b64decode(event["delta"])
                self.body.say(pcm)
                now = time.monotonic()
                self.playing_until = max(self.playing_until, now) + len(pcm) / 2 / RATE
                self.spoken += len(pcm) / 2 / RATE
            elif kind == "input_audio_buffer.speech_started":
                self.body.hush()        # the person cut in: drop what is left of the reply
                log.debug("speech started")
                self.mute = True
                self.playing_until = 0.0
                self.active = time.monotonic()
            elif kind == "input_audio_buffer.speech_stopped":
                self.active = time.monotonic()
            elif kind == "conversation.item.input_audio_transcription.completed":
                log.debug("heard: %s", event.get("transcript"))
            elif kind == "response.output_audio_transcript.done":
                log.debug("said: %s", event.get("transcript"))
            elif kind == "response.created":
                self.busy = True
                self.mute = False
                log.debug("response started")
            elif kind == "response.done":
                await self.finished(event.get("response") or {})
            elif kind == "error":
                message = (event.get("error") or {}).get("message") or "the voice provider reported an error"
                if "active response" in message or "ongoing" in message:
                    # Ours was refused: the response that runs is the room's own, and may use its tools.
                    self.owed, self.telling = self.owed or self.telling, False
                else:
                    log.warning("voice provider: %s", message)
        self.reason = self.reason or "the voice provider closed the line"

    async def finished(self, response):
        """A response ended: run the tools it called, then say what is waiting."""
        log.debug("response ended: %s %s", response.get("status"), [item.get("type") for item in response.get("output") or []])
        self.busy = False
        # A response that tells the assistant's words runs no tool: only the room's own speech hands
        # a request on or ends the session, whatever the words being told say.
        telling, self.telling = self.telling, False
        output = response.get("output") or []
        spoke = any(item.get("type") == "message" for item in output)
        again = False
        for item in output:
            if item.get("type") != "function_call":
                continue
            try:
                arguments = json.loads(item.get("arguments") or "{}")
            except ValueError:
                arguments = {}
            name, result, answer = item.get("name"), "Nothing was done.", True
            log.debug("tool: %s %s", name, arguments)
            if telling:
                result, answer = "Nothing was done: an [Assistant] message is only words to say aloud.", not spoke
            elif name == "sleep":
                self.reason = "asked to sleep"
                return
            elif name == "delegate":
                text = str(arguments.get("request") or "").strip()
                if text:
                    result, taken = await self.hand(text, bool(arguments.get("photo")))
                    answer = not (spoke and taken)   # it already said it is on it
            elif name == "look":
                result = await self.show()
            elif name in self.acts:
                result = self.move(name, arguments)
            await self.send({"type": "conversation.item.create", "item": {
                "type": "function_call_output", "call_id": item.get("call_id"), "output": result}})
            again = again or answer
        if again:
            await self.respond(telling)     # asked again to say the words, it still runs no tool
        else:
            await self.flush()

    async def hand(self, text, photo):
        body = {"text": text}
        try:
            if photo:
                body["jpeg"] = base64.b64encode(await asyncio.to_thread(self.body.photo)).decode()
            await asyncio.to_thread(self.link.call, "/v1/hand", body)
        except OSError as error:
            log.warning("hand-off failed: %s", error)
            return "The assistant could not be reached, so nothing was sent. Say so.", False
        self.handed += 1
        self.active = time.monotonic()
        self.waiting_until = self.active + self.grant["work_seconds"]
        return ("The assistant has the request and is working on it. Its answer will arrive as an [Assistant] "
                "message. If you have not said so yet, say you are on it, in a few words."), True

    def move(self, name, arguments):
        """Start one of the body's own tools without waiting for it: a dance takes
        seconds, and the voice goes on meanwhile."""
        if self.acting is not None and not self.acting.done():
            return "The body is still doing the last thing asked. Try again when it is done."

        async def act():
            try:
                await asyncio.to_thread(self.body.act, name, arguments)
            except Exception as error:      # a bad argument or a motor that refused: the talk goes on
                log.warning("%s failed: %s", name, error)

        self.acting = asyncio.create_task(act())
        return "Doing it now."

    async def show(self):
        try:
            jpeg = await asyncio.to_thread(self.body.photo)
        except OSError as error:
            return "The camera gave no picture: %s" % error
        await self.send({"type": "conversation.item.create", "item": {"type": "message", "role": "user", "content": [
            {"type": "input_text", "text": "[Camera] What the robot sees now."},
            {"type": "input_image", "image_url": "data:image/jpeg;base64," + base64.b64encode(jpeg).decode()}]}})
        return "The picture follows."

    async def respond(self, telling=False):
        self.busy, self.telling = True, telling
        await self.send({"type": "response.create"})

    async def flush(self):
        """Ask again for a response that was refused, or give the voice model the
        assistant's next words to say."""
        if self.busy:
            return
        if self.owed is not None:
            telling, self.owed = self.owed, None
            return await self.respond(telling)
        if not self.results:
            return
        await self.send({"type": "conversation.item.create", "item": {"type": "message", "role": "user", "content": [
            {"type": "input_text", "text": "[Assistant] " + self.results.pop(0)}]}})
        self.waiting_until = 0.0
        self.active = time.monotonic()
        await self.respond(telling=True)

    async def orders(self):
        """What the adapter sends while the robot is awake."""
        while True:
            item = await self.inbox.get()
            kind = item.get("kind")
            if kind == "say":
                self.results.append(item["text"])
                await self.flush()
            elif kind == "look":
                await send_frame(self.body, self.link, item)
            elif kind == "sleep":
                self.reason = "told to sleep"
                return

    async def watch(self):
        """Sleep after a stretch of silence, but not while an answer promised to the room may still come."""
        while self.reason is None:
            await asyncio.sleep(1)
            now = time.monotonic()
            if now - max(self.active, self.playing_until) > self.grant["idle_seconds"] and now > self.waiting_until:
                self.reason = "quiet"


async def send_frame(body, link, item):
    try:
        jpeg = await asyncio.to_thread(body.photo)
        await asyncio.to_thread(link.call, "/v1/frame", {"id": item.get("id"), "jpeg": base64.b64encode(jpeg).decode()})
    except OSError as error:
        log.warning("look failed: %s", error)


async def serve(body, link, stop):
    inbox = asyncio.Queue()
    state = {"robot": "asleep"}

    async def poll():
        cursor, epoch = 0, ""
        while True:
            try:
                reply = await asyncio.to_thread(
                    link.call, "/v1/next?after=%d&epoch=%s&state=%s" % (cursor, epoch, state["robot"]), None, POLL_TIMEOUT)
            except (OSError, ValueError) as error:
                log.info("adapter not reachable: %s", error)
                await asyncio.sleep(5)
                continue
            cursor, epoch = reply["cursor"], reply["epoch"]
            for item in reply["items"]:
                inbox.put_nowait(item)

    async def report(robot):
        state["robot"] = robot
        try:
            await asyncio.to_thread(link.call, "/v1/state", {"state": robot})
        except OSError:
            pass    # the next poll says it again

    async def touch():
        while not await asyncio.to_thread(body.touched):
            await asyncio.sleep(0.2)
        return {"kind": "wake"}

    async def stopped():
        await asyncio.to_thread(stop.wait)

    poller, ending = asyncio.create_task(poll()), asyncio.create_task(stopped())
    await asyncio.to_thread(body.sleep)
    try:
        while not stop.is_set():
            waiting = [asyncio.create_task(inbox.get()), asyncio.create_task(touch()), ending]
            done, _ = await asyncio.wait(waiting, return_when=asyncio.FIRST_COMPLETED)
            for task in waiting[:2]:
                task.cancel()
            if ending in done:
                break
            item = next(task.result() for task in waiting[:2] if task in done)
            for task in waiting[:2]:    # both fired at once: keep the adapter's item for the session
                if task in done and task.result() is not item:
                    inbox.put_nowait(task.result())
            if item.get("kind") == "look":
                await send_frame(body, link, item)
            elif item.get("kind") in ("wake", "say"):
                try:
                    grant = await asyncio.to_thread(link.call, "/v1/session", {})
                except OSError as error:
                    log.warning("no voice session: %s", error)
                    continue
                await report("awake")
                log.info("awake: %s", item.get("kind"))
                voice = Voice(body, link, inbox, grant)
                session = asyncio.create_task(voice.run(item))
                await asyncio.wait([session, ending], return_when=asyncio.FIRST_COMPLETED)
                if not session.done():
                    session.cancel()
                    await asyncio.gather(session, return_exceptions=True)
                else:
                    log.info("asleep: %s (spoke %.0f s, handed over %d)", session.exception() or session.result(),
                             voice.spoken, voice.handed)
                await report("asleep")
    finally:
        poller.cancel()


class OppaBody(ReachyMiniApp):
    """The Reachy Mini app the robot's dashboard starts."""

    def run(self, reachy_mini: ReachyMini, stop_event: threading.Event) -> None:
        config = json.loads(CONFIG.read_text())
        # "debug": true also logs what was heard and said, which is the room's own talk.
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
        log.setLevel(logging.DEBUG if config.get("debug") else logging.INFO)
        asyncio.run(serve(Body(reachy_mini), Link(config["oppa"], config["token"]), stop_event))


def main():
    app = OppaBody()
    try:
        app.wrapped_run()
    except KeyboardInterrupt:
        app.stop()


if __name__ == "__main__":
    main()
