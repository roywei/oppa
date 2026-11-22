"""OPPA's body on a Reachy Mini: the microphone, speaker, camera and motors, and
nothing that thinks.

Asleep, the app holds no voice session and records nothing. It waits on a long
poll to the OPPA channel adapter on the owner's Mac (resources/adapters/reachy.py)
and watches its own antennas: the adapter can wake it, ask for one camera frame,
or give it words to say, and a person can wake it by moving an antenna.

Awake, it keeps one full-duplex session open with the voice provider, with a
short-lived key and a session set-up that both come from the adapter. The voice
model's three tools run here: `delegate` hands a request to the adapter,
`look` shows the model a camera frame, `sleep` ends the session. The robot also
sleeps by itself after a stretch of silence, so a forgotten session stops costing.

Configuration is one file, ~/.config/oppa-body.json:
    {"oppa": "http://the-mac.local:8767", "token": "<the adapter's token>"}
"""

import asyncio
import base64
import json
import logging
import re
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
WORDS = re.compile(r"[^\W_]")

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
        self.awake = True

    def sleep(self):
        mini = self.mini
        self.awake = False
        mini.stop_head_tracking()
        mini.disable_wobbling()
        mini.media.stop_recording()     # asleep, nothing is recorded
        mini.media.stop_playing()
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
        """One camera frame. Asleep, the head is down, so it lifts for the picture."""
        lifted = not self.awake
        if lifted:
            self.mini.enable_motors()
            self.mini.wake_up()
            time.sleep(0.6)
        try:
            for _ in range(20):
                jpeg = self.mini.media.get_frame_jpeg()
                if jpeg:
                    return bytes(jpeg)
                time.sleep(0.1)
            raise OSError("the camera gave no picture")
        finally:
            if lifted:
                self.mini.goto_sleep()
                self.mini.disable_motors()
                self.antennas = None

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
    """One voice session, from waking to sleeping. One response runs at a time:
    `owed` means a response we asked for was refused because another was
    running, so it is asked for again when that one ends."""

    def __init__(self, body, link, inbox, grant):
        self.body, self.link, self.inbox, self.grant = body, link, inbox, grant
        self.ws = None
        self.busy = self.owed = self.telling = self.mute = False
        self.heard = ""
        self.results = []           # words from the assistant, waiting to be said
        self.promised = 0           # requests handed over and not yet answered
        self.active = time.monotonic()
        self.playing_until = 0.0
        self.reason = None          # why the session ends
        self.spoken = 0.0           # seconds of speech played, for the log
        self.handed = 0

    async def send(self, event):
        await self.ws.send(json.dumps(event))

    async def run(self, first):
        loop = asyncio.get_running_loop()
        self.ws = await websockets.connect(
            self.grant["url"], additional_headers={"Authorization": "Bearer " + self.grant["secret"]}, max_size=None)
        try:
            await self.send({"type": "session.update", "session": self.grant["session"]})
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
                self.heard = ""
                self.playing_until = 0.0
                self.active = time.monotonic()
            elif kind == "input_audio_buffer.speech_stopped":
                self.active = time.monotonic()
            elif kind == "conversation.item.input_audio_transcription.completed":
                text = (event.get("transcript") or "").strip()
                self.heard = text if WORDS.search(text) else ""
                log.debug("heard: %s", text)
            elif kind == "response.output_audio_transcript.done":
                log.debug("said: %s", event.get("transcript"))
            elif kind == "response.created":
                self.busy = True
                self.mute = False
            elif kind == "response.done":
                await self.finished(event.get("response") or {})
            elif kind == "error":
                message = (event.get("error") or {}).get("message") or "the voice provider reported an error"
                if "active response" in message or "ongoing" in message:
                    self.owed = True
                else:
                    log.warning("voice provider: %s", message)
        self.reason = self.reason or "the voice provider closed the line"

    async def finished(self, response):
        """A response ended: run the tools it called, then say what is waiting."""
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
                text = self.heard or str(arguments.get("request") or "").strip()
                if text:
                    result = await self.hand(text, bool(arguments.get("photo")))
                    answer = not (spoke and self.promised)   # it already said it is on it
            elif name == "look":
                result = await self.show()
            await self.send({"type": "conversation.item.create", "item": {
                "type": "function_call_output", "call_id": item.get("call_id"), "output": result}})
            again = again or answer
        self.heard = ""
        if again:
            await self.respond()
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
            return "The assistant could not be reached, so nothing was sent. Say so."
        self.promised += 1
        self.handed += 1
        self.active = time.monotonic()
        return ("The assistant has the request and is working on it. Its answer will arrive as an [Assistant] "
                "message. If you have not said so yet, say you are on it, in a few words.")

    async def show(self):
        try:
            jpeg = await asyncio.to_thread(self.body.photo)
        except OSError as error:
            return "The camera gave no picture: %s" % error
        await self.send({"type": "conversation.item.create", "item": {"type": "message", "role": "user", "content": [
            {"type": "input_text", "text": "[Camera] What the robot sees now."},
            {"type": "input_image", "image_url": "data:image/jpeg;base64," + base64.b64encode(jpeg).decode()}]}})
        return "The picture follows."

    async def respond(self):
        self.busy, self.owed = True, False
        await self.send({"type": "response.create"})

    async def flush(self):
        """Give the voice model the next words from the assistant to say."""
        if self.busy:
            return
        if not self.owed:
            if not self.results:
                return
            await self.send({"type": "conversation.item.create", "item": {"type": "message", "role": "user", "content": [
                {"type": "input_text", "text": "[Assistant] " + self.results.pop(0)}]}})
            self.promised = max(0, self.promised - 1)
        self.telling = True
        self.active = time.monotonic()
        await self.respond()

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
        """Sleep after a stretch of silence; wait longer while an answer is owed to the room."""
        while self.reason is None:
            await asyncio.sleep(1)
            quiet = time.monotonic() - max(self.active, self.playing_until)
            if quiet > (self.grant["work_seconds"] if self.promised else self.grant["idle_seconds"]):
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
