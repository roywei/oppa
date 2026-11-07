"""iMessage channel adapter: OPPA channel protocol v1 over a long-lived `imsg rpc`.

This process translates; it never reads message meaning. It reports what imsg
observed and what a send proved:

- a message GUID from imsg, or the adapter's own echo of the sent text, is a receipt;
- a failure imsg proves was never dispatched is `not sent` (code 1001);
- everything else, including `may_have_completed` and `still_in_flight`, stays unknown.

The watch cursor (a chat.db ROWID) persists in the adapter's state directory. A
restart replays a small overlap after the saved cursor; the host logs each
native message once. A replaced Messages database invalidates the cursor.

An edit changes a row in place, which imsg's watch never reports, so chats with
a message forwarded in the last 20 minutes are read again every few seconds, and
a forwarded message whose text changed is forwarded again under its GUID. The
host records that as an edit, as it does a changed row in a restart's overlap.

Tapbacks (`react`) and the typing bubble (`typing`) need imsg's bridge: a helper
inside Messages, which runs only with SIP off and Messages started by `imsg
launch`. The adapter claims them when the helper runs or it may start it
(--launch-bridge with SIP off, once per boot); each call checks the helper again
and is refused as not sent while it is down. Text works the same either way. Only
the six classic tapbacks: imsg cannot send a custom emoji one. A tapback is a
receipt only once history shows a new reaction of ours of that kind on the message.

Usage: imessage.py --state DIR [--imsg PATH] [--db PATH] [--overlap N] [--edit-poll SECONDS] [--launch-bridge]
Requires imsg 0.15 or later and Full Disk Access for the process running it.
"""

import argparse
import collections
import json
import math
import os
import subprocess
import sys
import tempfile
import threading
import time

PROTOCOL_VERSION = 1
NOT_SENT = 1001
UNVERIFIED = 1002
ECHO_WAIT_SECONDS = 8.0
IMSG_CALL_SECONDS = 120.0
MAX_ID = 4096
MAX_BODY = 256 * 1024
MAX_MEMBERS = 64
# Messages allows an edit for 15 minutes after sending.
EDIT_WINDOW_SECONDS = 20 * 60
# At most this many of a chat's newest rows are read to find a remembered message's edit.
HISTORY_ROWS = 800
# The tapbacks imsg's bridge sends.
TAPBACKS = ("love", "like", "dislike", "laugh", "emphasize", "question")
# History names a sent "emphasize" as "emphasis".
READ_AS = {"emphasize": "emphasis"}
# imsg reports these before any dispatch: invalid params, database unavailable.
IMSG_NOT_DISPATCHED = (-32602, -32002)


class ImsgError(Exception):
    def __init__(self, error):
        super().__init__(error.get("message", "imsg error"))
        self.code = error.get("code", -32603)
        self.data = error.get("data") if isinstance(error.get("data"), dict) else {}


class Imsg:
    """One `imsg rpc` child. Responses are matched by id; notifications go to a handler."""

    def __init__(self, command, on_notification, on_exit):
        self.process = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
        )
        self.lock = threading.Lock()
        self.pending = {}
        self.next_id = 1
        self.on_notification = on_notification
        self.on_exit = on_exit
        threading.Thread(target=self.read, daemon=True).start()

    def call(self, method, params=None, timeout=IMSG_CALL_SECONDS):
        done = threading.Event()
        slot = {}
        with self.lock:
            request_id = self.next_id
            self.next_id += 1
            self.pending[request_id] = (done, slot)
            frame = {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
            try:
                self.process.stdin.write((json.dumps(frame) + "\n").encode())
                self.process.stdin.flush()
            except OSError as error:
                self.pending.pop(request_id, None)
                raise ImsgError({"code": -32603, "message": f"imsg stdin failed: {error}"})
        if not done.wait(timeout):
            with self.lock:
                self.pending.pop(request_id, None)
            raise TimeoutError(f"imsg {method} timed out")
        if "error" in slot:
            raise ImsgError(slot["error"])
        return slot.get("result")

    def read(self):
        for line in self.process.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if "id" in message and message["id"] is not None and "method" not in message:
                with self.lock:
                    waiter = self.pending.pop(message["id"], None)
                if waiter:
                    done, slot = waiter
                    if "error" in message:
                        slot["error"] = message["error"] if isinstance(message["error"], dict) else {}
                    else:
                        slot["result"] = message.get("result")
                    done.set()
            elif "method" in message:
                self.on_notification(message["method"], message.get("params") or {})
        with self.lock:
            for done, slot in self.pending.values():
                slot["error"] = {"code": -32603, "message": "imsg rpc ended"}
                done.set()
            self.pending.clear()
        self.on_exit()

    def close(self):
        try:
            self.process.stdin.close()
        except OSError:
            pass


class State:
    """Durable watch cursor, scoped to one Messages database file."""

    def __init__(self, directory, database):
        self.path = os.path.join(directory, "imessage-cursor.json")
        os.makedirs(directory, mode=0o700, exist_ok=True)
        try:
            stat = os.stat(database)
            self.database = f"{stat.st_dev}:{stat.st_ino}"
        except OSError:
            self.database = None
        self.cursor = None
        self.floor = None
        self.lock = threading.Lock()
        try:
            with open(self.path) as handle:
                saved = json.load(handle)
            if saved.get("database") == self.database:
                self.cursor = int(saved["cursor"])
                self.floor = int(saved["floor"])
        except (OSError, ValueError, KeyError, TypeError):
            pass

    def resume_after(self, overlap):
        if self.cursor is None:
            return 0  # the current tail: never answer history from before first start
        return max(self.floor, self.cursor - overlap, 1)

    def advance(self, rowid):
        with self.lock:
            if self.floor is None:
                self.floor = rowid - 1
            if self.cursor is not None and rowid <= self.cursor:
                return
            self.cursor = rowid
            data = json.dumps({"database": self.database, "cursor": self.cursor, "floor": self.floor})
            handle, temporary = tempfile.mkstemp(dir=os.path.dirname(self.path))
            with os.fdopen(handle, "w") as out:
                out.write(data)
                out.flush()
                os.fsync(out.fileno())
            os.replace(temporary, self.path)


class Adapter:
    def __init__(self, options):
        self.options = options
        database = options.db or os.path.expanduser("~/Library/Messages/chat.db")
        self.state = State(options.state, database)
        self.out_lock = threading.Lock()
        self.flow = threading.Condition()
        # Chats with a send in flight hold the account's own messages until the
        # send resolves, so an echo is matched to its send instead of logged twice.
        self.sending = collections.Counter()
        self.held = []
        self.own = collections.deque(maxlen=512)
        self.subscription = None
        # imsg may emit a subscription's first rows before its id reaches us.
        self.subscribing = False
        self.early = []
        self.closing = False
        # GUID -> (chat_id, body, when) of messages forwarded within the edit window.
        self.recent = {}
        self.reread_error = None
        # Chat GUID -> chat.db row id, which imsg's history reads by.
        self.chat_ids = {}
        self.tapbacks = self.typing_ok = False
        self.bridge_up = None
        # Cleared while `imsg launch` restarts Messages.
        self.quiet = threading.Event()
        self.quiet.set()
        command = [options.imsg, "rpc"] + (["--db", options.db] if options.db else [])
        self.imsg = Imsg(command, self.notification, self.imsg_exited)

    # Host side -----------------------------------------------------------------

    def emit(self, frame):
        line = json.dumps(frame, ensure_ascii=False) + "\n"
        with self.out_lock:
            sys.stdout.write(line)
            sys.stdout.flush()

    def reply(self, request_id, result=None, error=None):
        frame = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            frame["error"] = error
        else:
            frame["result"] = result
        self.emit(frame)

    def serve(self):
        for line in sys.stdin:
            try:
                request = json.loads(line)
            except ValueError:
                continue
            method, request_id = request.get("method"), request.get("id")
            params = request.get("params") or {}
            if method == "shutdown":
                break
            handler = {"initialize": self.initialize, "send": self.send, "react": self.react, "typing": self.typing,
                       "health": self.health}.get(method)
            if handler is None:
                if request_id is not None:
                    self.reply(request_id, error={"code": -32601, "message": "unknown method"})
                continue
            threading.Thread(target=self.handle, args=(handler, request_id, params), daemon=True).start()
        self.closing = True
        self.imsg.close()
        self.imsg.process.wait()

    def handle(self, handler, request_id, params):
        try:
            result = handler(params)
        except AdapterFailure as failure:
            self.reply(request_id, error={"code": failure.code, "message": failure.message[:1024]})
        except Exception as error:  # everything unexpected leaves delivery unknown
            self.reply(request_id, error={"code": -32603, "message": str(error)[:1024]})
        else:
            if request_id is not None:
                self.reply(request_id, result)

    def initialize(self, params):
        if params.get("protocol_version") != PROTOCOL_VERSION:
            raise AdapterFailure(-32602, "unsupported protocol version")
        status = self.imsg.call("status")
        database = status.get("database") or {}
        if not database.get("ready"):
            raise AdapterFailure(-32603, "Messages database unavailable; grant Full Disk Access to the process running oppa serve")
        missing = {"send", "watch.subscribe"} - set(status.get("methods") or [])
        if missing:
            raise AdapterFailure(-32603, "imsg is missing " + ", ".join(sorted(missing)) + "; install imsg 0.15 or later")
        status = self.bridge(status)
        self.subscribe(self.state.resume_after(self.options.overlap))
        if "messages.history" in (status.get("methods") or []):
            threading.Thread(target=self.watch_edits, daemon=True).start()
        else:
            self.reread_error = "imsg has no messages.history; edits are seen only after a restart"
        return {"protocol_version": PROTOCOL_VERSION,
                "capabilities": {"attachments": True, "receipts": True, "threads": False, "reactions": self.tapbacks, "typing": self.typing_ok}}

    def bridge(self, status):
        """Tapbacks and typing go through imsg's helper inside Messages, which needs
        SIP off and Messages started by `imsg launch`. The channel claims them when
        the helper runs or this adapter may start it; each call checks it again."""
        methods = set(status.get("methods") or [])
        self.observe_bridge(status)
        possible = self.bridge_up is not None or (self.options.launch_bridge and sip_disabled())
        self.tapbacks = possible and {"tapback", "messages.history"} <= methods
        self.typing_ok = possible and "typing" in methods
        if self.tapbacks or self.typing_ok:
            threading.Thread(target=self.watch_bridge, daemon=True).start()
        return status

    def observe_bridge(self, status):
        """What the helper can do now: its selectors when ready, else None."""
        selectors = (status.get("bridge") or {}).get("selectors")
        self.bridge_up = (selectors if isinstance(selectors, dict) else {}) if bridge_ready(status) else None

    def watch_bridge(self):
        """Start the helper when it is down, at most once per boot, since starting it
        restarts Messages and a Mac that rejects it would otherwise do so on every
        start; then read its state every half minute."""
        while not self.closing:
            if self.bridge_up is None and self.options.launch_bridge and self.first_launch_this_boot():
                # Messages is down while it restarts, so sends wait for it.
                self.quiet.clear()
                try:
                    subprocess.run([self.options.imsg, "launch"], stdin=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
                except (OSError, subprocess.SubprocessError):
                    pass
                time.sleep(5)
                self.quiet.set()
            try:
                self.observe_bridge(self.imsg.call("status", timeout=10))
            except Exception:
                self.bridge_up = None
            time.sleep(self.options.bridge_poll)

    def first_launch_this_boot(self):
        if not sip_disabled():
            return False
        try:
            boot = subprocess.run(["/usr/sbin/sysctl", "-n", "kern.boottime"], capture_output=True,
                                  text=True, timeout=10).stdout.strip()
        except (OSError, subprocess.SubprocessError):
            return False
        marker = os.path.join(self.options.state, "bridge-launched")
        try:
            with open(marker) as handle:
                if handle.read() == boot:
                    return False
        except OSError:
            pass
        try:
            with open(marker, "w") as handle:
                handle.write(boot)
        except OSError:
            return False
        return True

    def health(self, _params):
        status = self.imsg.call("status", timeout=10)
        database = status.get("database") or {}
        return {"ok": bool(database.get("ready")) and self.subscription is not None,
                "imsg_version": status.get("version"), "cursor": self.state.cursor,
                "edits": self.reread_error or "watched",
                "bridge": "ready" if self.bridge_up is not None else "off: tapbacks and typing are not sent"}

    def send(self, params):
        chat = params.get("conversation")
        body = params.get("body") or ""
        files = [a.get("path") for a in params.get("attachments") or [] if isinstance(a, dict) and a.get("path")]
        if not isinstance(chat, str) or not chat or (not body and not files):
            raise AdapterFailure(NOT_SENT, "nothing to send or no conversation")
        if not self.quiet.wait(30):
            raise AdapterFailure(NOT_SENT, "Messages is restarting with imsg's helper")
        with self.flow:
            self.sending[chat] += 1
        receipt, sent_any = None, False
        try:
            parts = ([{"text": body}] if body else []) + [{"file": path} for path in files]
            for part in parts:
                try:
                    result = self.imsg.call("send", dict(chat_guid=chat, **part))
                except ImsgError as error:
                    if not sent_any and (error.data.get("disposition") == "not_started" or error.code in IMSG_NOT_DISPATCHED):
                        raise AdapterFailure(NOT_SENT, f"imsg did not dispatch the send: {error}")
                    raise AdapterFailure(UNVERIFIED, f"imsg send outcome unknown: {error}")
                except TimeoutError as error:
                    raise AdapterFailure(UNVERIFIED, f"imsg send outcome unknown: {error}")
                sent_any = True
                guid = (result or {}).get("guid")
                if guid:
                    self.own.append(guid)
                elif "text" in part:
                    guid = self.await_echo(chat, part["text"])
                receipt = receipt or guid
            if not receipt:
                raise AdapterFailure(UNVERIFIED, "Messages accepted the send but no message id was observed")
            return {"native_id": receipt}
        finally:
            with self.flow:
                self.sending[chat] -= 1
                if self.sending[chat] <= 0:
                    del self.sending[chat]
                self.release_held()

    def react(self, params):
        """A tapback through imsg's bridge, confirmed only when Messages shows a new
        reaction of ours of that kind on the message. imsg's answer alone is no receipt."""
        chat, target, kind = params.get("conversation"), params.get("message"), params.get("reaction")
        if not (bounded(chat) and bounded(target)) or kind not in TAPBACKS:
            raise AdapterFailure(NOT_SENT, "no conversation, message or known reaction")
        if not (self.bridge_up or {}).get("sendReaction"):
            raise AdapterFailure(NOT_SENT, "imsg's bridge is not running, so Messages takes no tapbacks now")
        chat_id = self.chat_id(chat)
        before = self.own_reactions(chat_id, target) if chat_id is not None else None
        if before is None:
            raise AdapterFailure(NOT_SENT, "the message is not in this chat's recent history")
        try:
            self.imsg.call("tapback", {"chat_guid": chat, "message_guid": target, "kind": kind})
        except ImsgError as error:
            if error.data.get("disposition") == "not_started" or error.code in IMSG_NOT_DISPATCHED:
                raise AdapterFailure(NOT_SENT, f"imsg did not dispatch the tapback: {error}")
            raise AdapterFailure(UNVERIFIED, f"imsg tapback outcome unknown: {error}")
        except TimeoutError as error:
            raise AdapterFailure(UNVERIFIED, f"imsg tapback outcome unknown: {error}")
        deadline = time.monotonic() + ECHO_WAIT_SECONDS
        while True:
            try:
                now = self.own_reactions(chat_id, target) or {}
            except Exception:
                now = {}
            new = [rowid for rowid, seen in now.items() if seen == READ_AS.get(kind, kind) and rowid not in before]
            if new:
                return {"native_id": f"reaction:{max(new)}"}
            if time.monotonic() >= deadline:
                raise AdapterFailure(UNVERIFIED, "Messages took the tapback but it was not seen on the message")
            time.sleep(0.25)

    def own_reactions(self, chat_id, target):
        """{reaction row id: kind} of the account's own reactions on `target`, or None
        when the message is not in view."""
        for message in self.history(chat_id, {target}):
            if message.get("guid") == target:
                return {r["id"]: r.get("type") for r in message.get("reactions") or []
                        if isinstance(r, dict) and r.get("is_from_me") and isinstance(r.get("id"), int)}
        return None

    def chat_id(self, chat):
        with self.flow:
            known = self.chat_ids.get(chat)
        if known is not None:
            return known
        result = self.imsg.call("chats.list", {"limit": 500}, timeout=30)
        for row in (result or {}).get("chats") or []:
            if isinstance(row, dict) and isinstance(row.get("id"), int):
                with self.flow:
                    self.chat_ids[row.get("guid")] = row["id"]
                    self.chat_ids[row.get("identifier")] = row["id"]
        with self.flow:
            return self.chat_ids.get(chat)

    def typing(self, params):
        """The typing bubble, while the host composes a reply. Best effort: a
        failure only means no bubble."""
        chat = params.get("conversation")
        if not bounded(chat) or not (self.bridge_up or {}).get("typing"):
            raise AdapterFailure(NOT_SENT, "no conversation, or imsg's bridge is not running")
        self.imsg.call("typing", {"chat_guid": chat, "typing": params.get("on") is True}, timeout=5)
        return {}

    def await_echo(self, chat, text):
        deadline = time.monotonic() + ECHO_WAIT_SECONDS
        with self.flow:
            while True:
                for index, message in enumerate(self.held):
                    if message.get("chat_guid") == chat and message.get("text") == text and message.get("guid") not in self.own:
                        self.own.append(message["guid"])
                        return message["guid"]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self.flow.wait(remaining)

    # imsg side -----------------------------------------------------------------

    def subscribe(self, since_rowid):
        params = {"attachments": True, "include_reactions": False}
        if since_rowid:
            params["since_rowid"] = since_rowid
        with self.flow:
            self.subscribing = True
            self.subscription = None
        try:
            result = self.imsg.call("watch.subscribe", params)
        finally:
            with self.flow:
                self.subscribing = False
                early, self.early = self.early, []
        with self.flow:
            self.subscription = result.get("subscription")
        for method, early_params in early:
            self.notification(method, early_params)

    def notification(self, method, params):
        with self.flow:
            if self.subscribing:
                self.early.append((method, params))
                return
        if method == "message" and params.get("subscription") == self.subscription:
            message = params.get("message") or {}
            with self.flow:
                if message.get("is_from_me") and message.get("guid") in self.own:
                    self.advance(message)
                    return
                if message.get("is_from_me") and message.get("chat_guid") in self.sending:
                    self.held.append(message)
                    self.flow.notify_all()
                    return
            self.forward(message)
        elif method == "watch.overflow" and params.get("subscription") == self.subscription:
            resume = params.get("resume_after_rowid")
            threading.Thread(target=self.resubscribe, args=(resume,), daemon=True).start()

    def resubscribe(self, since_rowid):
        try:
            self.subscribe(since_rowid)
        except Exception:
            # A deaf adapter must not look healthy; exiting stops serve visibly.
            os._exit(3)

    def release_held(self):
        # Caller holds self.flow.
        keep = []
        for message in self.held:
            if message.get("chat_guid") in self.sending:
                keep.append(message)
            elif message.get("guid") in self.own:
                self.advance(message)
            else:
                self.forward(message)
        self.held = keep

    def advance(self, message):
        if isinstance(message.get("id"), int):
            self.state.advance(message["id"])

    def forward(self, message):
        event = normalize(message)
        if event is not None:
            with self.flow:
                self.publish(message, event)
        self.advance(message)

    def publish(self, message, event):
        # Caller holds self.flow, so what `recent` holds is always the text sent last.
        if isinstance(message.get("chat_id"), int):
            self.chat_ids[event["conversation"]["native_id"]] = message["chat_id"]
        self.emit({"jsonrpc": "2.0", "method": "event", "params": event})
        if not message.get("is_from_me") and isinstance(message.get("chat_id"), int):
            # The conversation it was filed under, so a re-read row filed elsewhere is never its edit.
            self.recent[event["native_id"]] = (message["chat_id"], event["body"], time.monotonic(),
                                               event["conversation"]["native_id"])

    def watch_edits(self):
        while not self.closing:
            time.sleep(self.options.edit_poll)
            self.reread()

    def reread(self):
        """Read each chat with a remembered message again, and forward those whose text changed."""
        with self.flow:
            now = time.monotonic()
            self.recent = {guid: seen for guid, seen in self.recent.items() if now - seen[2] < EDIT_WINDOW_SECONDS}
            chats = collections.defaultdict(dict)
            for guid, seen in self.recent.items():
                chats[seen[0]][guid] = seen
        failures = []
        for chat, before in sorted(chats.items()):
            try:
                messages = self.history(chat, set(before))
            except Exception as error:
                failures.append(f"chat {chat}: {error}")
                continue
            found = {message.get("guid") for message in messages}
            with self.flow:
                # No longer in the chat (deleted, unsent, or past what is read): not looked for again.
                for guid, seen in before.items():
                    if guid not in found and self.recent.get(guid) is seen:
                        del self.recent[guid]
            for message in messages:
                event = normalize(message)
                guid = event and event["native_id"]
                with self.flow:
                    # A message sent again while this read was in flight may be newer
                    # than the read; the next pass reads it again.
                    seen = self.recent.get(guid)
                    if (seen is not None and seen is before.get(guid) and event["body"] != seen[1]
                            and event["conversation"]["native_id"] == seen[3]):
                        self.publish(message, event)
        self.reread_error = ("reading chats again failed: " + "; ".join(failures))[:1024] if failures else None

    def history(self, chat, guids):
        # Newest first: read further back until every remembered message is in view, the chat ends, or
        # HISTORY_ROWS rows are in view.
        limit = 50
        while True:
            result = self.imsg.call("messages.history", {"chat_id": chat, "limit": limit, "attachments": True}, timeout=30)
            messages = (result or {}).get("messages") or []
            if len(messages) < limit or guids <= {message.get("guid") for message in messages} or limit >= HISTORY_ROWS:
                return messages
            limit = min(limit * 4, HISTORY_ROWS)

    def imsg_exited(self):
        # Without imsg nothing can be observed or sent; ending stdout stops the host.
        if not self.closing:
            os._exit(3)


class AdapterFailure(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def bounded(value, limit=MAX_ID):
    return value if isinstance(value, str) and value.strip() and len(value) <= limit and "\0" not in value else None


def normalize(message):
    """Map one imsg message to a protocol v1 event, or None when it carries nothing."""
    if message.get("is_reaction"):
        return None
    chat = bounded(message.get("chat_guid")) or bounded(message.get("chat_identifier"))
    guid = bounded(message.get("guid"))
    if chat is None or guid is None:
        return None
    is_me = bool(message.get("is_from_me"))
    handle = bounded(message.get("sender")) or ("me" if is_me else None)
    if handle is None:
        return None
    body = message.get("text") if isinstance(message.get("text"), str) else ""
    body = body.replace("\0", "")[:MAX_BODY]
    attachments = []
    for item in message.get("attachments") or []:
        if not isinstance(item, dict) or item.get("missing") or item.get("is_sticker"):
            continue
        path = bounded(item.get("converted_path")) or bounded(item.get("original_path"))
        if path is None:
            continue
        attachments.append({"path": path,
                            "mime_type": item.get("converted_mime_type") or item.get("mime_type") or None,
                            "name": bounded(item.get("transfer_name"))})
    if not body.strip() and not attachments:
        return None
    # A reply written for a direct conversation must never land in a group, so
    # a chat is direct only when nothing suggests a group: no ";+;" marker, and
    # either imsg's flag says not a group or, without a flag, the id carries the
    # one-to-one ";-;" marker. imsg 0.15 always sets the flag from the GUID, so
    # this guards other producers and future changes.
    flag = message.get("is_group")
    direct = ";+;" not in chat and (flag is False or (flag is None and ";-;" in chat))
    group = not direct
    conversation = {"native_id": chat, "kind": "group" if group else "dm"}
    title = bounded(message.get("chat_name"))
    if group and title:
        conversation["title"] = title
    # imsg lists a chat's other participants with every message; the host
    # names each one in a group turn, so who "my wife" is needs no guess.
    if group and isinstance(message.get("participants"), list):
        members = []
        for participant in message["participants"]:
            participant = bounded(participant)
            if participant is not None and participant not in members:
                members.append(participant)
        conversation["members"] = members[:MAX_MEMBERS]
    sender = {"handle": handle, "is_me": is_me}
    if bounded(message.get("sender_name")):
        sender["display_name"] = message["sender_name"]
    event = {"conversation": conversation, "native_id": guid, "sender": sender,
             "body": body, "attachments": attachments[:64]}
    if bounded(message.get("reply_to_guid")):
        event["reply_to"] = message["reply_to_guid"]
    if bounded(message.get("created_at")):
        event["at"] = message["created_at"]
    return event


def bridge_ready(status):
    bridge = status.get("bridge") or {}
    return bridge.get("v2_ready") is True or (bridge.get("ready") is True and "v2_ready" not in bridge)


def sip_disabled():
    try:
        out = subprocess.run(["/usr/bin/csrutil", "status"], capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.SubprocessError):
        return False
    return "status: disabled" in out


def interval(text):
    """Seconds between edit reads: a finite number above zero, checked before the adapter starts, since a
    bad one would stop the reading thread while health still reported edits as watched."""
    try:
        value = float(text)
    except ValueError:
        value = math.nan
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be a number of seconds above 0, not %r" % text)
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True, help="private directory for the watch cursor")
    parser.add_argument("--imsg", default="imsg", help="imsg 0.15+ executable")
    parser.add_argument("--db", help="Messages database (defaults to imsg's own default)")
    parser.add_argument("--overlap", type=int, default=20, help="ROWIDs replayed after a restart")
    parser.add_argument("--launch-bridge", action="store_true",
                        help="with SIP off, start imsg's Messages helper when it is down, restarting Messages")
    parser.add_argument("--bridge-poll", type=interval, default=30.0, help="seconds between reads of the bridge's state")
    parser.add_argument("--edit-poll", type=interval, default=5.0, help="seconds between reads of recently active chats for edits")
    Adapter(parser.parse_args()).serve()


if __name__ == "__main__":
    main()
