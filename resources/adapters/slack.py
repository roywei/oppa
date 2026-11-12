"""Slack channel adapter: OPPA channel protocol v1 over Slack's Socket Mode and Web API.

One Slack app the owner creates from the manifest link `--login-link` prints carries three tokens:

- the bot token (xoxb-) receives and answers: the owner's direct chat with the app, and every channel the
  app is invited to, such as a room the owner shares with other agents;
- the app-level token (xapp-) opens the Socket Mode connection, so nothing on this Mac listens for Slack;
- the user token (xoxp-, only users:read) names the owner, whose handle the login registers.

`ask` hands the owner's other agents a request as this app and reads their replies. Agents skip app posts, so
they find these requests by checking the channel on a schedule the owner set up with each of them.

Who wrote a message is evidence the adapter reports, never something it reads from text: a message an app
posted (it carries a bot or app id) is that app's, `app:<app id>`, even when Slack shows it under the
owner's name, as with an assistant that posts with the owner's token; only a person's own message has
their user id as its handle. This app's posts, `ask`'s included, are the assistant's (`is_me`).

In a direct chat a reply goes into the conversation; in a channel it goes into the thread of the message it
answers, so a room stays readable. Text only: a file arrives as a bracketed note, and a send with
attachments is refused. A reply goes out as Markdown, which Slack renders (tables, code blocks, links); a
long one goes in parts that never cut a code block open, and no reply can page a room (slack_format.py).

`oppa channels add slack` logs in: --login-link prints the manifest link, each --login-token reads one
token from the clipboard (it never passes through a chat or a command line), checks it with Slack and keeps
it (0600), and once all three are kept prints the owner's handle; --login-activate puts them in use. Without
a login the adapter keeps running and says so in `health`.

Usage: slack.py --state DIR [--api-url URL] [--login-link [--name NAME] | --login-token [--clipboard CMD] |
                 --login-activate]
       slack.py --state DIR ask --channel NAME --text TEXT [--mention NAME]... [--thread TS] [--wait S] [--settle S]
       slack.py --state DIR replies --channel NAME --thread TS [--after TS] [--wait S] [--settle S]
"""

import argparse
import base64
import datetime
import hashlib
import html
import json
import os
import re
import shlex
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

import slack_format

PROTOCOL_VERSION = 1
NOT_SENT = 1001
UNVERIFIED = 1002
API_URL = "https://slack.com/api"
MAX_ID = 4096
MAX_BODY = 256 * 1024
REQUEST_SECONDS = 20.0
PING_SECONDS = 30.0
READ_SECONDS = 90.0
POLL_SECONDS = 5.0
ROOTS_KEPT = 5000
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
KINDS = {"xoxb-": "bot", "xapp-": "app", "xoxp-": "user"}
# Message subtypes that are someone saying something; joins, topic changes and the like are not.
SAID = {None, "thread_broadcast", "file_share", "bot_message", "me_message"}
MANIFEST_SCOPES = {
    "bot": ["chat:write", "im:history", "im:read", "channels:history", "channels:read", "groups:history",
            "groups:read", "mpim:history", "mpim:read", "users:read"],
    "user": ["users:read"],
}


class Refused(Exception):
    """Slack answered and did not take the request."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


class Api:
    def __init__(self, base_url, token):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def call(self, method, payload=None, timeout=REQUEST_SECONDS):
        body = json.dumps(payload or {}, ensure_ascii=False).encode()
        request = urllib.request.Request(f"{self.base_url}/{method}", data=body, method="POST", headers={
            "Content-Type": "application/json; charset=utf-8", "Authorization": "Bearer " + self.token})
        with OPENER.open(request, timeout=timeout) as response:
            result = json.loads(response.read() or b"{}")
        if not isinstance(result, dict):
            raise ValueError(f"Slack {method} answered with something other than an object")
        if not result.get("ok"):
            raise Refused(f"Slack {method} refused: {result.get('error') or 'not ok'}")
        return result

    def get(self, method, params, timeout=REQUEST_SECONDS):
        """A read method with form arguments; some reads take no JSON body."""
        request = urllib.request.Request(f"{self.base_url}/{method}?{urllib.parse.urlencode(params)}",
                                         headers={"Authorization": "Bearer " + self.token})
        with OPENER.open(request, timeout=timeout) as response:
            result = json.loads(response.read() or b"{}")
        if not isinstance(result, dict) or not result.get("ok"):
            raise Refused(f"Slack {method} refused: {(result or {}).get('error') or 'not ok'}")
        return result


class WebSocket:
    """The client side of RFC 6455, enough for Socket Mode: text frames, ping and pong, close."""

    def __init__(self, url):
        parts = urllib.parse.urlsplit(url)
        secure = parts.scheme == "wss"
        raw = socket.create_connection((parts.hostname, parts.port or (443 if secure else 80)), timeout=REQUEST_SECONDS)
        self.sock = ssl.create_default_context().wrap_socket(raw, server_hostname=parts.hostname) if secure else raw
        key = base64.b64encode(os.urandom(16)).decode()
        target = (parts.path or "/") + ("?" + parts.query if parts.query else "")
        self.sock.sendall((f"GET {target} HTTP/1.1\r\nHost: {parts.netloc}\r\nUpgrade: websocket\r\n"
                           f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        self.file = self.sock.makefile("rb")
        status = self.file.readline()
        headers = {}
        while True:
            line = self.file.readline()
            if line in (b"\r\n", b"\n", b""):
                break
            name, _, value = line.decode("latin-1").partition(":")
            headers[name.strip().lower()] = value.strip()
        accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
        if b" 101 " not in status or headers.get("sec-websocket-accept") != accept:
            self.sock.close()
            raise ConnectionError("Slack refused the Socket Mode upgrade")
        self.sock.settimeout(READ_SECONDS)
        self.lock = threading.Lock()

    def send(self, payload, opcode=1):
        data = payload.encode() if isinstance(payload, str) else payload
        size = len(data)
        header = bytes([0x80 | opcode])
        if size < 126:
            header += bytes([0x80 | size])
        elif size < 65536:
            header += bytes([0x80 | 126]) + size.to_bytes(2, "big")
        else:
            header += bytes([0x80 | 127]) + size.to_bytes(8, "big")
        mask = os.urandom(4)
        with self.lock:
            self.sock.sendall(header + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))

    def read(self, size):
        data = self.file.read(size)
        if data is None or len(data) < size:
            raise ConnectionError("the Socket Mode connection closed")
        return data

    def receive(self):
        """The next text message, or None once the server closes."""
        message = b""
        while True:
            first, second = self.read(2)
            size = second & 0x7F
            if size == 126:
                size = int.from_bytes(self.read(2), "big")
            elif size == 127:
                size = int.from_bytes(self.read(8), "big")
            mask = self.read(4) if second & 0x80 else None
            data = self.read(size)
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            opcode = first & 0x0F
            if opcode == 8:
                return None
            if opcode == 9:
                self.send(data, 10)
            elif opcode in (0, 1, 2):
                message += data
                if first & 0x80:
                    return message.decode("utf-8", "replace")

    def close(self):
        try:
            self.send(b"", 8)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class State:
    """The tokens and what Slack said about them, the names seen and the posts `ask` made, in one private
    directory."""

    def __init__(self, directory):
        self.directory = directory
        os.makedirs(directory, mode=0o700, exist_ok=True)

    def path(self, name):
        return os.path.join(self.directory, name)

    def read(self, name):
        try:
            with open(self.path(name)) as handle:
                value = json.load(handle)
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            return {}

    def write(self, name, value):
        handle, temporary = tempfile.mkstemp(dir=self.directory)
        with os.fdopen(handle, "w") as out:
            json.dump(value, out)
            out.flush()
            os.fsync(out.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, self.path(name))

    def remove(self, name):
        try:
            os.remove(self.path(name))
        except FileNotFoundError:
            pass


class Names:
    """Display names of people, bots and channels, asked of Slack once and kept for the process."""

    def __init__(self, api):
        self.api = api
        self.cache = {}

    def lookup(self, key, method, params, pick):
        if key not in self.cache:
            try:
                self.cache[key] = pick(self.api.get(method, params))
            except Exception:
                return None  # a name is a courtesy; a message without one is still delivered
        return self.cache[key]

    def user(self, user_id):
        def pick(result):
            user = result.get("user") or {}
            profile = user.get("profile") or {}
            return profile.get("display_name") or profile.get("real_name") or user.get("real_name") or user.get("name")
        return self.lookup("user:" + user_id, "users.info", {"user": user_id}, pick)

    def channel(self, channel_id):
        return self.lookup("channel:" + channel_id, "conversations.info", {"channel": channel_id},
                           lambda result: (result.get("channel") or {}).get("name"))


class Adapter:
    def __init__(self, options):
        self.options = options
        self.state = State(options.state)
        self.out_lock = threading.Lock()
        self.lock = threading.Lock()
        self.account = None
        self.bot = None
        self.names = None
        self.roots = {}
        self.tails = set()
        self.error = None
        self.connected = False
        self.skipped = 0
        self.unreadable = 0
        self.running = False
        self.closing = False

    # Host side -----------------------------------------------------------------

    def emit(self, frame):
        line = json.dumps(frame, ensure_ascii=False) + "\n"
        with self.out_lock:
            sys.stdout.write(line)
            sys.stdout.flush()

    def serve(self):
        for line in sys.stdin:
            try:
                request = json.loads(line)
            except ValueError:
                continue
            method, request_id = request.get("method"), request.get("id")
            if method == "shutdown":
                break
            handler = {"initialize": self.initialize, "send": self.send, "health": self.health}.get(method)
            if handler is None:
                if request_id is not None:
                    self.emit({"jsonrpc": "2.0", "id": request_id, "error": {"code": -32601, "message": "unknown method"}})
                continue
            threading.Thread(target=self.handle, args=(handler, request_id, request.get("params") or {}),
                             daemon=True).start()
        self.closing = True

    def handle(self, handler, request_id, params):
        try:
            frame = {"result": handler(params)}
        except AdapterFailure as failure:
            frame = {"error": {"code": failure.code, "message": failure.message[:1024]}}
        except Exception as error:  # everything unexpected leaves delivery unknown
            frame = {"error": {"code": -32603, "message": str(error)[:1024]}}
        if request_id is not None:
            self.emit(dict(frame, jsonrpc="2.0", id=request_id))

    def initialize(self, params):
        if params.get("protocol_version") != PROTOCOL_VERSION:
            raise AdapterFailure(-32602, "unsupported protocol version")
        with self.lock:
            start, self.running = not self.running, True
        if start:
            threading.Thread(target=self.listen, daemon=True).start()
        return {"protocol_version": PROTOCOL_VERSION,
                "capabilities": {"attachments": False, "receipts": True, "threads": True, "reactions": False, "typing": False}}

    def health(self, _params):
        account, _ = self.sync()
        status = {"ok": bool(account) and self.connected and self.error is None,
                  "login": "bound" if account else "needed", "connected": self.connected,
                  "skipped": self.skipped, "unreadable": self.unreadable}
        if account:
            status["team"] = account.get("team")
        if self.error:
            status["error"] = self.error
        if not account:
            status["error"] = "run oppa channels add slack and follow its steps"
        return status

    def send(self, params):
        channel = params.get("conversation")
        body = params.get("body") or ""
        if not isinstance(channel, str) or not channel or not body.strip():
            raise AdapterFailure(NOT_SENT, "nothing to send or no conversation")
        if params.get("attachments"):
            raise AdapterFailure(NOT_SENT, "the Slack adapter sends text only")
        _, bot = self.sync()
        if bot is None:
            raise AdapterFailure(NOT_SENT, "Slack is not logged in")
        reply_to = params.get("reply_to")
        thread = None
        if isinstance(reply_to, str) and reply_to:
            root = self.roots.get((channel, reply_to))
            # A direct chat answers in line unless the message was itself in a thread; a channel answers in the
            # message's thread.
            thread = root if root and root != reply_to else (None if channel.startswith("D") else root or reply_to)
        receipt = None
        for part in slack_format.parts(slack_format.quiet(body)):
            payload = {"channel": channel, "markdown_text": part, "unfurl_links": False}
            if thread:
                payload["thread_ts"] = thread
            try:
                result = bot.call("chat.postMessage", payload)
            except Exception as error:
                if receipt is None and refused(error):
                    raise AdapterFailure(NOT_SENT, f"Slack did not take the send: {error}")
                raise AdapterFailure(UNVERIFIED, f"Slack send outcome unknown: {error}")
            ts = result.get("ts")
            if not bounded(ts):
                raise AdapterFailure(UNVERIFIED, "Slack took the send without a message id")
            self.remember(channel, ts, thread or ts)
            if receipt is not None:
                self.tails.add((channel, ts))  # the host knows a reply by its first part's echo
            receipt = receipt or ts
        return {"native_id": receipt}

    # Slack side ----------------------------------------------------------------

    def sync(self):
        """The login saved last; a new one from --login-activate takes over at the next connection."""
        with self.lock:
            account = self.state.read("account.json")
            if account != (self.account or {}):
                self.account = account if all(account.get(kind) for kind in KINDS.values()) else None
                self.bot = self.account and Api(self.options.api_url, self.account["bot"])
                self.names = self.bot and Names(self.bot)
            return self.account, self.bot

    def listen(self):
        failures = 0
        while not self.closing:
            account, bot = self.sync()
            if account is None:
                self.connected = False
                time.sleep(1)
                continue
            try:
                url = Api(self.options.api_url, account["app"]).call("apps.connections.open")["url"]
                self.connect(url, account)
                failures, self.error = 0, None
            except Exception as error:  # nothing Slack sends may stop the adapter
                failures, self.error = failures + 1, f"Socket Mode: {type(error).__name__}: {error}"[:1024]
                self.connected = False
                time.sleep(30 if failures >= 3 else 2)

    def connect(self, url, account):
        socket_ = WebSocket(url)
        stop = threading.Event()

        def ping():
            while not stop.wait(PING_SECONDS):
                try:
                    socket_.send(b"", 9)
                except OSError:
                    return

        threading.Thread(target=ping, daemon=True).start()
        try:
            while not self.closing:
                text = socket_.receive()
                if text is None:
                    return
                envelope = json.loads(text)
                kind = envelope.get("type")
                if kind == "hello":
                    self.connected, self.error = True, None
                elif kind == "disconnect":
                    return  # Slack asks for a fresh connection now and then
                if envelope.get("envelope_id"):
                    socket_.send(json.dumps({"envelope_id": envelope["envelope_id"]}))
                if kind == "events_api":
                    event = (envelope.get("payload") or {}).get("event") or {}
                    try:
                        self.forward(event, account)
                    except Exception:  # one message the adapter cannot read never holds up the rest
                        self.unreadable += 1
                    if self.sync()[0] is not account:
                        return  # a new login takes over
        finally:
            stop.set()
            self.connected = False
            socket_.close()

    def remember(self, channel, ts, root):
        with self.lock:
            self.roots[(channel, ts)] = root
            while len(self.roots) > ROOTS_KEPT:
                self.roots.pop(next(iter(self.roots)))

    def forward(self, event, account):
        if event.get("type") != "message":
            return
        subtype = event.get("subtype")
        if subtype == "message_changed":
            message = dict(event.get("message") or {}, channel=event.get("channel"), channel_type=event.get("channel_type"))
            if (event.get("previous_message") or {}).get("text") == message.get("text"):
                return  # an unfurl or a reply count changed, not the text
        elif subtype in SAID:
            message = event
        else:
            self.skipped += 1
            return
        channel, ts = bounded(message.get("channel")), bounded(message.get("ts"))
        if channel is None or ts is None:
            return
        thread = bounded(message.get("thread_ts"))
        self.remember(channel, ts, thread or ts)
        sender = self.sender(message, account)
        if sender and sender["is_me"] and (channel, ts) in self.tails:
            return
        body = self.text(message)
        if sender is None or not body.strip():
            return
        direct = message.get("channel_type") == "im"
        conversation = {"native_id": channel, "kind": "dm" if direct else "group"}
        if not direct:
            name = self.names.channel(channel)
            if name:
                conversation["title"] = "#" + name
        event_out = {"conversation": conversation, "native_id": ts, "sender": sender, "body": body[:MAX_BODY],
                     "attachments": []}
        if thread and thread != ts:
            event_out["reply_to"] = thread
        try:
            at = datetime.datetime.fromtimestamp(float(ts), datetime.timezone.utc)
            event_out["at"] = at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        except ValueError:
            pass
        self.emit({"jsonrpc": "2.0", "method": "event", "params": event_out})

    def sender(self, message, account):
        user = bounded(message.get("user"))
        profile = message.get("bot_profile") or {}
        app = bounded(message.get("app_id") or profile.get("app_id"))
        bot = bounded(message.get("bot_id"))
        if (bot and bot == account.get("bot_id")) or (app and app == account.get("app_id")):
            return {"handle": user or bot, "is_me": True}
        if app or bot:
            handle = "app:" + app if app else "bot:" + bot
            name = profile.get("name") or (self.names.user(user) if user and user != account.get("owner") else None)
            if user == account.get("owner"):
                name = f"{name or 'an app'}, posting as the owner"
            return dict({"handle": handle, "is_me": False}, **({"display_name": name} if name else {}))
        if user is None:
            return None
        name = self.names.user(user)
        return dict({"handle": user, "is_me": False}, **({"display_name": name} if name else {}))

    def text(self, message):
        return readable(message, self.names.user if self.names else (lambda _: None))


class AdapterFailure(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def refused(error):
    """True when the request provably never took effect: refused by Slack, or never connected."""
    if isinstance(error, Refused):
        return True
    if isinstance(error, urllib.error.HTTPError):
        return 400 <= error.code < 500
    reason = getattr(error, "reason", None)
    return isinstance(reason, (ConnectionRefusedError, socket.gaierror))


def bounded(value, limit=MAX_ID):
    return value if isinstance(value, str) and value.strip() and len(value) <= limit and "\0" not in value else None


ENTITY = re.compile(r"<([@#!])?([^<>|]+)(?:\|([^<>]*))?>")


def readable(message, name_of):
    """Slack's text with mentions, channel links and links as a person reads them, and a note per file."""

    def entity(match):
        sigil, target, label = match.groups()
        if sigil == "@":
            return "@" + (label or name_of(target) or target)
        if sigil == "#":
            return "#" + (label or target)
        if sigil == "!":
            return "@" + (label or target.split("^")[0])
        return f"{label} ({target})" if label and label != target else target

    text = message.get("text") if isinstance(message.get("text"), str) else ""
    lines = [html.unescape(ENTITY.sub(entity, text))] if text.strip() else []
    for item in message.get("files") or []:
        if isinstance(item, dict) and bounded(item.get("name"), 256):
            lines.append(f"[file: {item['name']}]")
    return "\n".join(lines).replace("\0", "")


# Logging in -------------------------------------------------------------------

def manifest(name):
    name = (name or "OPPA").strip()[:35] or "OPPA"
    return {
        "display_information": {"name": name, "description": "Your OPPA assistant"},
        "features": {"bot_user": {"display_name": name, "always_online": True},
                     "app_home": {"messages_tab_enabled": True, "messages_tab_read_only_enabled": False,
                                  "home_tab_enabled": False}},
        "oauth_config": {"scopes": MANIFEST_SCOPES},
        "settings": {"event_subscriptions": {"bot_events": ["message.im", "message.channels", "message.groups",
                                                             "message.mpim"]},
                     "interactivity": {"is_enabled": False}, "org_deploy_enabled": False,
                     "socket_mode_enabled": True, "token_rotation_enabled": False},
    }


def login_link(options):
    """The link that opens Slack's new-app page with this adapter's manifest filled in."""
    State(options.state).write("pending.json", dict(State(options.state).read("pending.json"), name=options.name))
    link = "https://api.slack.com/apps?new_app=1&manifest_json=" + urllib.parse.quote(
        json.dumps(manifest(options.name), separators=(",", ":")), safe="")
    print(json.dumps({"status": "create", "link": link}), flush=True)


def login_token(options):
    """Read one token from the clipboard, check it with Slack, and keep it. Prints the kind kept and what is still
    missing, or `ready` with the owner's handle once all three are kept and agree. The token is never printed."""
    state = State(options.state)
    try:
        pasted = subprocess.run(shlex.split(options.clipboard), stdin=subprocess.DEVNULL, capture_output=True,
                                timeout=10).stdout.decode("utf-8", "replace").strip()
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SystemExit(f"could not read the clipboard: {error}")
    kind = next((kind for prefix, kind in KINDS.items() if pasted.startswith(prefix)), None)
    pending = state.read("pending.json")
    missing = lambda: [k for k in ("bot", "user", "app") if not pending.get(k)]
    if kind is None or not re.fullmatch(r"[A-Za-z0-9-]{20,400}", pasted):
        return print(json.dumps({"status": "no_token", "missing": missing()}), flush=True)
    try:
        if kind == "app":
            Api(options.api_url, pasted).call("apps.connections.open")
            pending["app"] = pasted
        else:
            who = Api(options.api_url, pasted).call("auth.test")
            if kind == "bot":
                bot_id = bounded(who.get("bot_id"))
                info = Api(options.api_url, pasted).get("bots.info", {"bot": bot_id}) if bot_id else {}
                if not bot_id or not bounded((info.get("bot") or {}).get("app_id")):
                    raise Refused("Slack did not name this bot's id and app")
                pending.update(bot=pasted, bot_id=bot_id, bot_user=who.get("user_id"),
                               app_id=(info.get("bot") or {}).get("app_id"), team=who.get("team"),
                               bot_team=who.get("team_id"))
            else:
                pending.update(user=pasted, owner=who.get("user_id"), owner_team=who.get("team_id"))
    except Exception as error:
        return print(json.dumps({"status": "invalid", "kind": kind, "error": str(error)[:300], "missing": missing()}),
                     flush=True)
    state.write("pending.json", pending)
    if missing():
        return print(json.dumps({"status": "kept", "kind": kind, "missing": missing()}), flush=True)
    if pending.get("bot_team") != pending.get("owner_team"):
        return print(json.dumps({"status": "mismatch", "kind": kind, "missing": [],
                                 "error": "the bot and user tokens are from different workspaces"}), flush=True)
    if not bounded(pending.get("owner")):
        raise SystemExit("Slack checked the user token without naming its user")
    print(json.dumps({"status": "ready", "kind": kind, "team": pending.get("team"),
                      "handle": "slack:" + pending["owner"]}), flush=True)


def login_activate(options):
    state = State(options.state)
    pending = state.read("pending.json")
    if not all(pending.get(kind) for kind in KINDS.values()):
        raise SystemExit("no checked Slack login to use")
    state.write("account.json", pending)
    state.remove("pending.json")
    print(json.dumps({"status": "active"}), flush=True)


# Asking another agent ----------------------------------------------------------

def logged_in(options):
    account = State(options.state).read("account.json")
    if not all(account.get(kind) for kind in KINDS.values()):
        raise SystemExit("Slack is not set up: run oppa channels add slack")
    return account


def channel_id(bot, name):
    """A channel id, or the id of the channel this app is in with that name."""
    name = name.lstrip("#")
    if re.fullmatch(r"[CGD][A-Z0-9]{6,}", name):
        return name
    cursor = ""
    while True:
        params = {"types": "public_channel,private_channel", "exclude_archived": "true", "limit": "200"}
        if cursor:
            params["cursor"] = cursor
        page = bot.get("conversations.list", params)
        for channel in page.get("channels") or []:
            if channel.get("name") == name:
                if not channel.get("is_member", True):
                    raise SystemExit(f"invite the app to #{name} first: /invite in that channel")
                return channel["id"]
        cursor = (page.get("response_metadata") or {}).get("next_cursor") or ""
        if not cursor:
            raise SystemExit(f"no channel named #{name} that the app is in; invite it there with /invite")


def mention(bot, name):
    """`<@U…>` for the member whose name, display name or real name is `name`, bots included."""
    wanted = name.lstrip("@").strip().lower()
    cursor = ""
    while True:
        page = bot.get("users.list", dict({"limit": "200"}, **({"cursor": cursor} if cursor else {})))
        for member in page.get("members") or []:
            profile = member.get("profile") or {}
            names = {str(v).lower() for v in (member.get("name"), member.get("real_name"), profile.get("display_name"),
                                              profile.get("real_name")) if v}
            if wanted in names and not member.get("deleted"):
                return f"<@{member['id']}>"
        cursor = (page.get("response_metadata") or {}).get("next_cursor") or ""
        if not cursor:
            raise SystemExit(f"no Slack member named {name}")


def collect(options, account, bot, channel, thread, after, top_level):
    """Everyone else's messages after `after`, in the thread and, for a new thread, at the top of the channel too.
    Waits up to --wait seconds for a first one, then until none has come for --settle seconds."""
    names = Names(bot)
    own = {account.get("bot_id"), account.get("app_id")} - {None}
    deadline = time.monotonic() + options.wait
    seen, last_new = {}, None
    while True:
        found = bot.get("conversations.replies", {"channel": channel, "ts": thread, "limit": "200"}).get("messages") or []
        if top_level:
            found += bot.get("conversations.history", {"channel": channel, "oldest": after, "limit": "100"}).get(
                "messages") or []
        for message in found:
            ts = message.get("ts")
            if not bounded(ts) or not re.fullmatch(r"\d+(\.\d+)?", ts) or float(ts) <= float(after):
                continue
            if {message.get("bot_id"), message.get("app_id")} & own:
                continue
            profile = message.get("bot_profile") or {}
            user = message.get("user")
            who = profile.get("name") or (names.user(user) if user else None) or user or "someone"
            if user == account.get("owner") and (message.get("app_id") or message.get("bot_id")):
                who = f"{who} (an app posting as the owner)"
            elif user == account.get("owner"):
                who = f"{who} (the owner)"
            reply = {"from": who, "ts": ts, "text": readable(message, names.user),
                     "in_thread": message.get("thread_ts") == thread}
            # An agent that posts "Thinking…" and edits its answer in counts from the edit.
            if seen.get(ts) != reply:
                seen[ts], last_new = reply, time.monotonic()
        now = time.monotonic()
        if now >= deadline or (last_new is not None and now - last_new >= options.settle):
            break
        time.sleep(min(POLL_SECONDS, max(0.1, deadline - now)))
    replies = sorted(seen.values(), key=lambda r: float(r["ts"]))
    return {"channel": channel, "thread": thread, "replies": replies,
            "status": "answered" if replies else "no_reply_yet"}


def label(account):
    """How `ask` starts each post, which the agents' scheduled checks look for."""
    return f"\N{ROBOT FACE} {(account.get('name') or 'OPPA').strip()}:"


def ask(options):
    """Post as this app, labelled with the assistant's name, then wait for the agents' replies."""
    account = logged_in(options)
    bot = Api(options.api_url, account["bot"])
    channel = channel_id(bot, options.channel)
    mentions = " ".join(mention(bot, name) for name in options.mention)
    text = " ".join(part for part in (label(account), mentions, options.text.strip()) if part)
    payload = {"channel": channel, "text": text, "unfurl_links": False}
    if options.thread:
        payload["thread_ts"] = options.thread
    ts = bot.call("chat.postMessage", payload)["ts"]
    result = collect(options, account, bot, channel, options.thread or ts, ts, top_level=not options.thread)
    print(json.dumps(dict(result, posted=ts), ensure_ascii=False), flush=True)


def replies(options):
    account = logged_in(options)
    bot = Api(options.api_url, account["bot"])
    channel = channel_id(bot, options.channel)
    result = collect(options, account, bot, channel, options.thread, options.after or options.thread, top_level=True)
    print(json.dumps(result, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True, help="private directory for the tokens and what Slack said of them")
    parser.add_argument("--api-url", default=API_URL, help="Slack Web API address")
    parser.add_argument("--login-link", action="store_true", help="print the link that creates the Slack app, then exit")
    parser.add_argument("--name", default="OPPA", help="the app's name in Slack, for --login-link")
    parser.add_argument("--login-token", action="store_true", help="keep the token on the clipboard, then exit")
    parser.add_argument("--clipboard", default="/usr/bin/pbpaste", help="command that prints the clipboard")
    parser.add_argument("--login-activate", action="store_true", help="use the checked tokens, then exit")
    commands = parser.add_subparsers(dest="command")
    asking = commands.add_parser("ask", help="post as this app to other agents and wait for their replies")
    asking.add_argument("--channel", required=True)
    asking.add_argument("--text", required=True)
    asking.add_argument("--mention", action="append", default=[], help="a member to @mention, by name")
    asking.add_argument("--thread", help="post in this thread instead of starting one")
    checking = commands.add_parser("replies", help="read a thread's replies")
    checking.add_argument("--channel", required=True)
    checking.add_argument("--thread", required=True)
    checking.add_argument("--after", help="only replies after this message")
    for command in (asking, checking):
        command.add_argument("--wait", type=float, default=300.0, help="seconds to wait for a first reply")
        command.add_argument("--settle", type=float, default=30.0, help="seconds without a new reply that end it")
    options = parser.parse_args()
    if options.command in ("ask", "replies"):
        try:
            (ask if options.command == "ask" else replies)(options)
        except (Refused, OSError, ValueError) as error:
            # A task reads this as an answer, not a traceback.
            print(json.dumps({"status": "error", "error": str(error)[:500]}), flush=True)
            sys.exit(1)
    elif options.login_link:
        login_link(options)
    elif options.login_token:
        login_token(options)
    elif options.login_activate:
        login_activate(options)
    else:
        Adapter(options).serve()


if __name__ == "__main__":
    main()
