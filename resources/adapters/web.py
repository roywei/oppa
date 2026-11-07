"""Web channel adapter: OPPA channel protocol v1 on stdio, and the owner's
dashboard on a local web page.

This process translates; it never reads message meaning. A message the owner
types on the page becomes one `event` from the handle `owner`. A `send` from the
host is appended to an in-memory feed that the page reads by long poll, and its
native id is the receipt: the reply reached this process and is on the page,
which does not prove anyone read it. Web conversations are threads: `main`, and
`t-<random>` for each new chat. The canonical log stays in OPPA. The page loads
a thread's history, and every other view, by running one `oppa` subcommand
without a shell and with a timeout.

Only the owner may use the page. The server listens on loopback by default,
refuses a Host header other than its own address (DNS rebinding) and a write
whose Origin is another site, and requires the cookie that the login link in
`<state>/login-url` sets. The token and message bodies are never logged.

Usage: web.py --state DIR --home DIR --private-dir DIR [--oppa PATH]
              [--port 8765] [--bind 127.0.0.1] [--source DIR] [--channel web]
"""

import argparse
import base64
import collections
import datetime
import hashlib
import hmac
import http.cookies
import http.server
import ipaddress
import json
import os
import re
import secrets
import signal
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse

PROTOCOL_VERSION = 1
NOT_SENT = 1001
OWNER_HANDLE = "owner"
MAX_BODY = 256 * 1024           # a message body in UTF-8 bytes, the protocol's bound
MAX_NOTE = 256 * 1024           # a vault note, the vault's own bound
MAX_REQUEST = 2 * 1024 * 1024   # an HTTP request body
MAX_PROMPT = 8000               # a schedule's instruction, in characters
MAX_ERROR = 400                 # an error message for the page
CLI_SECONDS = 20
DOCTOR_SECONDS = 120
POLL_SECONDS = 25
REPLY_WAIT_SECONDS = 300        # how long the page shows typing dots without a reply
FEED_ITEMS = 2000
HISTORY_EVENTS = 400
LOG_BUDGET = 16 * 1024
THREAD = re.compile(r"main|t-[a-z0-9]{8,32}")
SCHEDULE_ID = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_-]{0,63}")
CLOCK = re.compile(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]")
DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
ACCESS = ("read", "connectors", "computer")
NOTES = ("assistant/persona.md", "assistant/rules.md")
# One folder name under people/: letters and digits in any script, spaces,
# dots, hyphens and underscores, never a leading dot or space.
PERSON = r"(?![. ])[\w .-]{0,63}[\w.-]"
PROFILE = re.compile(r"people/(" + PERSON + r")/profile\.md")
PEOPLE_NOTE = re.compile(r"people/(" + PERSON + r")/(.+)")

_log_lock = threading.Lock()
_log_left = [LOG_BUDGET]


def log(line):
    """One line to stderr within a fixed budget: the host reads only the first
    64 KiB of an adapter's stderr, and a full pipe would block this process."""
    data = (line[:300] + "\n").encode("utf-8", "replace")
    with _log_lock:
        if _log_left[0] < len(data):
            return
        _log_left[0] -= len(data)
        try:
            sys.stderr.buffer.write(data)
            sys.stderr.buffer.flush()
        except (OSError, ValueError):
            pass


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def write_private(path, text):
    """Replace a file atomically; mkstemp makes it readable by its owner only."""
    handle, temporary = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".tmp-")
    try:
        with os.fdopen(handle, "w") as out:
            out.write(text)
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def owner_token(state):
    """The login token, made on first start. A missing or damaged file gets a new one."""
    path = os.path.join(state, "token")
    try:
        with open(path) as handle:
            token = handle.read().strip()
        if re.fullmatch(r"[A-Za-z0-9_-]{32,}", token):
            os.chmod(path, 0o600)
            return token
    except FileNotFoundError:
        pass
    token = secrets.token_urlsafe(32)
    write_private(path, token + "\n")
    return token


def first_line(text, limit=60):
    line = next((line.strip() for line in text.splitlines() if line.strip()), "")
    line = " ".join(line.split())
    return line if len(line) <= limit else line[:limit - 1].rstrip() + "…"


class Refused(Exception):
    """A page request this adapter will not carry out, with its HTTP status and
    a plain message for the owner."""

    def __init__(self, status, message, **extra):
        super().__init__(message)
        self.status = status
        self.message = message
        self.extra = extra


class CliError(Exception):
    def __init__(self, message, status=502):
        super().__init__(message[:MAX_ERROR])
        self.status = status


class AdapterFailure(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def last_line(stderr):
    """The line of a failed command's stderr that says what went wrong."""
    lines = [line.strip() for line in stderr.decode("utf-8", "replace").splitlines()]
    lines = [line for line in lines if line and not line.startswith(("For more information", "Usage:"))]
    if not lines:
        return "no diagnostic"
    line = re.sub(r"^(?:Error: |error: |\d+: )", "", lines[-1])
    return line[:MAX_ERROR]


def run_bounded(argv, timeout):
    """Run argv in its own process group; on timeout the whole group is killed."""
    try:
        process = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
    except OSError as error:
        raise CliError("could not run %s: %s" % (os.path.basename(argv[0]), error.strerror or error)) from None
    try:
        out, err = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            pass
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            pass
        return None, b"", b""
    return process.returncode, out, err


class Oppa:
    """Runs `oppa` subcommands for the page: an argv list, never a shell."""

    def __init__(self, program, home, private):
        self.base = [program, "--home", home, "--private-dir", private]

    def run(self, args, timeout=CLI_SECONDS, ok=(0,)):
        name = " ".join(args[:2]) if len(args) > 1 and re.fullmatch(r"[a-z]+", args[1]) else args[0]
        code, out, err = run_bounded(self.base + list(args), timeout)
        if code is None:
            raise CliError("oppa %s did not finish within %d seconds" % (name, timeout), 504)
        if code not in ok:
            raise CliError("oppa %s failed: %s" % (name, last_line(err)))
        return code, out.decode("utf-8", "replace")

    def json(self, args, timeout=CLI_SECONDS, ok=(0,)):
        code, out = self.run(args, timeout, ok)
        try:
            return code, json.loads(out)
        except ValueError:
            raise CliError("oppa %s did not print JSON" % args[0]) from None


class Feed:
    """Messages this process carried, for the page's long poll. Memory only:
    after a restart the page reloads from the canonical log."""

    def __init__(self):
        self.epoch = secrets.token_hex(8)
        self.cond = threading.Condition(threading.RLock())  # re-entered by App.route_send
        self.seq = 0
        self.items = collections.deque(maxlen=FEED_ITEMS)
        self.asked = {}  # thread -> when the owner's latest message without a reply arrived

    def add(self, thread, direction, native_id, body, at):
        with self.cond:
            self.seq += 1
            item = {"seq": self.seq, "thread": thread, "direction": direction,
                    "native_id": native_id, "body": body, "at": at}
            self.items.append(item)
            if direction == "in":
                self.asked[thread] = time.monotonic()
            else:
                self.asked.pop(thread, None)
            self.cond.notify_all()
            return item

    def waiting(self):
        """Seconds since each thread's unanswered owner message, within the window."""
        current = time.monotonic()
        return {thread: round(current - since, 1) for thread, since in self.asked.items()
                if current - since < REPLY_WAIT_SECONDS}

    def poll(self, after, timeout):
        """Items after `after`, waiting up to `timeout` for one. A cursor older
        than the oldest item kept asks the page to reload."""
        deadline = time.monotonic() + timeout
        with self.cond:
            while True:
                if self.items and after < self.items[0]["seq"] - 1:
                    return None, self.seq, self.waiting()
                items = [item for item in self.items if item["seq"] > after]
                remaining = deadline - time.monotonic()
                if items or remaining <= 0:
                    return items, self.seq, self.waiting()
                self.cond.wait(remaining)

    def thread_items(self, thread):
        with self.cond:
            return [item for item in self.items if item["thread"] == thread]


class Threads:
    """The web threads this adapter knows: `main` always, and each `t-<random>`
    from its first message on, kept in <state>/threads.json with its title. Only
    a known thread takes a `send`."""

    def __init__(self, state):
        self.path = os.path.join(state, "threads.json")
        self.lock = threading.Lock()
        self.known = {"main": {"title": "Main", "created_at": None}}
        try:
            with open(self.path) as handle:
                saved = json.load(handle)
            for thread, info in saved.items():
                if thread != "main" and THREAD.fullmatch(thread) and isinstance(info, dict):
                    title = info.get("title") if isinstance(info.get("title"), str) else ""
                    self.known[thread] = {"title": first_line(title) or "New chat",
                                          "created_at": info.get("created_at")}
        except (OSError, ValueError, AttributeError):
            pass

    def title(self, thread):
        with self.lock:
            info = self.known.get(thread)
            return info["title"] if info else None

    def remember(self, thread, title):
        """Record a thread under its first title and return the title it keeps."""
        with self.lock:
            if thread not in self.known:
                self.known[thread] = {"title": first_line(title or "") or "New chat", "created_at": now()}
                saved = {key: info for key, info in self.known.items() if key != "main"}
                write_private(self.path, json.dumps(saved, ensure_ascii=False, indent=1))
            return self.known[thread]["title"]

    def listing(self):
        with self.lock:
            return {thread: dict(info) for thread, info in self.known.items()}


def load_page():
    """The page and a Content-Security-Policy that allows only its own inline
    script and style, so injected markup could not run code even if rendering
    let some through."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web.html")
    with open(path, "rb") as handle:
        page = handle.read()
    text = page.decode("utf-8")

    def hashes(tag):
        found = re.findall(r"<%s>(.*?)</%s>" % (tag, tag), text, re.S)
        return " ".join("'sha256-%s'" % base64.b64encode(hashlib.sha256(body.encode("utf-8")).digest()).decode()
                        for body in found) or "'none'"

    policy = ("default-src 'none'; script-src %s; style-src %s; img-src data:; connect-src 'self'; "
              "base-uri 'none'; form-action 'none'; frame-ancestors 'none'") % (hashes("script"), hashes("style"))
    return page, policy


class App:
    """State shared by the protocol loop and the page's requests."""

    def __init__(self, options):
        self.options = options
        self.state = os.path.abspath(options.state)
        os.makedirs(self.state, mode=0o700, exist_ok=True)
        os.chmod(self.state, 0o700)
        self.token = owner_token(self.state)
        self.session = hmac.new(self.token.encode(), b"oppa-web-session", hashlib.sha256).hexdigest()
        self.feed = Feed()
        self.threads = Threads(self.state)
        self.oppa = Oppa(options.oppa, options.home, options.private_dir)
        self.account = None
        self.emit = None
        self.server = None
        self.port = None
        self.failure = None
        self.hosts = set()
        self.cookie = "oppa_web"
        try:
            self.page, self.policy = load_page()
        except (OSError, UnicodeDecodeError):
            self.page, self.policy = None, None
            self.failure = "web.html is missing or unreadable next to web.py"

    def listen(self):
        options = self.options
        try:
            server = Server((options.bind, options.port), Handler)
        except OSError as error:
            self.failure = "cannot listen on %s:%d: %s; choose another --port" % (
                options.bind, options.port, error.strerror or error)
            log("web: " + self.failure)
            return
        server.app = self
        self.port = server.server_address[1]
        names = ["127.0.0.1", "localhost"]
        if options.bind not in ("0.0.0.0", "127.0.0.1"):
            names.append(options.bind)
        self.hosts = {"%s:%d" % (name, self.port) for name in names}
        if self.port == 80:
            self.hosts.update(names)
        # Cookies are per host, not per port: name this one after its port.
        self.cookie = "oppa_web_%d" % self.port
        address = options.bind if options.bind != "0.0.0.0" else "127.0.0.1"
        write_private(os.path.join(self.state, "login-url"),
                      "http://%s:%d/login?token=%s\n" % (address, self.port, self.token))
        self.server = server
        threading.Thread(target=server.serve_forever, daemon=True).start()
        # Last, so whoever waits for the port finds the login link beside it.
        write_private(os.path.join(self.state, "port"), "%d\n" % self.port)
        log("web: listening on %s:%d; the login link is in %s" % (
            options.bind, self.port, os.path.join(self.state, "login-url")))

    # Requests -------------------------------------------------------------------

    def host_allowed(self, host):
        return isinstance(host, str) and host.lower() in self.hosts

    def signed_in(self, header):
        if not header:
            return False
        cookies = http.cookies.SimpleCookie()
        try:
            cookies.load(header)
        except http.cookies.CookieError:
            return False
        morsel = cookies.get(self.cookie)
        return morsel is not None and hmac.compare_digest(morsel.value.encode(), self.session.encode())

    def token_matches(self, token):
        return hmac.compare_digest(token.encode("utf-8"), self.token.encode("utf-8"))

    def conversation(self, thread):
        if self.account is None:
            raise Refused(503, "The web channel is waiting for oppa serve to connect.")
        return "%s/%s/%s" % (self.options.channel, self.account, thread)

    # Chat -----------------------------------------------------------------------

    def route_session(self, query, body):
        return {"channel": self.options.channel, "account": self.account, "connected": self.account is not None,
                "source": bool(self.options.source), "epoch": self.feed.epoch}

    def route_threads(self, query, body):
        threads = self.threads.listing()
        latest, error = {}, None
        if self.account is not None:
            prefix = "%s/%s/" % (self.options.channel, self.account)
            try:
                _, conversations = self.oppa.json(["conversations"])
            except CliError as failure:
                conversations, error = [], str(failure)
            for conversation in conversations if isinstance(conversations, list) else []:
                ident = conversation.get("id") if isinstance(conversation, dict) else None
                thread = ident[len(prefix):] if isinstance(ident, str) and ident.startswith(prefix) else None
                if thread and THREAD.fullmatch(thread):
                    latest[thread] = conversation.get("last_event_at")
                    if thread not in threads:
                        # A thread the log knows but this state lost still takes replies.
                        threads[thread] = {"title": self.threads.remember(thread, conversation.get("title")),
                                           "created_at": None}
        listed = [{"id": thread, "title": info["title"], "created_at": info["created_at"],
                   "last_event_at": latest.get(thread),
                   "conversation": self.conversation(thread) if self.account else None}
                  for thread, info in threads.items()]
        # Main first, then the most recently active.
        listed.sort(key=lambda t: t["last_event_at"] or t["created_at"] or "", reverse=True)
        listed.sort(key=lambda t: t["id"] != "main")
        return {"threads": listed, "error": error}

    def route_new_thread(self, query, body):
        return {"id": "t-" + secrets.token_hex(6)}

    def route_send(self, query, body):
        thread = body.get("thread")
        if not isinstance(thread, str) or not THREAD.fullmatch(thread):
            raise Refused(400, "That is not a web chat thread.")
        text = body.get("body")
        if not isinstance(text, str) or not text.strip():
            raise Refused(400, "Write a message first.")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        if "\0" in text:
            raise Refused(400, "The message contains a character that cannot be sent.")
        if len(text.encode("utf-8")) > MAX_BODY:
            raise Refused(413, "That message is too long to send.")
        conversation = self.conversation(thread)
        if self.emit is None:
            raise Refused(503, "The web channel is waiting for oppa serve to connect.")
        title = self.threads.title(thread) or self.threads.remember(thread, text)
        native_id = "in-" + secrets.token_hex(8)
        at = now()
        # One step under the feed's lock, so a reply can never reach the feed
        # before the message it answers.
        with self.feed.cond:
            self.emit({"jsonrpc": "2.0", "method": "event", "params": {
                "conversation": {"native_id": thread, "kind": "dm", "title": title},
                "native_id": native_id,
                "sender": {"handle": OWNER_HANDLE, "display_name": None, "is_me": False},
                "body": text, "attachments": [], "at": at}})
            item = self.feed.add(thread, "in", native_id, text, at)
        return {"item": item, "conversation": conversation}

    def route_feed(self, query, body):
        try:
            after = max(0, int(query.get("after") or 0))
            timeout = min(float(POLL_SECONDS), max(0.0, float(query.get("timeout") or POLL_SECONDS)))
        except ValueError:
            raise Refused(400, "Invalid feed cursor.") from None
        if query.get("epoch") != self.feed.epoch:
            items, cursor, waiting = None, self.feed.seq, self.feed.waiting()
        else:
            items, cursor, waiting = self.feed.poll(after, timeout)
        return {"reset": items is None, "epoch": self.feed.epoch, "cursor": cursor,
                "items": items or [], "waiting": waiting}

    def route_history(self, query, body):
        thread = query.get("thread")
        if not isinstance(thread, str) or not THREAD.fullmatch(thread):
            raise Refused(400, "That is not a web chat thread.")
        conversation = self.conversation(thread)
        # Taking the feed before the log leaves no gap: what the log lacks is
        # merged in, and anything newer reaches the page by its poll.
        recent = self.feed.thread_items(thread)
        _, events = self.oppa.json(["history", "--conversation", conversation])
        if not isinstance(events, list):
            raise CliError("oppa history did not print a list of events")
        keys = ("seq", "created_at", "direction", "author", "kind", "body", "delivery", "native_id")
        shown = [{key: event.get(key) for key in keys} for event in events if isinstance(event, dict)]
        logged = {event["native_id"] for event in shown if event.get("native_id")}
        # A reply logged before its receipt has no native id yet; match it by text.
        unreceipted = collections.Counter(event["body"] for event in shown
                                          if event.get("direction") == "out" and not event.get("native_id"))
        for item in recent:
            if item["native_id"] in logged:
                continue
            if item["direction"] == "out" and unreceipted[item["body"]] > 0:
                unreceipted[item["body"]] -= 1
                continue
            shown.append({"seq": None, "created_at": item["at"], "direction": item["direction"],
                          "author": None, "kind": "message", "body": item["body"],
                          "delivery": "sent" if item["direction"] == "out" else None,
                          "native_id": item["native_id"], "pending": True})
        return {"conversation": conversation, "events": shown[-HISTORY_EVENTS:],
                "truncated": len(shown) > HISTORY_EVENTS}

    # Configuration --------------------------------------------------------------

    def route_conversations(self, query, body):
        return {"conversations": self.oppa.json(["conversations"])[1]}

    def route_schedules(self, query, body):
        args = ["schedules", "--all"] if query.get("all") == "1" else ["schedules"]
        return {"schedules": self.oppa.json(args)[1]}

    def route_add_schedule(self, query, body):
        conversation, at = body.get("conversation"), body.get("at")
        date, days, prompt = body.get("date") or None, body.get("days") or [], body.get("prompt")
        if not isinstance(at, str) or not CLOCK.fullmatch(at):
            raise Refused(400, "Give the time as HH:MM, for example 07:30.")
        if date is not None:
            try:
                if not isinstance(date, str) or len(date) != 10:
                    raise ValueError()
                datetime.date.fromisoformat(date)
            except ValueError:
                raise Refused(400, "Give the date as YYYY-MM-DD, or leave it empty.") from None
        if not isinstance(days, list) or any(day not in DAYS for day in days):
            raise Refused(400, "Weekdays must be among mon, tue, wed, thu, fri, sat and sun.")
        if date is not None and days:
            raise Refused(400, "Choose a date for a one-off job or weekdays for a repeating one, not both.")
        if not isinstance(prompt, str) or not prompt.strip():
            raise Refused(400, "Write the instruction the assistant should carry out.")
        if "\0" in prompt or len(prompt) > MAX_PROMPT:
            raise Refused(400, "The instruction is too long or contains a character that cannot be saved.")
        if not isinstance(conversation, str) or not conversation:
            raise Refused(400, "Choose the conversation the job runs in.")
        _, known = self.oppa.json(["conversations"])
        if conversation not in {c.get("id") for c in known if isinstance(c, dict)}:
            raise Refused(400, "Choose a conversation from the list.")
        args = ["schedules", "add", "--conversation", conversation, "--at", at]
        if date is not None:
            args += ["--date", date]
        if days:
            args += ["--days", ",".join(day for day in DAYS if day in days)]
        # One argv element even when the text starts with a hyphen.
        args.append("--prompt=" + prompt)
        return {"schedule": self.oppa.json(args)[1]}

    def route_cancel_schedule(self, query, body):
        ident = body.get("id")
        if not isinstance(ident, str) or not SCHEDULE_ID.fullmatch(ident):
            raise Refused(400, "That is not a schedule id.")
        return {"schedule": self.oppa.json(["schedules", "cancel", ident])[1]}

    def route_settings(self, query, body):
        return {"settings": self.oppa.json(["settings"])[1]}

    def route_access(self, query, body):
        level = body.get("access")
        if level not in ACCESS:
            raise Refused(400, "Access is read, connectors or computer.")
        return {"result": self.oppa.json(["settings", "access", level])[1]}

    # Notes ----------------------------------------------------------------------

    def note_path(self, path):
        if not isinstance(path, str) or not (path in NOTES or PROFILE.fullmatch(path)):
            raise Refused(400, "The dashboard edits assistant/persona.md, assistant/rules.md and "
                               "people/<name>/profile.md only.")
        return path

    def read_note(self, path):
        revision = self.oppa.run(["memory", "revision"])[1].strip()
        try:
            content, exists = self.oppa.run(["memory", "read", path])[1], True
        except CliError as failure:
            if "does not exist" not in str(failure):
                raise
            content, exists = "", False
        version = hashlib.sha256(content.encode("utf-8")).hexdigest() if exists else ""
        return {"path": path, "exists": exists, "content": content, "revision": revision, "version": version}

    def route_note(self, query, body):
        return {"note": self.read_note(self.note_path(query.get("path")))}

    def route_save_note(self, query, body):
        path = self.note_path(body.get("path"))
        content, version = body.get("content"), body.get("version")
        if not isinstance(content, str) or not isinstance(version, str):
            raise Refused(400, "Send the note's text and the version it was loaded at.")
        content = content.replace("\r\n", "\n").replace("\r", "\n")
        if content and not content.endswith("\n"):
            content += "\n"
        if "\0" in content or len(content.encode("utf-8")) > MAX_NOTE:
            raise Refused(413, "The note is too long, or contains a character that cannot be saved.")
        if not content.strip():
            raise Refused(400, "The note is empty. Write something before saving.")
        current = self.read_note(path)
        if current["version"] != version:
            raise Refused(409, "This note changed since you opened it, so your edit was not saved. "
                               "The latest version is loaded; your text is kept so you can apply it again.",
                          conflict=True, note=current)
        if current["exists"] and current["content"] == content:
            return {"saved": False, "note": current}
        try:
            receipt = self.put_note(path, content, current["revision"], "Edited in the dashboard")
        except CliError as failure:
            if "stale vault revision" in str(failure):
                raise Refused(409, "The vault changed while saving, so your edit was not saved. The latest "
                                   "version is loaded; your text is kept so you can apply it again.",
                              conflict=True, note=self.read_note(path)) from None
            if "owner dirty file" in str(failure):
                raise Refused(409, "This note has changes in the vault folder that are not committed. "
                                   "Commit or discard them there first, then save again.") from None
            raise
        return {"saved": True, "receipt": receipt, "note": self.read_note(path)}

    def put_note(self, path, content, revision, reason):
        """One revision-checked vault commit of a note, by the owner from the dashboard."""
        handle, temporary = tempfile.mkstemp(dir=self.state, prefix="note-", suffix=".md")
        try:
            with os.fdopen(handle, "w", encoding="utf-8") as out:
                out.write(content)
            return self.oppa.json(["memory", "put", path, "--file", temporary, "--expected-revision", revision,
                                   "--reason", reason, "--source", "dashboard"])[1]
        finally:
            try:
                os.unlink(temporary)
            except OSError:
                pass

    def route_people(self, query, body):
        _, hits = self.oppa.json(["memory", "search", "people/"])
        people = {}
        for hit in hits if isinstance(hits, list) else []:
            path = hit.get("path") if isinstance(hit, dict) else None
            match = PEOPLE_NOTE.fullmatch(path) if isinstance(path, str) else None
            if not match:
                continue
            person = people.setdefault(match.group(1), {"name": match.group(1), "profile": None, "notes": []})
            if match.group(2) == "profile.md":
                person["profile"] = {"path": path, "excerpt": hit.get("excerpt") or ""}
            else:
                person["notes"].append(path)
        return {"people": sorted(people.values(), key=lambda person: person["name"].lower())}

    # Health -------------------------------------------------------------------

    def route_doctor(self, query, body):
        code, checks = self.oppa.json(["doctor"], timeout=DOCTOR_SECONDS, ok=(0, 1))
        return {"ready": code == 0, "checks": checks}

    def route_tasks(self, query, body):
        return {"tasks": self.oppa.json(["tasks", "list"])[1]}

    def route_outbox(self, query, body):
        return {"outbox": self.oppa.json(["outbox"])[1]}


ROUTES = {
    ("GET", "/api/session"): App.route_session,
    ("GET", "/api/threads"): App.route_threads,
    ("POST", "/api/threads"): App.route_new_thread,
    ("POST", "/api/send"): App.route_send,
    ("GET", "/api/feed"): App.route_feed,
    ("GET", "/api/history"): App.route_history,
    ("GET", "/api/conversations"): App.route_conversations,
    ("GET", "/api/schedules"): App.route_schedules,
    ("POST", "/api/schedules"): App.route_add_schedule,
    ("POST", "/api/schedules/cancel"): App.route_cancel_schedule,
    ("GET", "/api/settings"): App.route_settings,
    ("POST", "/api/settings/access"): App.route_access,
    ("GET", "/api/note"): App.route_note,
    ("PUT", "/api/note"): App.route_save_note,
    ("GET", "/api/people"): App.route_people,
    ("GET", "/api/dev/doctor"): App.route_doctor,
    ("GET", "/api/dev/tasks"): App.route_tasks,
    ("GET", "/api/dev/outbox"): App.route_outbox,
}


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def server_bind(self):
        # HTTPServer.server_bind looks the address up in DNS; nothing here needs it.
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]

    def handle_error(self, request, client_address):
        error = sys.exc_info()[1]
        if not isinstance(error, (ConnectionError, TimeoutError)):
            log("web: a request failed: %s" % type(error).__name__)


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "oppa-web"
    sys_version = ""
    protocol_version = "HTTP/1.1"
    timeout = 60

    def log_message(self, *args):
        pass  # request lines can carry the login token; nothing is logged

    def do_GET(self):
        self.dispatch("GET")

    def do_POST(self):
        self.dispatch("POST")

    def do_PUT(self):
        self.dispatch("PUT")

    def do_DELETE(self):
        self.dispatch("DELETE")

    def dispatch(self, method):
        app = self.server.app
        url = urllib.parse.urlsplit(self.path)
        raw = b""
        if method in ("POST", "PUT", "DELETE"):
            # Read the body before any answer, refusals included: closing a
            # socket with unread input resets it, and the client can lose the
            # answer. Writes then close their connection.
            self.close_connection = True
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                length = -1
            if not 0 <= length <= MAX_REQUEST:
                return self.reply(413, {"error": "The request is too large."})
            raw = self.rfile.read(length)
        host = self.headers.get("Host")
        if not app.host_allowed(host):
            return self.reply(403, {"error": "This server answers only to its own address."})
        origin = self.headers.get("Origin")
        if method != "GET" and origin is not None and origin.lower() != "http://" + host.lower():
            return self.reply(403, {"error": "Requests from other sites are refused."})
        if method == "GET" and url.path == "/":
            return self.page()
        if method == "GET" and url.path == "/login":
            return self.login(url.query)
        if not url.path.startswith("/api/"):
            return self.reply(404, {"error": "Not found."})
        if not app.signed_in(self.headers.get("Cookie")):
            return self.reply(401, {"error": "Sign in with the login link first."})
        route = ROUTES.get((method, url.path))
        if route is None:
            return self.reply(404, {"error": "Not found."})
        query = {key: values[0] for key, values in urllib.parse.parse_qs(url.query).items()}
        body = None
        if method in ("POST", "PUT"):
            body = self.parse_json(raw)
            if body is None:
                return
        try:
            result = route(app, query, body or {})
        except Refused as refused:
            payload = {"error": refused.message}
            payload.update(refused.extra)
            return self.reply(refused.status, payload)
        except CliError as failure:
            return self.reply(failure.status, {"error": str(failure)})
        except Exception as error:  # an adapter bug must not look like an answer
            log("web: %s failed: %s" % (url.path, type(error).__name__))
            return self.reply(500, {"error": "The web channel failed to handle this request."})
        self.reply(200, result)

    def parse_json(self, raw):
        # A form or a plain-text post, which another site could send, is not JSON.
        if (self.headers.get("Content-Type") or "").split(";")[0].strip() != "application/json":
            self.reply(415, {"error": "Send JSON."})
            return None
        try:
            body = json.loads(raw.decode("utf-8"))
        except ValueError:
            body = None
        if not isinstance(body, dict):
            self.reply(400, {"error": "Send a JSON object."})
            return None
        return body

    def common_headers(self):
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")

    def reply(self, status, payload):
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.respond(status, "application/json; charset=utf-8", data)

    def respond(self, status, content_type, data, headers=()):
        try:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.common_headers()
            for name, value in headers:
                self.send_header(name, value)
            if self.close_connection:
                self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(data)
        except (ConnectionError, TimeoutError):
            self.close_connection = True

    def page(self):
        app = self.server.app
        if app.page is None:
            return self.respond(500, "text/plain; charset=utf-8", b"web.html is missing next to web.py\n")
        self.respond(200, "text/html; charset=utf-8", app.page, [("Content-Security-Policy", app.policy)])

    def login(self, query):
        app = self.server.app
        token = (urllib.parse.parse_qs(query).get("token") or [""])[0]
        if not app.token_matches(token):
            body = ("<!doctype html><meta charset=utf-8><title>OPPA</title>"
                    "<p>This sign-in link is not valid. Open the address in the web channel's "
                    "login-url file on this Mac.</p>").encode("utf-8")
            return self.respond(403, "text/html; charset=utf-8", body)
        cookie = "%s=%s; Path=/; HttpOnly; SameSite=Strict; Max-Age=%d" % (app.cookie, app.session, 400 * 86400)
        self.respond(303, "text/plain; charset=utf-8", b"Signed in.\n", [("Location", "/"), ("Set-Cookie", cookie)])


# Channel protocol ------------------------------------------------------------------

class Adapter:
    """The host side: newline-delimited JSON-RPC on stdio. stdout carries
    protocol frames only, written whole under one lock."""

    def __init__(self, app):
        self.app = app
        self.out_lock = threading.Lock()
        app.emit = self.emit

    def emit(self, frame):
        line = (json.dumps(frame, ensure_ascii=False) + "\n").encode("utf-8")
        with self.out_lock:
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()

    def reply(self, request_id, result=None, error=None):
        frame = {"jsonrpc": "2.0", "id": request_id}
        if error is not None:
            frame["error"] = error
        else:
            frame["result"] = result
        self.emit(frame)

    def serve(self):
        for line in sys.stdin.buffer:
            try:
                request = json.loads(line.decode("utf-8"))
            except ValueError:
                continue
            if not isinstance(request, dict):
                continue
            method, request_id = request.get("method"), request.get("id")
            params = request.get("params") if isinstance(request.get("params"), dict) else {}
            if method == "shutdown":
                break
            handler = {"initialize": self.initialize, "send": self.send, "health": self.health}.get(method)
            if handler is None:
                if request_id is not None:
                    self.reply(request_id, error={"code": -32601, "message": "unknown method"})
                continue
            try:
                result = handler(params)
            except AdapterFailure as failure:
                if request_id is not None:
                    self.reply(request_id, error={"code": failure.code, "message": failure.message[:1024]})
            except Exception as error:  # anything unexpected leaves delivery unknown
                if request_id is not None:
                    self.reply(request_id, error={"code": -32603, "message": str(error)[:1024]})
            else:
                if request_id is not None:
                    self.reply(request_id, result)

    def initialize(self, params):
        if params.get("protocol_version") != PROTOCOL_VERSION:
            raise AdapterFailure(-32602, "unsupported protocol version")
        account = params.get("account")
        if not isinstance(account, str) or not account or "/" in account:
            raise AdapterFailure(-32602, "invalid account")
        if self.app.failure:
            raise AdapterFailure(-32603, self.app.failure)
        self.app.account = account
        return {"protocol_version": PROTOCOL_VERSION,
                "capabilities": {"attachments": False, "receipts": True, "threads": False,
                                 "reactions": False, "typing": False}}

    def send(self, params):
        thread = params.get("conversation")
        body = params.get("body")
        if not isinstance(thread, str) or self.app.threads.title(thread) is None:
            raise AdapterFailure(NOT_SENT, "no such web thread")
        if params.get("attachments"):
            raise AdapterFailure(NOT_SENT, "the web page does not take attachments")
        if not isinstance(body, str) or not body.strip():
            raise AdapterFailure(NOT_SENT, "nothing to send")
        native_id = "out-" + secrets.token_hex(8)
        self.app.feed.add(thread, "out", native_id, body, now())
        return {"native_id": native_id}

    def health(self, _params):
        return {"ok": self.app.server is not None and self.app.failure is None,
                "port": self.app.port, "threads": len(self.app.threads.listing())}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True, help="private directory for the token, port and threads")
    parser.add_argument("--oppa", default=os.environ.get("OPPA_EXECUTABLE"),
                        help="the oppa executable the page's views run; defaults to the one serve runs")
    parser.add_argument("--home", required=True, help="the instance's runtime directory")
    parser.add_argument("--private-dir", required=True, help="the instance's private configuration directory")
    parser.add_argument("--port", type=int, default=8765, help="TCP port; 0 picks a free one")
    parser.add_argument("--bind", default="127.0.0.1", help="IPv4 address to listen on")
    parser.add_argument("--source", help="OPPA source checkout (reported in the session)")
    parser.add_argument("--channel", default="web", help="this channel's id in [[channels]]")
    options = parser.parse_args()
    if not options.oppa:
        parser.error("--oppa is required when serve does not start the adapter")
    try:
        ipaddress.IPv4Address(options.bind)
    except ValueError:
        parser.error("--bind takes an IPv4 address, such as 127.0.0.1")
    if not 0 <= options.port <= 65535:
        parser.error("--port must be between 0 and 65535")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", options.channel):
        parser.error("--channel must be the [[channels]] id")
    if options.source:
        options.source = os.path.abspath(options.source)
    app = App(options)
    adapter = Adapter(app)
    if app.failure is None:
        app.listen()
    adapter.serve()


if __name__ == "__main__":
    main()
