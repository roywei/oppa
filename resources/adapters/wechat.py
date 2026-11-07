"""WeChat channel adapter: OPPA channel protocol v1 over Tencent's iLink bot API.

WeChat's ClawBot plugin binds one WeChat user to a bot account. This process
long-polls `getupdates` for that user's messages and answers with
`sendmessage`; it only makes outbound HTTPS requests and never reads meaning.

- A reply needs the `context_token` of a message from its recipient, so the
  latest one per user is kept in the state directory and survives restarts.
- `sendmessage` returns no message id. A send the server accepted (`ret` 0)
  reports the adapter's own `client_id` as its native id; a refusal before
  anything was accepted is `not sent` (code 1001); anything else stays unknown.
- Text only: other items arrive as a bracketed note, and a send with
  attachments is refused. Long replies go out in parts of at most 2000
  characters, split at paragraph, line or word breaks.
- A message in a WeChat group is never forwarded: iLink documents no group
  sends, and a reply must never land in a direct chat it was not written for.

Without a login, or after the server expires one (`ret` -14), the adapter keeps
running and reports it in `health`, so the other channels keep answering; it
starts polling as soon as a new login is saved. `oppa channels add wechat` logs
in: --login-start gets a QR code link, --login-finish waits for its scan, keeps
the login (0600) and prints the handle for the people registry, and
--login-activate puts it in use once the registry has it.

Usage: wechat.py --state DIR [--base-url URL] [--login-start | --login-finish [--wait SECONDS] | --login-activate]
"""

import argparse
import base64
import datetime
import json
import os
import random
import secrets
import socket
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

PROTOCOL_VERSION = 1
NOT_SENT = 1001
UNVERIFIED = 1002
BASE_URL = "https://ilinkai.weixin.qq.com"
CHANNEL_VERSION = "1.0.2"
SESSION_EXPIRED = -14
POLL_SECONDS = 35.0
SEND_SECONDS = 20.0
PART = 2000
MAX_ID = 4096
MAX_BODY = 256 * 1024
NOTES = {2: "[image]", 3: "[voice message]", 4: "[file]", 5: "[video]"}


class Refused(Exception):
    """The server answered and did not take the request."""

    def __init__(self, ret, message):
        super().__init__(message)
        self.ret = ret


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """A redirected POST would be replayed as a GET whose answer proves nothing."""

    def redirect_request(self, *args):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


class Ilink:
    def __init__(self, base_url, token=None):
        self.base_url = base_url.rstrip("/")
        self.token = token

    def headers(self, body):
        headers = {"Content-Type": "application/json", "AuthorizationType": "ilink_bot_token",
                   "Content-Length": str(len(body)), "iLink-App-ClientVersion": "1",
                   # A fresh random uint32 each request, as decimal text in base64.
                   "X-WECHAT-UIN": base64.b64encode(str(random.getrandbits(32)).encode()).decode()}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        return headers

    def request(self, method, path, payload=None, timeout=SEND_SECONDS):
        body = b"" if payload is None else json.dumps(dict(payload, base_info={"channel_version": CHANNEL_VERSION}),
                                                      ensure_ascii=False).encode()
        request = urllib.request.Request(self.base_url + path, data=body if payload is not None else None,
                                         headers=self.headers(body), method=method)
        with OPENER.open(request, timeout=timeout) as response:
            text = response.read()
        result = json.loads(text) if text.strip() else {}
        if not isinstance(result, dict):
            raise ValueError("iLink answered with something other than an object")
        ret = result.get("ret") or result.get("errcode") or 0
        if ret:
            raise Refused(ret, f"iLink {path} refused: ret {ret} {result.get('errmsg') or ''}".strip())
        return result


class State:
    """The login, the poll cursor and each user's latest context token, in one private directory."""

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


class Adapter:
    def __init__(self, options):
        self.options = options
        self.state = State(options.state)
        self.out_lock = threading.Lock()
        # Guards the login in use, its client and reply tokens, and the files they live in.
        self.lock = threading.Lock()
        self.account = None
        self.client = None
        self.contexts = {}
        self.error = None
        self.groups_skipped = 0
        self.unreadable = 0
        self.polling = False
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
            start, self.polling = not self.polling, True
        if start:
            threading.Thread(target=self.poll, daemon=True).start()
        return {"protocol_version": PROTOCOL_VERSION,
                "capabilities": {"attachments": False, "receipts": True, "threads": False, "reactions": False, "typing": False}}

    def health(self, _params):
        account, _ = self.sync()
        login = "bound" if account else ("expired" if self.state.read("account.json") else "needed")
        status = {"ok": bool(account) and self.error is None, "login": login,
                  "groups_skipped": self.groups_skipped, "unreadable": self.unreadable}
        if account:
            status["user"] = account.get("ilink_user_id")
        if self.error:
            status["error"] = self.error
        if not account:
            status["error"] = "run oppa channels add wechat and scan its QR code in WeChat"
        return status

    def send(self, params):
        user = params.get("conversation")
        body = params.get("body") or ""
        if not isinstance(user, str) or not user or not body.strip():
            raise AdapterFailure(NOT_SENT, "nothing to send or no conversation")
        if params.get("attachments"):
            raise AdapterFailure(NOT_SENT, "the WeChat adapter sends text only")
        _, client = self.sync()
        token = self.contexts.get(user)
        if client is None:
            raise AdapterFailure(NOT_SENT, "WeChat is not logged in")
        if not token:
            raise AdapterFailure(NOT_SENT, "WeChat has no message from this user to reply to yet")
        receipt = None
        for part in parts(body):
            client_id = "oppa-" + secrets.token_hex(8)
            message = {"from_user_id": "", "to_user_id": user, "client_id": client_id, "message_type": 2,
                       "message_state": 2, "context_token": token,
                       "item_list": [{"type": 1, "text_item": {"text": part}}]}
            try:
                client.request("POST", "/ilink/bot/sendmessage", {"msg": message})
            except Exception as error:
                if isinstance(error, Refused) and error.ret == SESSION_EXPIRED:
                    self.expire(client)
                if receipt is None and refused(error):
                    raise AdapterFailure(NOT_SENT, f"WeChat did not take the send: {error}")
                raise AdapterFailure(UNVERIFIED, f"WeChat send outcome unknown: {error}")
            receipt = receipt or client_id
        return {"native_id": receipt}

    # iLink side ----------------------------------------------------------------

    def sync(self):
        """The login saved last, unless the server ended it. A scan replaces the one in use at once, with its
        own cursor and reply tokens."""
        with self.lock:
            account = self.state.read("account.json")
            if account.get("bot_token") in self.state.read("expired.json").get("bot_tokens", []):
                account = {}
            if account.get("bot_token") != (self.account or {}).get("bot_token"):
                self.account = account if account.get("bot_token") else None
                self.client = self.account and Ilink(account.get("baseurl") or self.options.base_url, account["bot_token"])
                self.contexts = self.state.read("contexts.json")
                self.error = None
            return self.account, self.client

    def expire(self, client):
        """The server ended this client's login. It is marked, never deleted, so a scan that --login-activate saves
        meanwhile, in its own process, is never lost."""
        with self.lock:
            ended = self.state.read("expired.json").get("bot_tokens", [])
            self.state.write("expired.json", {"bot_tokens": (ended + [client.token])[-8:]})
            if self.client is client:
                self.account, self.client, self.contexts = None, None, {}
                self.error = "the WeChat login expired; log in again"

    def poll(self):
        failures = 0
        while not self.closing:
            client = None
            try:
                account, client = self.sync()
                if client is None:
                    time.sleep(1)
                    continue
                self.receive(account, client)
                failures, self.error = 0, None
                continue
            except Refused as error:
                problem = str(error)
                if error.ret == SESSION_EXPIRED:
                    try:
                        self.expire(client)
                        continue
                    except Exception as failure:
                        problem = f"recording the expired login failed: {failure}"
            except (socket.timeout, TimeoutError):
                continue  # a long poll that ran out is an empty answer
            except Exception as error:  # nothing an answer holds may stop the poll
                problem = f"getupdates failed: {type(error).__name__}: {error}"
            failures, self.error = failures + 1, problem[:1024]
            time.sleep(30 if failures >= 3 else 2)

    def receive(self, account, client):
        bot = account.get("ilink_bot_id")
        cursor = self.state.read("cursor.json")
        buf = cursor.get("buf", "") if cursor.get("bot") == bot else ""
        result = client.request("POST", "/ilink/bot/getupdates", {"get_updates_buf": buf}, timeout=POLL_SECONDS + 10)
        for message in result.get("msgs") or []:
            try:
                self.forward(message, client)
            except Exception:  # one message the adapter cannot read never holds up the rest
                self.unreadable += 1
        if isinstance(result.get("get_updates_buf"), str) and result["get_updates_buf"]:
            with self.lock:
                if self.client is client:
                    self.state.write("cursor.json", {"bot": bot, "buf": result["get_updates_buf"]})

    def forward(self, message, client):
        if not isinstance(message, dict) or message.get("message_type") == 2:
            return  # the bot's own messages are this adapter's sends
        if message.get("group_id"):
            self.groups_skipped += 1
            return
        event = normalize(message)
        if event is None:
            return
        token = message.get("context_token")
        user = event["sender"]["handle"]
        with self.lock:
            # A token read under a login since replaced is no use, but the message still is.
            if self.client is client and isinstance(token, str) and token and self.contexts.get(user) != token:
                self.contexts[user] = token
                self.state.write("contexts.json", self.contexts)
        self.emit({"jsonrpc": "2.0", "method": "event", "params": event})


class AdapterFailure(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def refused(error):
    """True when the request provably never took effect: refused by the server, or never connected."""
    if isinstance(error, Refused):
        return True
    if isinstance(error, urllib.error.HTTPError):
        return 400 <= error.code < 500
    reason = getattr(error, "reason", None)
    return isinstance(reason, (ConnectionRefusedError, socket.gaierror))


def bounded(value, limit=MAX_ID):
    if isinstance(value, int) and not isinstance(value, bool):
        value = str(value)
    return value if isinstance(value, str) and value.strip() and len(value) <= limit and "\0" not in value else None


def normalize(message):
    """Map one iLink user message to a protocol v1 event, or None when it carries nothing."""
    user = bounded(message.get("from_user_id"))
    native_id = bounded(message.get("message_id"))
    if user is None or native_id is None:
        return None
    lines = []
    for item in message.get("item_list") or []:
        if not isinstance(item, dict):
            continue
        kind = item.get("type")
        text = ((item.get("text_item") if kind == 1 else item.get("voice_item") if kind == 3 else None) or {}).get("text")
        if isinstance(text, str) and text.strip():
            lines.append(text)
        elif kind == 4 and bounded((item.get("file_item") or {}).get("file_name"), 256):
            lines.append(f"[file: {item['file_item']['file_name']}]")
        elif kind in NOTES:
            lines.append(NOTES[kind])
    body = "\n".join(lines).replace("\0", "")[:MAX_BODY]
    if not body.strip():
        return None
    event = {"conversation": {"native_id": user, "kind": "dm"}, "native_id": native_id,
             "sender": {"handle": user, "is_me": False}, "body": body, "attachments": []}
    if isinstance(message.get("create_time_ms"), int):
        at = datetime.datetime.fromtimestamp(message["create_time_ms"] / 1000, datetime.timezone.utc)
        event["at"] = at.isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return event


def parts(body):
    """At most PART characters each, cut at a paragraph, line or word break when one is in reach."""
    pieces = []
    while len(body) > PART:
        window = body[:PART]
        cut = max((window.rfind(mark) for mark in ("\n\n", "\n", " ")), default=-1)
        cut = cut if cut > PART // 2 else PART
        pieces.append(body[:cut].rstrip())
        # Drop the break itself, never the indentation of the line after it.
        body = body[cut:].lstrip("\n") if body[cut:cut + 1] == "\n" else body[cut:].removeprefix(" ")
    return pieces + ([body] if body.strip() else [])


def login_start(options):
    """Ask WeChat for a QR code to bind a user, and keep it for --login-finish."""
    code = Ilink(options.base_url).request("GET", "/ilink/bot/get_bot_qrcode?bot_type=3")
    if not bounded(code.get("qrcode")) or not bounded(code.get("qrcode_img_content")):
        raise SystemExit("WeChat answered without a QR code")
    State(options.state).write("pending.json", {"qrcode": code["qrcode"], "link": code["qrcode_img_content"]})
    print(json.dumps({"status": "scan", "link": code["qrcode_img_content"]}), flush=True)


def login_finish(options):
    """Wait up to --wait seconds for the pending QR code's scan. Prints `bound` with the handle, `waiting`,
    `scanned` (not yet confirmed on the phone), `expired`, or `none` when no code is pending."""
    state, client = State(options.state), Ilink(options.base_url)
    pending = state.read("pending.json")
    if not pending.get("qrcode"):
        return print(json.dumps({"status": "none"}), flush=True)
    path = "/ilink/bot/get_qrcode_status?qrcode=" + urllib.request.quote(str(pending["qrcode"]))
    deadline = time.monotonic() + options.wait
    while True:
        remaining = deadline - time.monotonic()
        try:
            answer = client.request("GET", path, timeout=max(1.0, min(remaining, POLL_SECONDS + 10)))
        except (socket.timeout, TimeoutError, urllib.error.URLError) as error:
            if isinstance(error, urllib.error.URLError) and not isinstance(error.reason, (socket.timeout, TimeoutError)):
                raise
            answer = {"status": "wait"}  # the status call is held open until something changes
        status = answer.get("status")
        if status == "confirmed":
            break
        if status == "expired":
            state.remove("pending.json")
            return print(json.dumps({"status": "expired"}), flush=True)
        if time.monotonic() >= deadline:
            return print(json.dumps({"status": "scanned" if status == "scaned" else "waiting"}), flush=True)
        time.sleep(1)
    account = {key: answer.get(key) for key in ("bot_token", "ilink_bot_id", "ilink_user_id", "baseurl")}
    if not account["bot_token"] or not bounded(account["ilink_user_id"]):
        raise SystemExit("WeChat confirmed the scan without a token or user id")
    # Used only once --login-activate says the owner's registry took it, so a scan by the wrong person never
    # replaces a working login.
    state.write("confirmed.json", account)
    state.remove("pending.json")
    print(json.dumps({"status": "bound", "handle": "wechat:" + account["ilink_user_id"]}), flush=True)


def login_activate(options):
    """Make the confirmed login the one in use, with a new cursor and reply tokens."""
    state = State(options.state)
    account = state.read("confirmed.json")
    if not account.get("bot_token"):
        raise SystemExit("no confirmed WeChat login to use")
    for name in ("cursor.json", "contexts.json", "expired.json"):
        state.remove(name)
    state.write("account.json", account)
    state.remove("confirmed.json")
    print(json.dumps({"status": "active"}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--state", required=True, help="private directory for the login, cursor and reply tokens")
    parser.add_argument("--base-url", default=BASE_URL, help="iLink API address")
    parser.add_argument("--login-start", action="store_true", help="get a QR code to bind a WeChat user, then exit")
    parser.add_argument("--login-finish", action="store_true", help="wait for that QR code's scan, then exit")
    parser.add_argument("--login-activate", action="store_true", help="use the confirmed login, then exit")
    parser.add_argument("--wait", type=float, default=60.0, help="seconds --login-finish waits for the scan")
    options = parser.parse_args()
    if options.login_start:
        login_start(options)
    elif options.login_finish:
        login_finish(options)
    elif options.login_activate:
        login_activate(options)
    else:
        Adapter(options).serve()


if __name__ == "__main__":
    main()
