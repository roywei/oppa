#!/usr/bin/env python3
"""Secrets connector (connector protocol v1): everything in the owner's OPPA vault in 1Password.

Usage as a connector: onepassword.py [--op PATH] [--vault NAME] [--token-keychain SERVICE] [--security PATH].
Reads: items (each item's title, category, websites and field names, never a value) and get (an
item's field values, a current one-time code included). The owner shares a vault with the
assistant and everything in it is the assistant's to use.

Usage from a script: onepassword.py get ITEM [FIELD] prints one value and nothing else, so a skill's
script can use a key without it entering a transcript; onepassword.py items prints the titles. The
service account token is read from the Keychain for each call and stays in this process.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys

OP_WAIT = 30
STORE = "printf 'add-generic-password -U -s %s -a oppa -w %%s\\n' \"$(pbpaste)\" | security -i"
OP_FIX = "Install the 1Password CLI (brew install 1password-cli)"
NAME = re.compile(r"[^\x00-\x1f]{1,200}")


class Refusal(Exception):
    def __init__(self, error, fix=None):
        super().__init__(error)
        self.error, self.fix = error, fix


def token(options):
    try:
        found = subprocess.run([options.security, "find-generic-password", "-s", options.token_keychain, "-w"],
                               stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                               timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        raise Refusal("the Keychain did not answer", "Run `oppa connectors test secrets` at the Mac")
    value = found.stdout.decode("utf-8", "replace").strip()
    if found.returncode != 0 or not value:
        raise Refusal("no 1Password service account token is in the Keychain under " + options.token_keychain,
                      "Copy the token, then run: " + STORE % options.token_keychain)
    return value


def op(options, *arguments, stdin=None, many=False):
    """The 1Password CLI's JSON answer, which stays in this process; a failure is a Refusal.
    With `many`, the answer is several JSON objects one after another, as `item get -` prints them."""
    program = options.op if os.sep in options.op else shutil.which(options.op)
    if not program or not os.access(program, os.X_OK):
        raise Refusal("the 1Password CLI (op) is not installed or not on the service's PATH", OP_FIX)
    environment = dict(os.environ, OP_SERVICE_ACCOUNT_TOKEN=token(options), OP_BIOMETRIC_UNLOCK_ENABLED="false")
    try:
        run = subprocess.run([program, *arguments, "--vault", options.vault, "--format", "json"], env=environment,
                             input=stdin if stdin is not None else b"", stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, timeout=OP_WAIT)
    except subprocess.TimeoutExpired:
        raise Refusal("1Password did not answer within %d seconds" % OP_WAIT, "Check the Mac's network connection")
    except OSError:
        raise Refusal("the 1Password CLI could not run", OP_FIX)
    if run.returncode != 0:
        lower = run.stderr.decode("utf-8", "replace").lower()
        if "isn't an item" in lower or "more than one item" in lower:
            raise Refusal("the %s vault has no single item by that name" % options.vault,
                          "Call items for the exact titles")
        if "vault" in lower and ("isn't a vault" in lower or "not found" in lower):
            raise Refusal("the service account cannot read a vault named " + options.vault,
                          "Share the vault with the service account in 1Password")
        if any(word in lower for word in ("unauthorized", "token", "sign in", "signed in", "authoriz")):
            raise Refusal("1Password did not accept the service account token",
                          "Copy a current token, then run: " + STORE % options.token_keychain)
        raise Refusal("1Password refused the request")
    try:
        if not many:
            return json.loads(run.stdout)
        text, objects, decoder, at = run.stdout.decode("utf-8"), [], json.JSONDecoder(), 0
        while text[at:].strip():
            value, at = decoder.raw_decode(text, len(text) - len(text[at:].lstrip()))
            objects.append(value)
        return objects
    except ValueError:
        raise Refusal("1Password answered in a form this connector does not read", "Update the 1Password CLI")


def summary(item):
    return {"title": item.get("title"), "category": item.get("category"),
            "websites": [link.get("href") for link in item.get("urls") or [] if link.get("href")],
            "fields": [field.get("label") or field.get("id") for field in item.get("fields") or []
                       if field.get("value") not in (None, "") or field.get("type") == "OTP"]}


def item_name(args):
    name = args.get("item")
    if not isinstance(name, str) or not NAME.fullmatch(name) or name.startswith("-"):
        raise Refusal("item must be an item's title from items")
    return name


def items(options, args):
    query = str(args.get("query") or "").lower()
    entries = [entry for entry in op(options, "item", "list") if query in str(entry.get("title", "")).lower()]
    # One call for every item's fields, values left concealed.
    detailed = op(options, "item", "get", "-", stdin=json.dumps(entries).encode(), many=True) if entries else []
    return {"vault": options.vault, "items": [summary(item) for item in detailed]}


def values(options, name):
    item = op(options, "item", "get", name, "--reveal")
    fields = {}
    for field in item.get("fields") or []:
        label = field.get("label") or field.get("id")
        value = field.get("totp") if field.get("type") == "OTP" else field.get("value")
        if label and value not in (None, ""):
            fields[label] = value
    return item, fields


def get(options, args):
    item, fields = values(options, item_name(args))
    field = args.get("field")
    if field is not None:
        if field not in fields:
            raise Refusal("%s has no field named %s" % (item.get("title"), field),
                          "Its fields: " + ", ".join(fields))
        fields = {field: fields[field]}
    return dict(summary(item), values=fields)


def primary(item, fields):
    """The value a script most likely wants: the password, else the credential, else the first concealed."""
    for field in item.get("fields") or []:
        if field.get("purpose") == "PASSWORD" and field.get("value"):
            return field["value"]
    for label in ("credential", "api key", "token", "key"):
        for name, value in fields.items():
            if name.lower() == label:
                return value
    for field in item.get("fields") or []:
        if field.get("type") == "CONCEALED" and field.get("value"):
            return field["value"]
    raise Refusal("%s has no password or credential field; name one" % item.get("title"),
                  "Its fields: " + ", ".join(fields))


OPERATIONS = [
    {"name": "items", "tier": "read",
     "description": "The items in the owner's OPPA vault: title, category, websites and field names, never a "
                    "value. A script uses one with: python3 <runtime>/adapters/onepassword.py get '<title>' [field].",
     "input_schema": {"type": "object", "properties": {"query": {"type": "string", "maxLength": 200}}}},
    {"name": "get", "tier": "read",
     "description": "An item's field values (one field when named), with a current one-time code when it has "
                    "one. Prefer having a script read the value itself, so it stays out of the conversation.",
     "input_schema": {"type": "object", "properties": {"item": {"type": "string", "maxLength": 200},
                                                       "field": {"type": "string", "maxLength": 200}},
                      "required": ["item"]}},
]
CALLS = {"items": items, "get": get}


def emit(value):
    print(json.dumps(value, ensure_ascii=False))
    sys.exit(0)


def script(options, words):
    """Command line use by a skill's script: the value alone on stdout, a reason on stderr."""
    try:
        if words[0] == "items" and len(words) == 1:
            print("\n".join(entry.get("title", "") for entry in op(options, "item", "list")))
        elif words[0] == "get" and len(words) in (2, 3):
            item, fields = values(options, item_name({"item": words[1]}))
            if len(words) == 3 and words[2] not in fields:
                raise Refusal("%s has no field named %s" % (item.get("title"), words[2]),
                              "Its fields: " + ", ".join(fields))
            print(fields[words[2]] if len(words) == 3 else primary(item, fields))
        else:
            raise Refusal("usage: onepassword.py items | onepassword.py get ITEM [FIELD]")
    except Refusal as refusal:
        sys.stderr.write(refusal.error + ("\n" + refusal.fix if refusal.fix else "") + "\n")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--op", default=shutil.which("op") or "/opt/homebrew/bin/op")
    parser.add_argument("--vault", default="OPPA")
    parser.add_argument("--token-keychain", default="oppa-1password")
    parser.add_argument("--security", default="/usr/bin/security")
    parser.add_argument("words", nargs="*")
    options = parser.parse_args()
    if options.words:
        script(options, options.words)
        return
    try:
        request = json.loads(sys.stdin.read())
        assert isinstance(request, dict) and request.get("protocol_version") == 1
    except (ValueError, AssertionError):
        emit({"status": "error", "error": "invalid connector request"})
    if request.get("method") == "describe":
        emit({"protocol_version": 1, "operations": OPERATIONS})
    if request.get("method") == "health":
        try:
            emit({"ready": True, "vault": options.vault, "items": len(op(options, "item", "list"))})
        except Refusal as refusal:
            emit({"ready": False, "problem": refusal.error, **({"fix": refusal.fix} if refusal.fix else {})})
    call = CALLS.get(request.get("operation")) if request.get("method") == "call" else None
    if call is None:
        emit({"status": "error", "error": "unknown method or operation"})
    try:
        arguments = request.get("arguments")
        if not isinstance(arguments, dict):
            raise Refusal("invalid arguments")
        emit({"status": "observed", "result": call(options, arguments)})
    except Refusal as refusal:
        emit({"status": "error", "error": refusal.error, **({"fix": refusal.fix} if refusal.fix else {})})


if __name__ == "__main__":
    main()
