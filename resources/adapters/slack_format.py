"""An agent's Markdown, ready for Slack's chat.postMessage `markdown_text` argument.

Standard library only and nothing from OPPA, so another host can copy this one file. Slack renders standard
Markdown in `markdown_text` (tables, fenced code with a language, lists, links), so nothing is rewritten.
Two things are done to the text:

- `parts` cuts a long reply into messages. It never leaves a code fence open: a fence that must be cut is
  closed at the end of one part and opened again, with its language, at the start of the next.
- `quiet` keeps a reply from paging a room: @here, @channel and @everyone, and Slack's <!here> forms, get a
  zero-width space so they read the same and notify no one. Text inside code is left alone.

    for part in parts(markdown):
        post({"channel": channel, "markdown_text": part})
"""

import re

# Slack takes 12,000 characters of markdown_text. Parts stay near the 4,000 a plain message holds, so a
# part reads like one message and a table or code block is rarely cut.
PART = 3900
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
BROADCAST = re.compile(r"(?<![\w@])@(?=(?:here|channel|everyone)\b)|<(?=!(?:here|channel|everyone)\b)")
CODE_SPAN = re.compile(r"(`+)(?:.|\n)*?\1")
ZERO_WIDTH = "​"


def _closes(line, marker):
    text = line.strip()
    return len(text) >= len(marker) and set(text) == {marker[0]}


def quiet(markdown):
    """The same text with every room-wide mention outside code made inert."""
    out, fence = [], None
    for line in markdown.split("\n"):
        if fence:
            fence = None if _closes(line, fence) else fence
        elif (opened := FENCE.match(line)):
            fence = opened.group(1)
        else:
            spans = [match.span() for match in CODE_SPAN.finditer(line)]
            line = BROADCAST.sub(
                lambda m: m.group(0) if any(a <= m.start() < b for a, b in spans) else m.group(0) + ZERO_WIDTH, line)
        out.append(line)
    return "\n".join(out)


def _cut(line, room):
    """The line's first `room` characters or fewer, cut at a space when one is in reach, and what is left."""
    cut = line.rfind(" ", room // 2, room)
    cut = cut if cut > 0 else room
    return line[:cut], line[cut:].removeprefix(" ")


def parts(markdown, limit=PART):
    """Messages of at most `limit` characters, cut at a blank line when one is in reach, else at a line."""
    pieces, lines, size = [], [], 0
    fence = opener = None   # the open fence's marker and its opening line
    opened_at = None        # where in `lines` the open fence starts
    blank = None            # the last blank line outside a fence: (index in `lines`, characters before it)

    def emit(chunk):
        text = "\n".join(chunk).strip("\n")
        if text.strip():
            pieces.append(text)

    for rest in markdown.split("\n"):
        while rest is not None:
            # A line too long for one part is cut; inside a fence it leaves room to close and reopen it.
            room = max(limit - (len(opener) + len(fence) + 2 if fence else 0), limit // 4)
            line, rest = _cut(rest, room) if len(rest) > room else (rest, None)
            closer = (len(fence) + 1) if fence else 0
            while size + len(line) + closer > limit and len(lines) > (1 if fence else 0):
                if blank and blank[1] > limit // 2:
                    index = blank[0]
                    emit(lines[:index])
                    lines = lines[index + 1:]
                    opened_at = None if opened_at is None else opened_at - index - 1
                elif fence and opened_at == len(lines) - 1:
                    emit(lines[:-1])     # the fence opened on the last line: move it whole to the next part
                    lines, opened_at = [opener], 0
                elif fence:
                    emit(lines + [fence])
                    lines, opened_at = [opener], 0
                else:
                    emit(lines)
                    lines = []
                size, blank = sum(len(kept) + 1 for kept in lines), None
            if fence:
                if _closes(line, fence):
                    fence = opener = opened_at = None
            elif (opened := FENCE.match(line)):
                fence, opener, opened_at = opened.group(1), line, len(lines)
            elif not line.strip():
                blank = (len(lines), size)
            lines.append(line)
            size += len(line) + 1
    emit(lines)
    return pieces
