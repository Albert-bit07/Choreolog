#!/usr/bin/env python3
"""Export pinned TLC's full textual witness as typed JSON, without evaluating TLA+."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys

MAX_BYTES = 16 * 1024 * 1024
TOKEN = re.compile(r"\s*(<<|>>|\|->|:>|@@|[()[\]{},]|-?[0-9]+|[A-Za-z_][A-Za-z_0-9]*)")
SPACE = re.compile(r"\s*")
HEADER = re.compile(r"^State ([0-9]+): <([^\n]+)>\s*$", re.MULTILINE)
VARIABLE = re.compile(r"^/\\ ([A-Za-z_][A-Za-z_0-9]*) = ", re.MULTILINE)
FOOTER = re.compile(
    r"^[0-9]+ states generated, [0-9]+ distinct states found, " r"[0-9]+ states left on queue\.$",
    re.MULTILINE,
)


class TraceError(ValueError):
    """Unsupported, incomplete or ambiguous TLC output."""


class ValueParser:
    def __init__(self, text: str):
        self.text = text
        self.position = 0
        self.tokens = 0

    def peek(self) -> str:
        self.position = SPACE.match(self.text, self.position).end()
        if self.position == len(self.text):
            return ""
        if self.text[self.position] == '"':
            return '"'
        match = TOKEN.match(self.text, self.position)
        if not match:
            raise TraceError(f"unsupported value syntax at offset {self.position}")
        return match[1]

    def take(self, expected: str | None = None) -> str:
        token = self.peek()
        if not token or (expected is not None and token != expected):
            raise TraceError(f"expected {expected!r}, got {token!r}")
        self.position += len(token)
        self.tokens += 1
        if self.tokens > 1_000_000:
            raise TraceError("value exceeds token limit")
        return token

    def value(self, depth: int = 0):
        if depth > 64:
            raise TraceError("value exceeds nesting limit")
        token = self.peek()
        if token == '"':
            try:
                value, end = json.JSONDecoder().raw_decode(self.text, self.position)
            except json.JSONDecodeError as error:
                raise TraceError("unsupported TLC string escape or truncated string") from error
            self.position = end
            return value
        if token in ("<<", "{"):
            self.take(token)
            end = ">>" if token == "<<" else "}"
            values = []
            if self.peek() != end:
                while True:
                    values.append(self.value(depth + 1))
                    if self.peek() != ",":
                        break
                    self.take(",")
            self.take(end)
            return {"$sequence" if token == "<<" else "$set": values}
        if token == "[":
            self.take("[")
            record = {}
            while True:
                field = self.take()
                if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", field) or field in record:
                    raise TraceError("invalid or duplicate record field")
                self.take("|->")
                record[field] = self.value(depth + 1)
                if self.peek() != ",":
                    break
                self.take(",")
            self.take("]")
            return record
        if token == "(":
            self.take("(")
            pairs, keys = [], set()
            while True:
                key = self.value(depth + 1)
                canonical = json.dumps(key, sort_keys=True)
                if canonical in keys:
                    raise TraceError("duplicate function key")
                keys.add(canonical)
                self.take(":>")
                pairs.append([key, self.value(depth + 1)])
                if self.peek() != "@@":
                    break
                self.take("@@")
            self.take(")")
            return {"$function": pairs}
        if token in ("TRUE", "FALSE"):
            self.take()
            return token == "TRUE"
        if re.fullmatch(r"-?[0-9]+", token):
            return int(self.take())
        if re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", token):
            return {"$model": self.take()}
        raise TraceError(f"expected a value, got {token!r}")


def parse_value(text: str):
    parser = ValueParser(text)
    result = parser.value()
    if parser.peek():
        raise TraceError("trailing syntax after value")
    return result


def parse_trace(text: str, invariant: str, max_states: int = 10_000) -> list[dict]:
    if len(text.encode("utf-8")) > MAX_BYTES:
        raise TraceError("trace exceeds byte limit")
    expected = f"Error: Invariant {invariant} is violated."
    if expected not in text.splitlines() or "Error: The behavior up to this point is:" not in text:
        raise TraceError("expected invariant witness is absent")
    headers = list(HEADER.finditer(text))
    if not 2 <= len(headers) <= max_states:
        raise TraceError("invalid trace state count")
    footer = FOOTER.search(text, headers[-1].end())
    if not footer or "Finished in " not in text[footer.end() :]:
        raise TraceError("incomplete TLC result footer")
    states = []
    variables = None
    for index, header in enumerate(headers):
        if int(header[1]) != index + 1:
            raise TraceError("state numbers must be contiguous from 1")
        end = headers[index + 1].start() if index + 1 < len(headers) else footer.start()
        body = text[header.end() : end]
        fields = list(VARIABLE.finditer(body))
        if not fields or body[: fields[0].start()].strip():
            raise TraceError("state has no full variable assignment")
        values = {}
        for field_index, field in enumerate(fields):
            if field[1] in values:
                raise TraceError("duplicate state variable")
            end_value = (
                fields[field_index + 1].start() if field_index + 1 < len(fields) else len(body)
            )
            values[field[1]] = parse_value(body[field.end() : end_value])
        if variables is None:
            variables = set(values)
        elif set(values) != variables:
            raise TraceError("variable set changed; difftrace is unsupported")
        states.append({"state": index + 1, "location": header[2], "variables": values})
    return states


def source(path: pathlib.Path) -> dict:
    data = path.read_bytes()
    return {"name": path.name, "sha256": hashlib.sha256(data).hexdigest()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", required=True, type=pathlib.Path)
    parser.add_argument("--model", required=True, type=pathlib.Path)
    parser.add_argument("--config", required=True, type=pathlib.Path)
    parser.add_argument("--invariant", required=True)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()
    with args.log.open("rb") as stream:
        data = stream.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise TraceError("trace exceeds byte limit")
    text = data.decode("utf-8")
    states = parse_trace(text, args.invariant)
    document = {
        "v": 1,
        "kind": "tlc-text-behavior",
        "model": source(args.model),
        "config": source(args.config),
        "log_sha256": hashlib.sha256(data).hexdigest(),
        "tlc_version": text.splitlines()[0],
        "invariant": args.invariant,
        "states": states,
    }
    # Parse completely before replacing a prior artifact.
    args.output.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(f"Exported {len(states)} TLC states to {args.output}")
    print("Scope: typed witness export only; no C++ replay/refinement established.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (TraceError, OSError, UnicodeError) as error:
        print(f"TLC export FAIL: {error}", file=sys.stderr)
        sys.exit(1)
