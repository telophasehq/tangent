"""A dataclass's JSON Schema, for a plugin's `output-schema`.

Each field's type hint gives its JSON type: str, int, float, bool, a list, a
dict, another dataclass, or an Optional of one. A field that is Optional may
be null or left out; every other field is required.
"""

import dataclasses
import json
import types
import typing

_SCALARS = {str: "string", int: "integer", float: "number", bool: "boolean"}


def _optional(t) -> bool:
    return typing.get_origin(t) in (typing.Union, types.UnionType) and type(None) in typing.get_args(t)


def _of(t) -> dict:
    origin = typing.get_origin(t)
    if origin in (typing.Union, types.UnionType):
        return {"anyOf": [_of(a) for a in typing.get_args(t)]}
    if t is type(None):
        return {"type": "null"}
    if t in _SCALARS:
        return {"type": _SCALARS[t]}
    if t is list or origin is list:
        args = typing.get_args(t)
        return {"type": "array", "items": _of(args[0]) if args else {}}
    if t is dict or origin is dict:
        return {"type": "object"}
    if dataclasses.is_dataclass(t):
        return _object(t)
    return {}


def _object(cls) -> dict:
    hints = typing.get_type_hints(cls)
    fields = dataclasses.fields(cls)
    return {
        "type": "object",
        "properties": {f.name: _of(hints[f.name]) for f in fields},
        "required": [f.name for f in fields if not _optional(hints[f.name])],
    }


def schema_of(cls) -> str:
    """The JSON Schema of the records a dataclass writes, as text."""
    return json.dumps({"$schema": "https://json-schema.org/draft/2020-12/schema", **_object(cls)})
