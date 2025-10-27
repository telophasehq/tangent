from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import poll


def now() -> int:
    raise NotImplementedError

def resolution() -> int:
    raise NotImplementedError

def subscribe_instant(when: int) -> poll.Pollable:
    raise NotImplementedError

def subscribe_duration(when: int) -> poll.Pollable:
    raise NotImplementedError

