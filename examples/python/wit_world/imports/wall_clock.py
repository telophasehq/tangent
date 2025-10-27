from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some


@dataclass
class Datetime:
    seconds: int
    nanoseconds: int


def now() -> Datetime:
    raise NotImplementedError

def resolution() -> Datetime:
    raise NotImplementedError

