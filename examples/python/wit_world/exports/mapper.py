from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import log

@dataclass
class Meta:
    name: str
    version: str


@dataclass
class Pred_Has:
    value: str


@dataclass
class Pred_Eq:
    value: Tuple[str, log.Scalar]


@dataclass
class Pred_Prefix:
    value: Tuple[str, str]


@dataclass
class Pred_In:
    value: Tuple[str, List[log.Scalar]]


@dataclass
class Pred_Gt:
    value: Tuple[str, float]


@dataclass
class Pred_Regex:
    value: Tuple[str, str]


Pred = Union[Pred_Has, Pred_Eq, Pred_Prefix, Pred_In, Pred_Gt, Pred_Regex]


@dataclass
class Selector:
    any: List[Pred]
    all: List[Pred]
    none: List[Pred]

