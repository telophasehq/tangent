from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..exports import mapper
from ..imports import log

class Mapper(Protocol):

    @abstractmethod
    def metadata(self) -> mapper.Meta:
        raise NotImplementedError

    @abstractmethod
    def probe(self) -> List[mapper.Selector]:
        raise NotImplementedError

    @abstractmethod
    def process_logs(self, input: List[log.Logview]) -> bytes:
        """
        Raises: `wit_world.types.Err(wit_world.imports.str)`
        """
        raise NotImplementedError


