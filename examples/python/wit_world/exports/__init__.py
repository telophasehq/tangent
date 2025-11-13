from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import log
from ..exports import mapper

class Mapper(Protocol):

    @abstractmethod
    def metadata(self) -> mapper.Meta:
        raise NotImplementedError

    @abstractmethod
    def probe(self) -> List[mapper.Selector]:
        raise NotImplementedError

    @abstractmethod
    def process_logs(self, input: List[log.Logview]) -> List[log.Frame]:
        """
        Raises: `wit_world.types.Err(wit_world.imports.str)`
        """
        raise NotImplementedError


