from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some



@dataclass
class Scalar_Str:
    value: str


@dataclass
class Scalar_Int:
    value: int


@dataclass
class Scalar_Float:
    value: float


@dataclass
class Scalar_Boolean:
    value: bool


@dataclass
class Scalar_Bytes:
    value: bytes


Scalar = Union[Scalar_Str, Scalar_Int, Scalar_Float, Scalar_Boolean, Scalar_Bytes]


class Logview:
    
    def has(self, path: str) -> bool:
        """
        JSONPath/dot-path style, e.g. "detail.findings[0].CompanyName"
        """
        raise NotImplementedError
    def get(self, path: str) -> Optional[Scalar]:
        raise NotImplementedError
    def len(self, path: str) -> Optional[int]:
        raise NotImplementedError
    def get_list(self, path: str) -> Optional[List[Scalar]]:
        raise NotImplementedError
    def get_map(self, path: str) -> Optional[List[Tuple[str, Scalar]]]:
        raise NotImplementedError
    def keys(self, path: str) -> List[str]:
        raise NotImplementedError
    def __enter__(self) -> Self:
        """Returns self"""
        return self
                                
    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> bool | None:
        """
        Release this resource.
        """
        raise NotImplementedError



