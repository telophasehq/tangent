from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import poll
from ..imports import network

class ResolveAddressStream:
    
    def resolve_next_address(self) -> Optional[network.IpAddress]:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def subscribe(self) -> poll.Pollable:
        raise NotImplementedError
    def __enter__(self) -> Self:
        """Returns self"""
        return self
                                
    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> bool | None:
        """
        Release this resource.
        """
        raise NotImplementedError



def resolve_addresses(network: network.Network, name: str) -> ResolveAddressStream:
    """
    Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
    """
    raise NotImplementedError

