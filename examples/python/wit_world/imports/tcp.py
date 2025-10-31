from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import poll
from ..imports import streams
from ..imports import network

class ShutdownType(Enum):
    RECEIVE = 0
    SEND = 1
    BOTH = 2

class TcpSocket:
    
    def start_bind(self, network: network.Network, local_address: network.IpSocketAddress) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def finish_bind(self) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def start_connect(self, network: network.Network, remote_address: network.IpSocketAddress) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def finish_connect(self) -> Tuple[streams.InputStream, streams.OutputStream]:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def start_listen(self) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def finish_listen(self) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def accept(self) -> Tuple[Self, streams.InputStream, streams.OutputStream]:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def local_address(self) -> network.IpSocketAddress:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def remote_address(self) -> network.IpSocketAddress:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def is_listening(self) -> bool:
        raise NotImplementedError
    def address_family(self) -> network.IpAddressFamily:
        raise NotImplementedError
    def set_listen_backlog_size(self, value: int) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def keep_alive_enabled(self) -> bool:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def set_keep_alive_enabled(self, value: bool) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def keep_alive_idle_time(self) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def set_keep_alive_idle_time(self, value: int) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def keep_alive_interval(self) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def set_keep_alive_interval(self, value: int) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def keep_alive_count(self) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def set_keep_alive_count(self, value: int) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def hop_limit(self) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def set_hop_limit(self, value: int) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def receive_buffer_size(self) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def set_receive_buffer_size(self, value: int) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def send_buffer_size(self) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def set_send_buffer_size(self, value: int) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def subscribe(self) -> poll.Pollable:
        raise NotImplementedError
    def shutdown(self, shutdown_type: ShutdownType) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def __enter__(self) -> Self:
        """Returns self"""
        return self
                                
    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> bool | None:
        """
        Release this resource.
        """
        raise NotImplementedError



