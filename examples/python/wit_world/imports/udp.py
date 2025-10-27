from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import poll
from ..imports import network

@dataclass
class IncomingDatagram:
    data: bytes
    remote_address: network.IpSocketAddress

@dataclass
class OutgoingDatagram:
    data: bytes
    remote_address: Optional[network.IpSocketAddress]

class IncomingDatagramStream:
    
    def receive(self, max_results: int) -> List[IncomingDatagram]:
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


class OutgoingDatagramStream:
    
    def check_send(self) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def send(self, datagrams: List[OutgoingDatagram]) -> int:
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


class UdpSocket:
    
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
    def stream(self, remote_address: Optional[network.IpSocketAddress]) -> Tuple[IncomingDatagramStream, OutgoingDatagramStream]:
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
    def address_family(self) -> network.IpAddressFamily:
        raise NotImplementedError
    def unicast_hop_limit(self) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
        """
        raise NotImplementedError
    def set_unicast_hop_limit(self, value: int) -> None:
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
    def __enter__(self) -> Self:
        """Returns self"""
        return self
                                
    def __exit__(self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None) -> bool | None:
        """
        Release this resource.
        """
        raise NotImplementedError



