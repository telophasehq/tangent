from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import network
from ..imports import tcp


def create_tcp_socket(address_family: network.IpAddressFamily) -> tcp.TcpSocket:
    """
    Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
    """
    raise NotImplementedError

