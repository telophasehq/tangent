from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import network
from ..imports import udp


def create_udp_socket(address_family: network.IpAddressFamily) -> udp.UdpSocket:
    """
    Raises: `wit_world.types.Err(wit_world.imports.network.ErrorCode)`
    """
    raise NotImplementedError

