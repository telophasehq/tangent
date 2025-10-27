from typing import TypeVar, Generic, Union, Optional, Protocol, Tuple, List, Any, Self
from types import TracebackType
from enum import Flag, Enum, auto
from dataclasses import dataclass
from abc import abstractmethod
import weakref

from ..types import Result, Ok, Err, Some
from ..imports import wall_clock
from ..imports import streams
from ..imports import error

class DescriptorType(Enum):
    UNKNOWN = 0
    BLOCK_DEVICE = 1
    CHARACTER_DEVICE = 2
    DIRECTORY = 3
    FIFO = 4
    SYMBOLIC_LINK = 5
    REGULAR_FILE = 6
    SOCKET = 7

class DescriptorFlags(Flag):
    READ = auto()
    WRITE = auto()
    FILE_INTEGRITY_SYNC = auto()
    DATA_INTEGRITY_SYNC = auto()
    REQUESTED_WRITE_SYNC = auto()
    MUTATE_DIRECTORY = auto()

class PathFlags(Flag):
    SYMLINK_FOLLOW = auto()

class OpenFlags(Flag):
    CREATE = auto()
    DIRECTORY = auto()
    EXCLUSIVE = auto()
    TRUNCATE = auto()

@dataclass
class DescriptorStat:
    type: DescriptorType
    link_count: int
    size: int
    data_access_timestamp: Optional[wall_clock.Datetime]
    data_modification_timestamp: Optional[wall_clock.Datetime]
    status_change_timestamp: Optional[wall_clock.Datetime]


@dataclass
class NewTimestamp_NoChange:
    pass


@dataclass
class NewTimestamp_Now:
    pass


@dataclass
class NewTimestamp_Timestamp:
    value: wall_clock.Datetime


NewTimestamp = Union[NewTimestamp_NoChange, NewTimestamp_Now, NewTimestamp_Timestamp]


@dataclass
class DirectoryEntry:
    type: DescriptorType
    name: str

class ErrorCode(Enum):
    ACCESS = 0
    WOULD_BLOCK = 1
    ALREADY = 2
    BAD_DESCRIPTOR = 3
    BUSY = 4
    DEADLOCK = 5
    QUOTA = 6
    EXIST = 7
    FILE_TOO_LARGE = 8
    ILLEGAL_BYTE_SEQUENCE = 9
    IN_PROGRESS = 10
    INTERRUPTED = 11
    INVALID = 12
    IO = 13
    IS_DIRECTORY = 14
    LOOP = 15
    TOO_MANY_LINKS = 16
    MESSAGE_SIZE = 17
    NAME_TOO_LONG = 18
    NO_DEVICE = 19
    NO_ENTRY = 20
    NO_LOCK = 21
    INSUFFICIENT_MEMORY = 22
    INSUFFICIENT_SPACE = 23
    NOT_DIRECTORY = 24
    NOT_EMPTY = 25
    NOT_RECOVERABLE = 26
    UNSUPPORTED = 27
    NO_TTY = 28
    NO_SUCH_DEVICE = 29
    OVERFLOW = 30
    NOT_PERMITTED = 31
    PIPE = 32
    READ_ONLY = 33
    INVALID_SEEK = 34
    TEXT_FILE_BUSY = 35
    CROSS_DEVICE = 36

class Advice(Enum):
    NORMAL = 0
    SEQUENTIAL = 1
    RANDOM = 2
    WILL_NEED = 3
    DONT_NEED = 4
    NO_REUSE = 5

@dataclass
class MetadataHashValue:
    lower: int
    upper: int

class DirectoryEntryStream:
    
    def read_directory_entry(self) -> Optional[DirectoryEntry]:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
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


class Descriptor:
    
    def read_via_stream(self, offset: int) -> streams.InputStream:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def write_via_stream(self, offset: int) -> streams.OutputStream:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def append_via_stream(self) -> streams.OutputStream:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def advise(self, offset: int, length: int, advice: Advice) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def sync_data(self) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def get_flags(self) -> DescriptorFlags:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def get_type(self) -> DescriptorType:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def set_size(self, size: int) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def set_times(self, data_access_timestamp: NewTimestamp, data_modification_timestamp: NewTimestamp) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def read(self, length: int, offset: int) -> Tuple[bytes, bool]:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def write(self, buffer: bytes, offset: int) -> int:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def read_directory(self) -> DirectoryEntryStream:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def sync(self) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def create_directory_at(self, path: str) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def stat(self) -> DescriptorStat:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def stat_at(self, path_flags: PathFlags, path: str) -> DescriptorStat:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def set_times_at(self, path_flags: PathFlags, path: str, data_access_timestamp: NewTimestamp, data_modification_timestamp: NewTimestamp) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def link_at(self, old_path_flags: PathFlags, old_path: str, new_descriptor: Self, new_path: str) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def open_at(self, path_flags: PathFlags, path: str, open_flags: OpenFlags, flags: DescriptorFlags) -> Self:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def readlink_at(self, path: str) -> str:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def remove_directory_at(self, path: str) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def rename_at(self, old_path: str, new_descriptor: Self, new_path: str) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def symlink_at(self, old_path: str, new_path: str) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def unlink_file_at(self, path: str) -> None:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def is_same_object(self, other: Self) -> bool:
        raise NotImplementedError
    def metadata_hash(self) -> MetadataHashValue:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
        """
        raise NotImplementedError
    def metadata_hash_at(self, path_flags: PathFlags, path: str) -> MetadataHashValue:
        """
        Raises: `wit_world.types.Err(wit_world.imports.types.ErrorCode)`
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



def filesystem_error_code(err: error.Error) -> Optional[ErrorCode]:
    raise NotImplementedError

