from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud import storage

    from config import Config
    from services.arg_parser import ArgumentParser
    from services.bash_toolbox import BashToolbox
    from services.gcp.client import GCPClient
    from services.gcp.file_cache import GCPFileCache


class ServiceProviderABC(ABC):
    """ABC for ServiceProvider class."""

    @abstractmethod
    def get_config(self) -> Config:
        raise NotImplementedError()

    @abstractmethod
    def get_gcp_file_cache(self) -> GCPFileCache:
        raise NotImplementedError()

    @abstractmethod
    def get_gcp_client(self) -> GCPClient:
        raise NotImplementedError()

    @abstractmethod
    def get_library_gcp_client(self) -> storage.Client:
        raise NotImplementedError()

    @abstractmethod
    def get_bash_toolbox(self) -> BashToolbox:
        raise NotImplementedError()

    @abstractmethod
    def get_argument_parser(self) -> ArgumentParser:
        raise NotImplementedError()
