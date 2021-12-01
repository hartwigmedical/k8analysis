from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from arg_parser import ArgumentParser
from bash_toolbox import BashToolbox
from gcp_client import GCPClient, GCPFileCache
from google.cloud import storage


@dataclass()
class ServiceProvider(object):
    """Service from which all singleton-like objects should be gotten."""
    local_gcp_file_cache_directory: Path
    _gcp_file_cache: Optional[GCPFileCache] = None
    _gcp_client: Optional[GCPClient] = None
    _library_gcp_client: Optional[storage.Client] = None
    _bash_toolbox: Optional[BashToolbox] = None
    _argument_parser: Optional[ArgumentParser] = None

    def get_gcp_file_cache(self) -> GCPFileCache:
        if self._gcp_file_cache is None:
            self._gcp_file_cache = GCPFileCache(self.local_gcp_file_cache_directory, self.get_gcp_client())
        return self._gcp_file_cache

    def get_gcp_client(self) -> GCPClient:
        if self._gcp_client is None:
            self._gcp_client = GCPClient(self.get_library_gcp_client())
        return self._gcp_client

    def get_library_gcp_client(self) -> storage.Client:
        if self._library_gcp_client is None:
            self._library_gcp_client = storage.Client()
        return self._library_gcp_client

    def get_bash_toolbox(self) -> BashToolbox:
        if self._bash_toolbox is None:
            self._bash_toolbox = BashToolbox()
        return self._bash_toolbox

    def get_argument_parser(self) -> ArgumentParser:
        if self._argument_parser is None:
            self._argument_parser = ArgumentParser()
        return self._argument_parser
