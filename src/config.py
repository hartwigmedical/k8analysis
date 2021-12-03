from dataclasses import dataclass
from pathlib import Path

DEFAULT_LOCAL_GCP_FILE_CACHE_DIRECTORY = Path.home() / "gcp_local_file_cache"


@dataclass(frozen=True)
class Config(object):
    local_gcp_file_cache_directory: Path = DEFAULT_LOCAL_GCP_FILE_CACHE_DIRECTORY
