import concurrent.futures
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List

from services.gcp.base import GCPPath
from services.gcp.client import GCPClient


@dataclass(frozen=True)
class GCPFileCache(object):
    """
    Local cache of GCP files that mirrors the file structure in the buckets.

    It does this to avoid accidental name clashes, and to make sure the same file at GCP
    always gets the same local path, to help with caching.
    """
    local_directory: Path
    gcp_client: GCPClient

    SKIP_STATUS = "SKIP"
    SUCCESS_STATUS = "SUCCESS"

    def multiple_download_to_local(self, gcp_paths: List[GCPPath]) -> None:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_path = {}
            for gcp_path in gcp_paths:
                logging.info(f"Submitting download of '{gcp_path}'")
                future_to_path[executor.submit(self.download_to_local, gcp_path)] = gcp_path

            for future in concurrent.futures.as_completed(future_to_path.keys()):
                try:
                    result = future.result()
                    gcp_path = future_to_path[future]
                    logging.info(f"Finished download of '{gcp_path}' with result '{result}'")
                except Exception as exc:
                    raise ValueError(exc)

    def multiple_upload_from_local(self, gcp_paths: List[GCPPath]) -> None:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_path = {}
            for gcp_path in gcp_paths:
                logging.info(f"Submitting upload to '{gcp_path}'")
                future_to_path[executor.submit(self.upload_from_local, gcp_path)] = gcp_path

            for future in concurrent.futures.as_completed(future_to_path.keys()):
                try:
                    result = future.result()
                    gcp_path = future_to_path[future]
                    logging.info(f"Finished upload to '{gcp_path}' with result '{result}'")
                except Exception as exc:
                    raise ValueError(exc)

    def download_to_local(self, gcp_path: GCPPath) -> str:
        local_path = self.get_local_path(gcp_path)
        if local_path.exists():
            logging.info(f"Skipping download of '{gcp_path}' since it is already in the local file cache.")
            return self.SKIP_STATUS
        else:
            self.gcp_client.download_file(gcp_path, local_path)
            return self.SUCCESS_STATUS

    def upload_from_local(self, gcp_path: GCPPath) -> str:
        local_path = self.get_local_path(gcp_path)
        if self.gcp_client.file_exists(gcp_path):
            error_msg = f"Cannot upload file '{local_path}' from local file cache since this file already exists at GCP."
            raise FileExistsError(error_msg)
        else:
            self.gcp_client.upload_file(local_path, gcp_path)
            return self.SUCCESS_STATUS

    def get_local_path(self, gcp_path: GCPPath) -> Path:
        return self.local_directory / gcp_path.bucket_name / gcp_path.relative_path
