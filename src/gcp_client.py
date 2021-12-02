import concurrent.futures
import fnmatch
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List

from google.cloud import storage


@dataclass(frozen=True, order=True)
class GCPPath(object):
    bucket_name: str
    relative_path: str

    @classmethod
    def from_string(cls, path: str) -> "GCPPath":
        if not path.startswith("gs://"):
            raise ValueError(f"Path is not a GCP bucket path: '{path}'")
        bucket_name = path.split("/")[2]
        relative_path = "/".join(path.split("/")[3:])
        return GCPPath(bucket_name, relative_path)

    def __str__(self) -> str:
        return f"gs://{self.bucket_name}/{self.relative_path}"

    def get_parent_directory(self) -> "GCPPath":
        if self.relative_path[-1] == "/":
            relative_path = self.relative_path[:-1]
        else:
            relative_path = self.relative_path
        parent_directory_relative_path = "/".join(relative_path.split("/")[:-1])
        return GCPPath(self.bucket_name, parent_directory_relative_path)

    def append_suffix(self, suffix: str) -> "GCPPath":
        return GCPPath(self.bucket_name, self.relative_path + suffix)


@dataclass(frozen=True)
class GCPClient(object):
    client: storage.Client

    def file_exists(self, path: GCPPath) -> bool:
        return bool(self._get_blob(path).exists())

    def download_file(self, gcp_path: GCPPath, local_path: Path) -> None:
        logging.info(f"Starting download of '{gcp_path}' to '{local_path}'.")
        if not self.file_exists(gcp_path):
            raise FileNotFoundError(f"Cannot download file that doesn't exist: {gcp_path}")
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self._get_blob(gcp_path).download_to_filename(str(local_path))
        if not local_path.exists():
            raise FileNotFoundError(f"Download of '{gcp_path}' to '{local_path}' has failed.")
        logging.info(f"Finished download of '{gcp_path}' to '{local_path}'.")

    def upload_file(self, local_path: Path, gcp_path: GCPPath) -> None:
        logging.info(f"Starting upload of '{local_path}' to '{gcp_path}'.")
        if not local_path.exists():
            raise FileNotFoundError(f"Cannot upload file that doesn't exist: '{local_path}'")
        self._get_blob(gcp_path).upload_from_filename(str(local_path))
        if not self.file_exists(gcp_path):
            raise FileNotFoundError(f"Upload of '{local_path}' to '{gcp_path}' has failed.")
        logging.info(f"Finished upload of '{local_path}' to '{gcp_path}'.")

    def get_files_in_directory(self, path: GCPPath) -> List[GCPPath]:
        if path.relative_path[-1] != "/":
            prefix = path.relative_path + "/"
        else:
            prefix = path.relative_path
        blobs = self._get_bucket(path.bucket_name).list_blobs(prefix=prefix, delimiter="/")
        return [GCPPath(path.bucket_name, blob.name) for blob in blobs]

    def get_matching_file_paths(self, path: GCPPath) -> List[GCPPath]:
        matching_paths: List[GCPPath] = []
        prefix_to_match = path.relative_path.split("*")[0]
        for blob in self._get_bucket(path.bucket_name).list_blobs(prefix=prefix_to_match):
            if fnmatch.fnmatch(blob.name, path.relative_path):
                matching_paths.append(GCPPath(path.bucket_name, blob.name))
        return matching_paths

    def _get_blob(self, path: GCPPath) -> storage.Blob:
        return self._get_bucket(path.bucket_name).blob(path.relative_path)

    def _get_bucket(self, bucket_name: str) -> storage.Bucket:
        return self.client.get_bucket(bucket_name)


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
            futures = []
            for gcp_path in gcp_paths:
                logging.info(f"Submitting download of '{gcp_path}'")
                futures.append(executor.submit(self.download_to_local, gcp_path))

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                logging.info(f"Finished download of '{gcp_path}' with result '{result}'")
            except Exception as exc:
                raise ValueError(exc)

    def multiple_upload_from_local(self, gcp_paths: List[GCPPath]) -> None:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for gcp_path in gcp_paths:
                logging.info(f"Submitting upload to '{gcp_path}'")
                futures.append(executor.submit(self.upload_from_local, gcp_path))

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
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
