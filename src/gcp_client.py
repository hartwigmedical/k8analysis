import fnmatch
from dataclasses import dataclass
from typing import List

from google.cloud import storage


@dataclass(frozen=True, order=True)
class GCPPath(object):
    bucket_name: str
    relative_path: str

    @classmethod
    def from_string(cls, path: str) -> "GCPPath":
        if not path.startswith("gs://"):
            raise ValueError(f"Path is not a GCP bucket path: {path}")
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


class GCPClient(object):
    def __init__(self) -> None:
        self._client = storage.Client()

    def file_exists(self, path: GCPPath) -> bool:
        return bool(self._get_blob(path).exists())

    def get_files_in_directory(self, path: GCPPath) -> List[GCPPath]:
        if path.relative_path[-1] != "/":
            prefix = path.relative_path + "/"
        else:
            prefix = path.relative_path
        blobs = self._get_bucket(path.bucket_name).list_blobs(prefix=prefix, delimiter="/")
        return [GCPPath(path.bucket_name, blob.name) for blob in blobs]

    def get_matching_paths(self, path: GCPPath) -> List[GCPPath]:
        matching_paths: List[GCPPath] = []
        prefix_to_match = path.relative_path.split("*")[0]
        for blob in self._get_bucket(path.bucket_name).list_blobs(prefix=prefix_to_match):
            if fnmatch.fnmatch(blob.name, path.relative_path):
                matching_paths.append(GCPPath(path.bucket_name, blob.name))
        return matching_paths

    def _get_blob(self, path: GCPPath) -> storage.Blob:
        return self._get_bucket(path.bucket_name).blob(path.relative_path)

    def _get_bucket(self, bucket_name: str) -> storage.Bucket:
        return self._client.get_bucket(bucket_name)
