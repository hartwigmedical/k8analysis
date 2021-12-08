from dataclasses import dataclass
from pathlib import Path
from typing import List

from services.gcp.base import GCPPath
from services.gcp.file_cache import GCPFileCache


@dataclass(frozen=True, order=True)
class LocalFastqPair(object):
    pair_name: str
    read1: Path
    read2: Path


@dataclass(frozen=True, order=True)
class GCPFastqPair(object):
    pair_name: str
    read1: GCPPath
    read2: GCPPath

    def get_local_version(self, gcp_file_cache: GCPFileCache) -> LocalFastqPair:
        local_read1 = gcp_file_cache.get_local_path(self.read1)
        local_read2 = gcp_file_cache.get_local_path(self.read2)
        return LocalFastqPair(self.pair_name, local_read1, local_read2)


class FastqPairMatcher(object):
    READ1_FASTQ_SUBSTRING = "_R1_"
    READ2_FASTQ_SUBSTRING = "_R2_"
    READ_PAIR_FASTQ_SUBSTRING = "_R?_"

    def pair_up_gcp_fastq_paths(self, fastq_gcp_paths: List[GCPPath]) -> List[GCPFastqPair]:
        pair_name_to_read1 = {}
        pair_name_to_read2 = {}

        for fastq_gcp_path in fastq_gcp_paths:
            fastq_file_name = fastq_gcp_path.relative_path.split("/")[-1]
            read1_substring_count = fastq_file_name.count(self.READ1_FASTQ_SUBSTRING)
            read2_substring_count = fastq_file_name.count(self.READ2_FASTQ_SUBSTRING)

            if read1_substring_count == 1 and read2_substring_count == 0:
                pair_name = fastq_file_name.replace(
                    self.READ1_FASTQ_SUBSTRING, self.READ_PAIR_FASTQ_SUBSTRING,
                ).split(".")[0]
                pair_name_to_read1[pair_name] = fastq_gcp_path
            elif read1_substring_count == 0 and read2_substring_count == 1:
                pair_name = fastq_file_name.replace(
                    self.READ2_FASTQ_SUBSTRING, self.READ_PAIR_FASTQ_SUBSTRING,
                ).split(".")[0]
                pair_name_to_read2[pair_name] = fastq_gcp_path
            else:
                raise ValueError(f"The FASTQ file is not marked clearly as read 1 or read 2: {fastq_gcp_path}")

        if set(pair_name_to_read1.keys()) != set(pair_name_to_read2.keys()):
            raise ValueError(f"Not all FASTQ files can be matched up in proper pairs of read 1 and read 2")

        fastq_pairs = [
            GCPFastqPair(pair_name, pair_name_to_read1[pair_name], pair_name_to_read2[pair_name])
            for pair_name in pair_name_to_read1.keys()
        ]
        fastq_pairs.sort()

        return fastq_pairs
