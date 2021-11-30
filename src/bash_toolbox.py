from pathlib import Path
from typing import List


class BashToolbox(object):
    """Class for running and combining bash commands and tools"""
    def align_dna_bam(
            self,
            local_read1_fastq_path: Path,
            local_read2_fastq_path: Path,
            local_reference_genome_path: Path,
            local_output_bam_path: Path,
            read_group_string: str,
    ) -> None:
        raise NotImplementedError()

    def merge_bams(self, local_input_bams: List[Path], local_output_bam: Path) -> None:
        raise NotImplementedError()

    def create_bam_index(self, local_bam_path: Path) -> None:
        raise NotImplementedError()
