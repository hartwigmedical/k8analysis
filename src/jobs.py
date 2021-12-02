import logging
import re
import shutil
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Set, List

from gcp_client import GCPPath
from service_provider import ServiceProvider

READ1_FASTQ_SUBSTRING = "_R1_"
READ2_FASTQ_SUBSTRING = "_R2_"
READ_PAIR_FASTQ_SUBSTRING = "_R?_"

LOCAL_WORKING_DIR = Path.home() / "local_lane_bam_dir"

RECORD_GROUP_ID_REGEX = re.compile(r"(.*_){2}S[0-9]+_L[0-9]{3}_R[1-2].*")


@dataclass(frozen=True, order=True)
class FastqPair(object):
    pair_name: str
    read1: GCPPath
    read2: GCPPath


class JobType(Enum):
    ALIGN = auto()

    @classmethod
    def get_type_names(cls) -> Set[str]:
        return {job_type.get_job_name() for job_type in cls}

    def get_job_name(self) -> str:
        return self.name.lower()


class Job(ABC):
    @classmethod
    @abstractmethod
    def get_job_type(cls) -> JobType:
        pass

    @abstractmethod
    def execute(self, service_provider: ServiceProvider) -> None:
        pass


@dataclass(frozen=True)
class AlignJob(Job):
    input_path: GCPPath
    ref_genome: GCPPath
    output_path: GCPPath

    @classmethod
    def get_job_type(cls) -> JobType:
        return JobType.ALIGN

    def execute(self, service_provider: ServiceProvider) -> None:
        logging.info(f"Starting {self.get_job_type().get_job_name()} job")
        logging.info(f"Settings:")
        logging.info(f"    input_path  = {self.input_path}")
        logging.info(f"    ref_genome  = {self.ref_genome}")
        logging.info(f"    output_path = {self.output_path}")

        gcp_client = service_provider.get_gcp_client()
        gcp_file_cache = service_provider.get_gcp_file_cache()

        if gcp_client.file_exists(self.output_path):
            logging.info("Skipping job. Output file already exists in bucket")
            return

        fastq_gcp_paths = gcp_client.get_matching_file_paths(self.input_path)

        if fastq_gcp_paths:
            matching_path_string = "\n".join(str(path) for path in fastq_gcp_paths)
            logging.info(f"Found FASTQ paths matching input path {self.input_path}:\n{matching_path_string}")
        else:
            raise ValueError(f"Could not find FASTQ paths matching the input path {self.input_path}")

        fastq_pairs = self._pair_up_fastq_paths(fastq_gcp_paths)
        paired_fastqs_string = "\n\n".join(
            f"Read1: {fastq_pair.read1}\nRead2: {fastq_pair.read2}" for fastq_pair in fastq_pairs
        )
        logging.info(f"The FASTQ paths have been paired up:\n{paired_fastqs_string}")

        logging.info(f"Searching for reference genome files to download: {self.ref_genome.get_parent_directory()}")
        reference_genome_bucket_files = gcp_client.get_files_in_directory(self.ref_genome.get_parent_directory())
        if reference_genome_bucket_files:
            reference_genome_files_string = "\n".join(str(path) for path in reference_genome_bucket_files)
            logging.info(f"Identified reference genome files to download:\n{reference_genome_files_string}")
        else:
            raise ValueError(f"Could not find reference genome paths matching the given path {self.ref_genome}")

        logging.info("Starting download of input files")
        gcp_file_cache.multiple_download_to_local(fastq_gcp_paths + reference_genome_bucket_files)
        logging.info("Finished download of input files")

        self._do_alignment_locally(fastq_pairs, service_provider)

        logging.info("Starting upload of output files")
        gcp_file_cache.multiple_upload_from_local([self.output_path, self.output_path.append_suffix(".bai")])
        logging.info("Finished upload of output files")

        logging.info(f"Finished {self.get_job_type().get_job_name()} job")

    def _pair_up_fastq_paths(self, fastq_gcp_paths: List[GCPPath]) -> List[FastqPair]:
        pair_name_to_read1 = {}
        pair_name_to_read2 = {}

        for fastq_gcp_path in fastq_gcp_paths:
            fastq_file_name = fastq_gcp_path.relative_path.split("/")[-1]
            read1_subtring_count = fastq_file_name.count(READ1_FASTQ_SUBSTRING)
            read2_subtring_count = fastq_file_name.count(READ2_FASTQ_SUBSTRING)

            if read1_subtring_count == 1 and read2_subtring_count == 0:
                pair_name = fastq_file_name.replace(READ1_FASTQ_SUBSTRING, READ_PAIR_FASTQ_SUBSTRING).split(".")[0]
                pair_name_to_read1[pair_name] = fastq_gcp_path
            elif read1_subtring_count == 0 and read2_subtring_count == 1:
                pair_name = fastq_file_name.replace(READ2_FASTQ_SUBSTRING, READ_PAIR_FASTQ_SUBSTRING).split(".")[0]
                pair_name_to_read2[pair_name] = fastq_gcp_path
            else:
                raise ValueError(f"The FASTQ file is not marked clearly as read 1 or read 2: {fastq_gcp_path}")

        if set(pair_name_to_read1.keys()) != set(pair_name_to_read2.keys()):
            raise ValueError(f"Not all FASTQ files can be matched up in proper pairs of read 1 and read 2")

        fastq_pairs = [
            FastqPair(pair_name, pair_name_to_read1[pair_name], pair_name_to_read2[pair_name])
            for pair_name in pair_name_to_read1.keys()
        ]
        fastq_pairs.sort()

        return fastq_pairs

    def _do_alignment_locally(self, fastq_pairs: List[FastqPair], service_provider: ServiceProvider) -> None:
        if LOCAL_WORKING_DIR.is_dir():
            LOCAL_WORKING_DIR.rmdir()
        elif LOCAL_WORKING_DIR.exists():
            LOCAL_WORKING_DIR.unlink()
        LOCAL_WORKING_DIR.mkdir(parents=True)

        local_lane_bams: List[Path] = []
        for fastq_pair in fastq_pairs:
            local_lane_bam = LOCAL_WORKING_DIR / f"{fastq_pair.pair_name}.bam"
            self._do_lane_alignment_locally(fastq_pair, local_lane_bam, service_provider)
            local_lane_bams.append(local_lane_bam)

        self._create_merged_bam_with_index(local_lane_bams, service_provider)

        LOCAL_WORKING_DIR.rmdir()

    def _create_merged_bam_with_index(self, local_lane_bams: List[Path], service_provider: ServiceProvider) -> None:
        local_final_bam_path = service_provider.get_gcp_file_cache().get_local_path(self.output_path)

        bash_toolbox = service_provider.get_bash_toolbox()
        if len(local_lane_bams) == 1:
            logging.info("Only one lane bam, so lane bam is merged bam.")
            shutil.move(str(local_lane_bams[0]), str(local_final_bam_path))
        else:
            logging.info("Start merging lane bams")
            bash_toolbox.merge_bams(local_lane_bams, local_final_bam_path)
            logging.info("Finished merging lane bams")

        logging.info("Start creating index for merged bam")
        bash_toolbox.create_bam_index(local_final_bam_path)
        logging.info("Finished creating index for merged bam")

    def _do_lane_alignment_locally(
            self, fastq_pair: FastqPair, local_lane_bam: Path, service_provider: ServiceProvider,
    ) -> None:
        logging.info(f"Start creating lane bam {local_lane_bam}")

        gcp_file_cache = service_provider.get_gcp_file_cache()

        local_read1_fastq_path = gcp_file_cache.get_local_path(fastq_pair.read1)
        local_read2_fastq_path = gcp_file_cache.get_local_path(fastq_pair.read2)
        local_reference_genome_path = gcp_file_cache.get_local_path(self.ref_genome)
        local_final_bam_path = gcp_file_cache.get_local_path(self.output_path)

        read_group_string = self._get_read_group_string(local_read1_fastq_path, local_final_bam_path)

        logging.info(f"Start alignment for lane bam {local_lane_bam}")
        service_provider.get_bash_toolbox().align_dna_bam(
            local_read1_fastq_path,
            local_read2_fastq_path,
            local_reference_genome_path,
            local_lane_bam,
            read_group_string,
        )
        logging.info(f"Finished creating lane bam {local_lane_bam}")

    def _get_read_group_string(self, local_read1_fastq_path: Path, local_output_final_bam_path: Path) -> str:
        record_group_id = local_read1_fastq_path.name.split(".")[0]
        if not RECORD_GROUP_ID_REGEX.match(record_group_id):
            error_msg = (
                f"Record group ID '{record_group_id}' does not match "
                f"the required regex '{RECORD_GROUP_ID_REGEX.pattern}'"
            )
            raise ValueError(error_msg)
        sample_name = local_output_final_bam_path.name.split(".")[0]
        flowcell_id = record_group_id.split("_")[1]

        read_group_string = (
            f"@RG\\tID:{record_group_id}\\tLB:{sample_name}\\tPL:ILLUMINA\\tPU:{flowcell_id}\\tSM:{sample_name}"
        )
        return read_group_string
