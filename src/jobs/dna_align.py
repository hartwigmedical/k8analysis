import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import List

from jobs.base import JobType, JobABC
from jobs.util import FastqPairMatcher, GCPFastqPair, LocalFastqPair
from services.gcp.base import GCPPath
from services.service_provider_abc import ServiceProviderABC
from util import create_or_cleanup_dir

READ1_FASTQ_SUBSTRING = "_R1_"
READ2_FASTQ_SUBSTRING = "_R2_"
READ_PAIR_FASTQ_SUBSTRING = "_R?_"

RECORD_GROUP_ID_REGEX = re.compile(r"(.*_){2}S[0-9]+_L[0-9]{3}_R[1-2].*")


@dataclass(frozen=True)
class DnaAlignJob(JobABC):
    input_path: GCPPath
    ref_genome: GCPPath
    output_path: GCPPath

    @classmethod
    def get_job_type(cls) -> JobType:
        return JobType.DNA_ALIGN

    def execute(self, service_provider: ServiceProviderABC) -> None:
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

        fastq_pairs = FastqPairMatcher().pair_up_gcp_fastq_paths(fastq_gcp_paths)
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

    def _do_alignment_locally(self, fastq_pairs: List[GCPFastqPair], service_provider: ServiceProviderABC) -> None:
        local_working_dir = service_provider.get_config().local_working_directory
        create_or_cleanup_dir(local_working_dir)

        local_lane_bams: List[Path] = []
        for fastq_pair in fastq_pairs:
            local_lane_bam = local_working_dir / f"{fastq_pair.pair_name}.bam"
            self._do_lane_alignment_locally(fastq_pair, local_lane_bam, service_provider)
            local_lane_bams.append(local_lane_bam)

        self._create_merged_bam_with_index(local_lane_bams, service_provider)

        create_or_cleanup_dir(local_working_dir)

    def _do_lane_alignment_locally(
            self, fastq_pair: GCPFastqPair, local_lane_bam: Path, service_provider: ServiceProviderABC,
    ) -> None:
        logging.info(f"Start creating lane bam {local_lane_bam}")

        gcp_file_cache = service_provider.get_gcp_file_cache()
        local_fastq_pair = fastq_pair.get_local_version(gcp_file_cache)
        local_reference_genome_path = gcp_file_cache.get_local_path(self.ref_genome)
        local_final_bam_path = gcp_file_cache.get_local_path(self.output_path)

        read_group_string = self._get_read_group_string(local_fastq_pair, local_final_bam_path)

        logging.info(f"Start alignment for lane bam {local_lane_bam}")
        service_provider.get_bash_toolbox().align_dna_bam(
            local_fastq_pair,
            local_reference_genome_path,
            local_lane_bam,
            read_group_string,
        )
        logging.info(f"Finished creating lane bam {local_lane_bam}")

    def _create_merged_bam_with_index(self, local_lane_bams: List[Path], service_provider: ServiceProviderABC) -> None:
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

    def _get_read_group_string(self, local_fastq_pair: LocalFastqPair, local_output_final_bam_path: Path) -> str:
        record_group_id = local_fastq_pair.read1.name.split(".")[0]
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
