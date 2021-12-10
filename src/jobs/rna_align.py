import logging
from dataclasses import dataclass
from typing import List

from jobs.base import JobType, JobABC
from jobs.util import FastqPairMatcher, GCPFastqPair
from services.gcp.base import GCPPath
from services.service_provider_abc import ServiceProviderABC
from util import create_or_cleanup_dir


@dataclass(frozen=True)
class RnaAlignJob(JobABC):
    input_path: GCPPath
    ref_genome_resource_dir: GCPPath
    output_path: GCPPath

    @classmethod
    def get_job_type(cls) -> JobType:
        return JobType.RNA_ALIGN

    def execute(self, service_provider: ServiceProviderABC) -> None:
        logging.info(f"Starting {self.get_job_type().get_job_name()} job")
        logging.info(f"Settings:")
        logging.info(f"    input_path  = {self.input_path}")
        logging.info(f"    ref_genome  = {self.ref_genome_resource_dir}")
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

        logging.info(f"Searching for reference genome files to download: {self.ref_genome_resource_dir}")
        reference_genome_bucket_files = gcp_client.get_files_in_directory(self.ref_genome_resource_dir)
        if reference_genome_bucket_files:
            reference_genome_files_string = "\n".join(str(path) for path in reference_genome_bucket_files)
            logging.info(f"Identified reference genome files to download:\n{reference_genome_files_string}")
        else:
            error_msg = f"Could not find reference genome paths in bucket dir: {self.ref_genome_resource_dir}"
            raise ValueError(error_msg)

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

        gcp_file_cache = service_provider.get_gcp_file_cache()
        bash_tool_box = service_provider.get_bash_toolbox()
        local_fastq_pairs = [pair.get_local_version(gcp_file_cache) for pair in fastq_pairs]
        local_reference_resource_dir = gcp_file_cache.get_local_path(self.ref_genome_resource_dir)
        local_final_bam_path = gcp_file_cache.get_local_path(self.output_path)

        logging.info(f"Start creating unsorted bam")
        local_unsorted_bam = bash_tool_box.align_rna_bam(
            local_fastq_pairs,
            local_reference_resource_dir,
            local_working_dir,
        )
        logging.info(f"Finished creating unsorted bam {local_unsorted_bam}")

        logging.info(f"Start sorting bam {local_unsorted_bam} to create {local_final_bam_path}")
        bash_tool_box.sort_bam(local_unsorted_bam, local_final_bam_path)
        logging.info(f"Finished sorting bam {local_unsorted_bam} to create {local_final_bam_path}")

        logging.info(f"Start indexing bam {local_final_bam_path}")
        bash_tool_box.create_bam_index(local_final_bam_path)
        logging.info(f"Finished indexing bam {local_final_bam_path}")

        create_or_cleanup_dir(local_working_dir)
