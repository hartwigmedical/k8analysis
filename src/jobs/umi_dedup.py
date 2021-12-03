import logging
from dataclasses import dataclass

from jobs.base import JobABC, JobType
from services.gcp.base import GCPPath
from services.service_provider_abc import ServiceProviderABC


@dataclass(frozen=True)
class UmiDedupJob(JobABC):
    input_path: GCPPath
    output_path: GCPPath

    @classmethod
    def get_job_type(cls) -> JobType:
        return JobType.UMI_DEDUP

    def execute(self, service_provider: ServiceProviderABC) -> None:
        logging.info(f"Starting {self.get_job_type().get_job_name()} job")
        logging.info(f"Settings:")
        logging.info(f"    input_path  = {self.input_path}")
        logging.info(f"    output_path = {self.output_path}")

        gcp_client = service_provider.get_gcp_client()
        gcp_file_cache = service_provider.get_gcp_file_cache()
        bash_tool_box = service_provider.get_bash_toolbox()

        if gcp_client.file_exists(self.output_path):
            logging.info("Skipping job. Output file already exists in bucket")
            return

        logging.info("Starting download of input files")
        gcp_file_cache.multiple_download_to_local([self.input_path, self.input_path.append_suffix(".bai")])
        logging.info("Finished download of input files")

        local_input_path = gcp_file_cache.get_local_path(self.input_path)
        local_output_path = gcp_file_cache.get_local_path(self.output_path)

        logging.info("Starting deduplication")
        bash_tool_box.deduplicate_with_umi(local_input_path, local_output_path)
        logging.info("Finished deduplication")

        logging.info("Starting creation of bam index")
        bash_tool_box.create_bam_index(local_output_path)
        logging.info("Finished  creation of bam  index")

        logging.info("Starting upload of output files")
        gcp_file_cache.multiple_upload_from_local([self.output_path, self.output_path.append_suffix(".bai")])
        logging.info("Finished upload of output files")

        logging.info(f"Finished {self.get_job_type().get_job_name()} job")
