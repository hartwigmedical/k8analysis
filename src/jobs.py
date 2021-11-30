import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Set, Union

from gcp_client import GCPClient, GCPPath


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
    def execute(self, gcp_client: GCPClient) -> None:
        pass


@dataclass(frozen=True)
class AlignJob(Job):
    input_path: GCPPath
    ref_genome: Union[str, GCPPath]
    output_path: GCPPath

    @classmethod
    def get_job_type(cls) -> JobType:
        return JobType.ALIGN

    def execute(self, gcp_client: GCPClient) -> None:
        logging.info(f"Starting {self.get_job_type().get_job_name()} job")
        logging.info(f"Settings:")
        logging.info(f"    input_path  = {self.input_path}")
        logging.info(f"    ref_genome  = {self.ref_genome}")
        logging.info(f"    output_path = {self.output_path}")

        if gcp_client.file_exists(self.output_path):
            logging.info("Skipping job. Output file already exists in bucket")
            return

        raise NotImplementedError(f"Implement this!")

        logging.info(f"Finished {self.get_job_type().get_job_name()} job")
