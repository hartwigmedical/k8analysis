from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Set


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
    def execute(self) -> None:
        pass


@dataclass(frozen=True)
class AlignJob(Job):
    input_path: str
    ref_genome: str
    output_path: str

    @classmethod
    def get_job_type(cls) -> JobType:
        return JobType.ALIGN

    def execute(self) -> None:
        raise NotImplementedError(f"Implement this!")