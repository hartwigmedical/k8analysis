from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Set

from service_provider import ServiceProvider


class JobType(Enum):
    ALIGN = auto()

    @classmethod
    def get_type_names(cls) -> Set[str]:
        return {job_type.get_job_name() for job_type in cls}

    def get_job_name(self) -> str:
        return self.name.lower()


class JobABC(ABC):
    @classmethod
    @abstractmethod
    def get_job_type(cls) -> JobType:
        pass

    @abstractmethod
    def execute(self, service_provider: ServiceProvider) -> None:
        pass