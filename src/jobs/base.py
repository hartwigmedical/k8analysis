from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Set

from services.service_provider_abc import ServiceProviderABC


class JobType(Enum):
    ALIGN = auto()
    COUNT_MAPPING_COORDS = auto()
    FLAGSTAT = auto()
    NON_UMI_DEDUP = auto()
    UMI_DEDUP = auto()

    @classmethod
    def get_type_names(cls) -> Set[str]:
        return {job_type.get_job_name() for job_type in cls}

    def get_job_name(self) -> str:
        return self.name.lower()


class JobABC(ABC):
    @classmethod
    @abstractmethod
    def get_job_type(cls) -> JobType:
        raise NotImplementedError()

    @abstractmethod
    def execute(self, service_provider: ServiceProviderABC) -> None:
        raise NotImplementedError()
