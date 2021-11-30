import argparse
import logging
import re
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Set, Pattern

REF_GENOME_37_ARGUMENT = "37"
REF_GENOME_38_ARGUMENT = "38"

BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9/._-]+$")
BAM_BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9/._-]+\.bam$")
WILDCARD_FASTQ_BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9*/._-]+\.fastq\.gz$")


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


def main(arguments: List[str]) -> None:
    set_up_logging()

    logging.info("Starting k8analysis.")

    logging.info("Extracting jobs from arguments.")
    jobs = extract_jobs(arguments)

    if jobs:
        for job in jobs:
            job.execute()
    else:
        logging.warning("No jobs detected.")

    logging.info("Finished k8analysis.")


def extract_jobs(arguments: List[str]) -> List[Job]:
    jobs: List[Job] = []

    while arguments:
        job_type_arg = arguments.pop(0)
        job_type = JobType[job_type_arg.upper()]
        logging.info(f"Detected job of type: {job_type.get_job_name()}.")
        job_args: List[str] = []
        while arguments and arguments[0] not in JobType.get_type_names():
            job_args.append(arguments.pop(0))
        job = parse_job(job_type, job_args)
        jobs.append(job)

    return jobs


def parse_job(job_type: JobType, job_args: List[str]) -> Job:
    if job_type == JobType.ALIGN:
        job = parse_align_job(job_args)
    else:
        raise NotImplementedError(f"Unimplemented job type: {job_type}.")
    return job


def parse_align_job(job_args: List[str]) -> AlignJob:
    parser = argparse.ArgumentParser(
        prog=JobType.ALIGN.get_job_name(),
        description="Run bwa mem alignment of paired reads at GCP.",
    )
    input_help = (
        "Wildcard path to the fastqs files that will be aligned, e.g. 'gs://some-kind/of/path*.fastq.gz'. "
        "Make sure that for each read pair the file path for read 1 contains '_R1_' exactly once and "
        "'_R2_' zero times, and that the file path for read 2 contains '_R2_' exactly once and '_R1_' zero times."
    )
    ref_genome_help = (
        "Reference genome version to align to. "
        "Either '37', '38', or some GCP bucket path to a FASTA file, e.g. 'gs://some/kind/of/path'."
    )
    output_help = (
        "Path in bucket to which the bam will be written, e.g. 'gs://some-other-kind/of/path.bam'. "
        "Will also output an index file, e.g. 'gs://some-other-kind/of/path.bam.bai'."
    )
    parser.add_argument("--input", "-i", type=parse_wildcard_fastq_bucket_path, required=True, help=input_help)
    parser.add_argument("--ref-genome", "-r", type=parse_reference_genome_value, required=True, help=ref_genome_help)
    parser.add_argument("--output", "-o", type=parse_bam_bucket_path, required=True, help=output_help)

    parsed_args = parser.parse_args(job_args)

    return AlignJob(parsed_args.input, parsed_args.ref_genome, parsed_args.output)


def parse_wildcard_fastq_bucket_path(arg_value: str) -> str:
    assert_argument_matches_regex(arg_value, WILDCARD_FASTQ_BUCKET_PATH_REGEX)
    return arg_value


def parse_bam_bucket_path(arg_value: str) -> str:
    assert_argument_matches_regex(arg_value, BAM_BUCKET_PATH_REGEX)
    return arg_value


def parse_reference_genome_value(arg_value: str) -> str:
    arg_value_format_recognized = (
        arg_value == REF_GENOME_37_ARGUMENT
        or arg_value == REF_GENOME_38_ARGUMENT
        or BUCKET_PATH_REGEX.match(arg_value)
    )
    if not arg_value_format_recognized:
        error_msg = (
            f"Value '{arg_value}' does not match '{REF_GENOME_37_ARGUMENT}', '{REF_GENOME_38_ARGUMENT}' "
            f"or regex '{BUCKET_PATH_REGEX.pattern}'."
        )
        raise argparse.ArgumentTypeError(error_msg)
    return arg_value


def assert_argument_matches_regex(arg_value: str, pattern: Pattern) -> None:
    if not pattern.match(arg_value):
        raise argparse.ArgumentTypeError(f"Value '{arg_value}' does not match the regex pattern '{pattern.pattern}'.")


def set_up_logging() -> None:
    logging.basicConfig(
        format="%(asctime)s - [%(levelname)-8s] - %(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )


if __name__ == '__main__':
    main(sys.argv[1:])
