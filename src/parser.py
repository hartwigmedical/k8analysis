import argparse
import logging
import re
from typing import List, Pattern

from jobs import JobType, Job, AlignJob


class Parser(object):
    REF_GENOME_37_ARGUMENT = "37"
    REF_GENOME_38_ARGUMENT = "38"
    BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9/._-]+$")
    BAM_BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9/._-]+\.bam$")
    WILDCARD_FASTQ_BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9*/._-]+\.fastq\.gz$")

    @classmethod
    def extract_jobs(cls, arguments: List[str]) -> List[Job]:
        jobs: List[Job] = []

        while arguments:
            job_type_arg = arguments.pop(0)
            job_type = JobType[job_type_arg.upper()]
            logging.info(f"Detected job of type: {job_type.get_job_name()}.")
            job_args: List[str] = []
            while arguments and arguments[0] not in JobType.get_type_names():
                job_args.append(arguments.pop(0))
            job = cls.parse_job(job_type, job_args)
            jobs.append(job)

        return jobs

    @classmethod
    def parse_job(cls, job_type: JobType, job_args: List[str]) -> Job:
        if job_type == JobType.ALIGN:
            job = cls.parse_align_job(job_args)
        else:
            raise NotImplementedError(f"Unimplemented job type: {job_type}.")
        return job

    @classmethod
    def parse_align_job(cls, job_args: List[str]) -> AlignJob:
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
        parser.add_argument(
            "--input", "-i", type=cls.parse_wildcard_fastq_bucket_path, required=True, help=input_help,
        )
        parser.add_argument(
            "--ref-genome", "-r", type=cls.parse_reference_genome_value, required=True, help=ref_genome_help,
        )
        parser.add_argument(
            "--output", "-o", type=cls.parse_bam_bucket_path, required=True, help=output_help,
        )

        parsed_args = parser.parse_args(job_args)

        return AlignJob(parsed_args.input, parsed_args.ref_genome, parsed_args.output)

    @classmethod
    def parse_wildcard_fastq_bucket_path(cls, arg_value: str) -> str:
        cls.assert_argument_matches_regex(arg_value, cls.WILDCARD_FASTQ_BUCKET_PATH_REGEX)
        return arg_value

    @classmethod
    def parse_bam_bucket_path(cls, arg_value: str) -> str:
        cls.assert_argument_matches_regex(arg_value, cls.BAM_BUCKET_PATH_REGEX)
        return arg_value

    @classmethod
    def parse_reference_genome_value(cls, arg_value: str) -> str:
        arg_value_format_recognized = (
            arg_value == cls.REF_GENOME_37_ARGUMENT
            or arg_value == cls.REF_GENOME_38_ARGUMENT
            or cls.BUCKET_PATH_REGEX.match(arg_value)
        )
        if not arg_value_format_recognized:
            error_msg = (
                f"Value '{arg_value}' does not match '{cls.REF_GENOME_37_ARGUMENT}', '{cls.REF_GENOME_38_ARGUMENT}' "
                f"or regex '{cls.BUCKET_PATH_REGEX.pattern}'."
            )
            raise argparse.ArgumentTypeError(error_msg)
        return arg_value

    @classmethod
    def assert_argument_matches_regex(cls, arg_value: str, pattern: Pattern) -> None:
        if not pattern.match(arg_value):
            error_msg = f"Value '{arg_value}' does not match the regex pattern '{pattern.pattern}'."
            raise argparse.ArgumentTypeError(error_msg)