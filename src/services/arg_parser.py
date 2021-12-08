import argparse
import logging
import re
import shlex
from dataclasses import dataclass
from typing import List, Pattern

from jobs.dna_align import DnaAlignJob
from jobs.base import JobType, JobABC
from jobs.count_mapping_coords import CountMappingCoordsJob
from jobs.flagstat import FlagstatJob
from jobs.non_umi_dedup import NonUmiDedupJob
from jobs.rna_align import RnaAlignJob
from jobs.umi_dedup import UmiDedupJob
from services.gcp.base import GCPPath


@dataclass(frozen=True)
class ArgumentParser(object):
    """Parse command line arguments and extract jobs from them."""
    REF_GENOME_37_ARGUMENT = "37"
    REF_GENOME_38_ARGUMENT = "38"

    REF_GENOME_37_BUCKET_FASTA_PATH = (
        "gs://common-resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
    )
    REF_GENOME_38_BUCKET_FASTA_PATH = (
        "gs://common-resources/reference_genome/38/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna"
    )

    BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9/._-]+$")
    BAM_BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9/._-]+\.bam$")
    FLAGSTAT_BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9/._-]+\.flagstat$")
    TXT_BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9/._-]+\.txt$")
    WILDCARD_FASTQ_BUCKET_PATH_REGEX = re.compile(r"^gs://[a-zA-Z0-9*/._-]+\.fastq\.gz$")

    def extract_jobs(self, arguments_string: str) -> List[JobABC]:
        arguments = shlex.split(arguments_string)

        jobs: List[JobABC] = []
        while arguments:
            job_type = self._get_job_type(arguments.pop(0))
            logging.info(f"Detected job of type: {job_type.get_job_name()}.")
            job_args: List[str] = []
            while arguments and arguments[0] not in JobType.get_type_names():
                job_args.append(arguments.pop(0))
            job = self._parse_job(job_type, job_args)
            jobs.append(job)

        return jobs

    def _parse_job(self, job_type: JobType, job_args: List[str]) -> JobABC:
        job: JobABC
        if job_type == JobType.DNA_ALIGN:
            job = self._parse_dna_align_job(job_args)
        elif job_type == JobType.COUNT_MAPPING_COORDS:
            job = self._parse_count_mapping_coords_job(job_args)
        elif job_type == JobType.FLAGSTAT:
            job = self._parse_flagstat_job(job_args)
        elif job_type == JobType.NON_UMI_DEDUP:
            job = self._parse_non_umi_dedup_job(job_args)
        elif job_type == JobType.RNA_ALIGN:
            job = self._parse_rna_align_job(job_args)
        elif job_type == JobType.UMI_DEDUP:
            job = self._parse_umi_dedup_job(job_args)
        else:
            raise NotImplementedError(f"Unimplemented job type: {job_type}.")
        return job

    def _parse_dna_align_job(self, job_args: List[str]) -> DnaAlignJob:
        parser = argparse.ArgumentParser(
            prog=JobType.DNA_ALIGN.get_job_name(),
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
            "--input", "-i", type=self._parse_wildcard_fastq_gcp_path, required=True, help=input_help,
        )
        parser.add_argument(
            "--ref-genome", "-r", type=self._parse_reference_genome_value, required=True, help=ref_genome_help,
        )
        parser.add_argument(
            "--output", "-o", type=self._parse_bam_gcp_path, required=True, help=output_help,
        )

        parsed_args = parser.parse_args(job_args)

        return DnaAlignJob(parsed_args.input, parsed_args.ref_genome, parsed_args.output)

    def _parse_count_mapping_coords_job(self, job_args: List[str]) -> CountMappingCoordsJob:
        parser = argparse.ArgumentParser(
            prog=JobType.COUNT_MAPPING_COORDS.get_job_name(),
            description="Count mapping coordinates at GCP.",
        )
        input_help = (
            "Path to the bam file of which mapping coordinates will be counted, e.g. gs://some-kind/of/path.bam ."
            "Make sure that an index file is also available, e.g. gs://some-kind/of/path.bam.bai ."
        )
        output_help = (
            "Path in bucket to which count will be uploaded, e.g. gs://some-other-kind/of/path.txt ."
        )
        parser.add_argument(
            "--input", "-i", type=self._parse_bam_gcp_path, required=True, help=input_help,
        )
        parser.add_argument(
            "--output", "-o", type=self._parse_txt_gcp_path, required=True, help=output_help,
        )

        parsed_args = parser.parse_args(job_args)

        return CountMappingCoordsJob(parsed_args.input, parsed_args.output)

    def _parse_flagstat_job(self, job_args: List[str]) -> FlagstatJob:
        parser = argparse.ArgumentParser(
            prog=JobType.FLAGSTAT.get_job_name(),
            description="Run sambamba flagstat at GCP.",
        )
        input_help = (
            "Path to the bam file on which sambamba flagstat will be run, e.g. gs://some-kind/of/path.bam ."
            "Make sure that an index file is also available, e.g. gs://some-kind/of/path.bam.bai ."
        )
        output_help = (
            "Path in bucket to which flagstat file will be written, e.g. gs://some-other-kind/of/path.flagstat ."
        )
        parser.add_argument(
            "--input", "-i", type=self._parse_bam_gcp_path, required=True, help=input_help,
        )
        parser.add_argument(
            "--output", "-o", type=self._parse_flagstat_gcp_path, required=True, help=output_help,
        )

        parsed_args = parser.parse_args(job_args)

        return FlagstatJob(parsed_args.input, parsed_args.output)

    def _parse_non_umi_dedup_job(self, job_args: List[str]) -> NonUmiDedupJob:
        parser = argparse.ArgumentParser(
            prog=JobType.NON_UMI_DEDUP.get_job_name(),
            description="Run sambamba markdup at GCP.",
        )
        input_help = (
            "Path to the bam file on which deduplication will be run, e.g. gs://some-kind/of/path.bam ."
            "Make sure that an index file is also available, e.g. gs://some-kind/of/path.bam.bai ."
        )
        output_help = (
            "Path in bucket to which the deduplicated bam will be written, e.g. gs://some-other-kind/of/path.bam ."
            "Will also output an index file, e.g. gs://some-other-kind/of/path.bam.bai ."
        )
        parser.add_argument(
            "--input", "-i", type=self._parse_bam_gcp_path, required=True, help=input_help,
        )
        parser.add_argument(
            "--output", "-o", type=self._parse_bam_gcp_path, required=True, help=output_help,
        )

        parsed_args = parser.parse_args(job_args)

        return NonUmiDedupJob(parsed_args.input, parsed_args.output)

    def _parse_rna_align_job(self, job_args: List[str]) -> RnaAlignJob:
        parser = argparse.ArgumentParser(
            prog=JobType.RNA_ALIGN.get_job_name(),
            description="Run STAR alignment of paired RNA reads at GCP.",
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
            "--input", "-i", type=self._parse_wildcard_fastq_gcp_path, required=True, help=input_help,
        )
        parser.add_argument(
            "--ref-genome", "-r", type=self._parse_reference_genome_value, required=True, help=ref_genome_help,
        )
        parser.add_argument(
            "--output", "-o", type=self._parse_bam_gcp_path, required=True, help=output_help,
        )

        parsed_args = parser.parse_args(job_args)

        return RnaAlignJob(parsed_args.input, parsed_args.ref_genome, parsed_args.output)

    def _parse_umi_dedup_job(self, job_args: List[str]) -> UmiDedupJob:
        parser = argparse.ArgumentParser(
            prog=JobType.UMI_DEDUP.get_job_name(),
            description="Run UMI-Collapse dedupping at GCP.",
        )
        input_help = (
            "Path to the bam file on which deduplication will be run, e.g. gs://some-kind/of/path.bam ."
            "Make sure that an index file is also available, e.g. gs://some-kind/of/path.bam.bai ."
        )
        output_help = (
            "Path in bucket to which the deduplicated bam will be written, e.g. gs://some-other-kind/of/path.bam ."
            "Will also output an index file, e.g. gs://some-other-kind/of/path.bam.bai ."
        )
        parser.add_argument(
            "--input", "-i", type=self._parse_bam_gcp_path, required=True, help=input_help,
        )
        parser.add_argument(
            "--output", "-o", type=self._parse_bam_gcp_path, required=True, help=output_help,
        )

        parsed_args = parser.parse_args(job_args)

        return UmiDedupJob(parsed_args.input, parsed_args.output)

    def _parse_wildcard_fastq_gcp_path(self, arg_value: str) -> GCPPath:
        self._assert_argument_matches_regex(arg_value, self.WILDCARD_FASTQ_BUCKET_PATH_REGEX)
        return GCPPath.from_string(arg_value)

    def _parse_bam_gcp_path(self, arg_value: str) -> GCPPath:
        self._assert_argument_matches_regex(arg_value, self.BAM_BUCKET_PATH_REGEX)
        return GCPPath.from_string(arg_value)

    def _parse_flagstat_gcp_path(self, arg_value: str) -> GCPPath:
        self._assert_argument_matches_regex(arg_value, self.FLAGSTAT_BUCKET_PATH_REGEX)
        return GCPPath.from_string(arg_value)

    def _parse_txt_gcp_path(self, arg_value: str) -> GCPPath:
        self._assert_argument_matches_regex(arg_value, self.TXT_BUCKET_PATH_REGEX)
        return GCPPath.from_string(arg_value)

    def _parse_reference_genome_value(self, arg_value: str) -> GCPPath:
        arg_value_format_recognized = (
            arg_value == self.REF_GENOME_37_ARGUMENT
            or arg_value == self.REF_GENOME_38_ARGUMENT
            or self.BUCKET_PATH_REGEX.match(arg_value)
        )
        if not arg_value_format_recognized:
            error_msg = (
                f"Value '{arg_value}' does not match '{self.REF_GENOME_37_ARGUMENT}', '{self.REF_GENOME_38_ARGUMENT}' "
                f"or regex '{self.BUCKET_PATH_REGEX.pattern}'."
            )
            raise argparse.ArgumentTypeError(error_msg)

        if arg_value == self.REF_GENOME_37_ARGUMENT:
            bucket_fasta_path = self.REF_GENOME_37_BUCKET_FASTA_PATH
        elif arg_value == self.REF_GENOME_38_ARGUMENT:
            bucket_fasta_path = self.REF_GENOME_38_BUCKET_FASTA_PATH
        else:
            # arg_value is itself a GCP bucket path
            bucket_fasta_path = arg_value
        return GCPPath.from_string(bucket_fasta_path)

    def _assert_argument_matches_regex(self, arg_value: str, pattern: Pattern[str]) -> None:
        if not pattern.match(arg_value):
            error_msg = f"Value '{arg_value}' does not match the regex pattern '{pattern.pattern}'."
            raise argparse.ArgumentTypeError(error_msg)

    def _get_job_type(self, job_type_arg: str) -> JobType:
        try:
            job_type = JobType[job_type_arg.upper()]
        except KeyError:
            error_msg = f"Unrecognized job name '{job_type_arg}'. Recognized job names: {JobType.get_type_names()}"
            raise ValueError(error_msg)
        return job_type
