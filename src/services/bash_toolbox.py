import logging
import multiprocessing
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple, Union, TextIO

from jobs.util import LocalFastqPair
from util import create_parent_dir_if_not_exists


@dataclass(frozen=True)
class BashCommandResults(object):
    output: Optional[str]
    errors: Optional[str]
    status: int


class BashToolbox(object):
    """Class for running and combining bash commands and tools"""
    GSUTIL = "gsutil"
    JAVA = "java"
    BWA = Path.home() / "bwa"
    SAMBAMBA = Path.home() / "sambamba"
    STAR = Path.home() / "STAR-2.7.3a" / "bin" / "Linux_x86_64_static" / "STAR"
    UMI_COLLAPSE_JAR = Path.home() / "UMICollapse" / "umicollapse.jar"

    SAMBAMBA_MARKDUP_OVERFLOW_LIST_SIZE = 4500000

    OUTPUT_ENCODING = "utf-8"

    def align_dna_bam(
            self,
            local_fastq_pair: LocalFastqPair,
            local_reference_genome_path: Path,
            local_output_bam_path: Path,
            read_group_string: str,
    ) -> None:
        thread_count = self._get_thread_count()
        bwa_align_command = (
            f'"{self.BWA}" mem -Y -t {thread_count} -R "{read_group_string}" "{local_reference_genome_path}" '
            f'"{local_fastq_pair.read1}" "{local_fastq_pair.read2}"'
        )
        sam_to_bam_command = f'"{self.SAMBAMBA}" view -f "bam" -S -l 0 "/dev/stdin"'
        bam_sort_command = f'"{self.SAMBAMBA}" sort -o "{local_output_bam_path}" "/dev/stdin"'
        combined_command = " | ".join([bwa_align_command, sam_to_bam_command, bam_sort_command])

        create_parent_dir_if_not_exists(local_output_bam_path)
        self._run_bash_command(combined_command)

    def align_rna_bam(
            self,
            local_fastq_pairs: List[LocalFastqPair],
            local_reference_resource_dir: Path,
            local_working_dir: Path,
    ) -> Path:
        thread_count = self._get_thread_count()
        r1_files = ",".join(f'"{pair.read1}"' for pair in local_fastq_pairs)
        r2_files = ",".join(f'"{pair.read2}"' for pair in local_fastq_pairs)
        star_align_command = (
            f'"{self.STAR}" '
            f'--runThreadN {thread_count} '
            f'--genomeDir "{local_reference_resource_dir}" '
            f'--genomeLoad NoSharedMemory '
            f'--readFilesIn {r1_files} {r2_files} '
            f'--readFilesCommand zcat '
            f'--outSAMtype BAM Unsorted '
            f'--outSAMunmapped Within '
            f'--outBAMcompression 0 '
            f'--outSAMattributes All '
            f'--outFilterMultimapNmax 10 '
            f'--outFilterMismatchNmax 3 limitOutSJcollapsed 3000000 '
            f'--chimSegmentMin 10 '
            f'--chimOutType WithinBAM SoftClip '
            f'--chimJunctionOverhangMin 10 '
            f'--chimSegmentReadGapMax 3 '
            f'--chimScoreMin 1 '
            f'--chimScoreDropMax 30 '
            f'--chimScoreJunctionNonGTAG 0 '
            f'--chimScoreSeparation 1 '
            f'--outFilterScoreMinOverLread 0.33 '
            f'--outFilterMatchNminOverLread 0.33 '
            f'--outFilterMatchNmin 35 '
            f'--alignSplicedMateMapLminOverLmate 0.33 '
            f'--alignSplicedMateMapLmin 35 '
            f'--alignSJstitchMismatchNmax 5 -1 5 5 '
            f'--outFileNamePrefix {local_working_dir} '
        )
        self._run_bash_command(star_align_command)

        output_bam_path = local_working_dir / "Aligned.out.bam"
        if not output_bam_path.exists():
            raise FileNotFoundError(f"Failed to create RNA bam: {output_bam_path}")
        return output_bam_path

    def sort_bam(
            self,
            local_input_bam_path: Path,
            local_output_bam_path: Path,
    ) -> None:
        thread_count = self._get_thread_count()
        bam_sort_command = (
            f'"{self.SAMBAMBA}" sort -t {thread_count} -o "{local_output_bam_path}" "{local_input_bam_path}"'
        )
        create_parent_dir_if_not_exists(local_output_bam_path)
        self._run_bash_command(bam_sort_command)

    def merge_bams(self, local_input_bams: List[Path], local_output_bam: Path) -> None:
        thread_count = self._get_thread_count()
        local_input_bams_string = " ".join(f'"{input_bam}"' for input_bam in local_input_bams)
        merge_command = f'"{self.SAMBAMBA}" merge -t {thread_count} "{local_output_bam}" {local_input_bams_string}'
        create_parent_dir_if_not_exists(local_output_bam)
        self._run_bash_command(merge_command)

    def create_bam_index(self, local_bam_path: Path) -> None:
        thread_count = self._get_thread_count()
        index_command = f'"{self.SAMBAMBA}" index -t {thread_count} "{local_bam_path}"'
        create_parent_dir_if_not_exists(local_bam_path)
        self._run_bash_command(index_command)

    def deduplicate_without_umi(self, local_input_bam_path: Path, local_output_bam_path: Path) -> None:
        thread_count = self._get_thread_count()
        dedup_command = (
            f'"{self.SAMBAMBA}" markdup -t {thread_count} '
            f'--overflow-list-size={self.SAMBAMBA_MARKDUP_OVERFLOW_LIST_SIZE} '
            f'"{local_input_bam_path}" "{local_output_bam_path}"'
        )
        create_parent_dir_if_not_exists(local_output_bam_path)
        self._run_bash_command(dedup_command)

    def deduplicate_with_umi(self, local_input_bam_path: Path, local_output_bam_path: Path) -> None:
        dedup_command = (
            f'{self.JAVA} -server -Xms8G -Xmx16G -Xss20M -jar "{self.UMI_COLLAPSE_JAR}" '
            f'bam -i "{local_input_bam_path}" -o "{local_output_bam_path}" --umi-sep ":" --paired --two-pass'
        )
        create_parent_dir_if_not_exists(local_output_bam_path)
        self._run_bash_command(dedup_command)

    def flagstat(self, local_input_bam_path: Path, local_output_flagstat_path: Path) -> None:
        thread_count = self._get_thread_count()
        flagstat_command = (
            f'"{self.SAMBAMBA}" flagstat -t "{thread_count}" "{local_input_bam_path}"'
        )
        create_parent_dir_if_not_exists(local_output_flagstat_path)
        self._run_bash_command(flagstat_command, local_output_flagstat_path)

    def count_mapping_coords(self, local_input_bam_path: Path, local_output_path: Path) -> None:
        thread_count = self._get_thread_count()
        view_command = f'"{self.SAMBAMBA}" view -t "{thread_count}" "{local_input_bam_path}"'
        select_command = 'awk \'{print $3 "\t" $4}\''
        uniqueness_command = 'sort -u'
        count_command = f'wc -l'
        combined_command = " | ".join([view_command, select_command, uniqueness_command, count_command])
        create_parent_dir_if_not_exists(local_output_path)
        self._run_bash_command(combined_command, local_output_path)

    def _get_thread_count(self) -> int:
        return multiprocessing.cpu_count()

    def _run_bash_command(self, command: str, output_file_path: Optional[Path] = None) -> BashCommandResults:
        logging.info(f"Running bash command:\n{command}")

        if not command:
            raise SyntaxError(f"No command found to run: {command}")

        try:
            output, errors, status = self._get_bash_command_output(command, output_file_path)
        except Exception as e:
            output = ""
            errors = f"Exception: {e}"
            status = -1

        if status != 0:
            raise ValueError(f"Bash pipeline failed: status={status}, errors={errors}")

        return BashCommandResults(output, errors, status)

    def _get_bash_command_output(self, command: str, output_file_path: Optional[Path] = None) -> Tuple[str, str, int]:
        # Source: https://stackoverflow.com/questions/46117715/python-subprocess-call-and-pipes
        # Other source: https://stackoverflow.com/questions/9655841/python-subprocess-how-to-use-pipes-thrice

        # Create pipeline of subprocesses that mimics bash piping
        command_list = command.split(" | ")
        processes: List[subprocess.Popen[bytes]] = []
        for index, command in enumerate(command_list):
            process_output: Union[TextIO, int]
            if output_file_path is not None and index == len(command_list) - 1:
                # Output of the last command should be redirected to the output file
                process_output = open(output_file_path, "w+")
            else:
                process_output = subprocess.PIPE

            args = shlex.split(command)
            if index == 0:
                process = subprocess.Popen(args, stdout=process_output)
            else:
                process = subprocess.Popen(args, stdin=processes[-1].stdout, stdout=process_output)
            processes.append(process)

        # Close pipelines in between the subprocesses.
        # This allows earlier subprocesses to receive a SIGPIPE if a later subprocess exits.
        # The final pipe is closed by the communicate method
        for process in reversed(processes[:-1]):
            if process.stdout is not None:
                process.stdout.close()

        output_bytes, errors_bytes = processes[-1].communicate()

        # The output encoding is an assumption
        output = None if output_bytes is None else output_bytes.decode(self.OUTPUT_ENCODING)
        errors = None if errors_bytes is None else errors_bytes.decode(self.OUTPUT_ENCODING)
        status = processes[-1].returncode

        return output, errors, status
