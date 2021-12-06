import logging
import multiprocessing
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

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
    UMI_COLLAPSE_JAR = Path.home() / "UMICollapse" / "umicollapse.jar"

    SAMBAMBA_MARKDUP_OVERFLOW_LIST_SIZE = 4500000

    OUTPUT_ENCODING = "utf-8"

    def align_dna_bam(
            self,
            local_read1_fastq_path: Path,
            local_read2_fastq_path: Path,
            local_reference_genome_path: Path,
            local_output_bam_path: Path,
            read_group_string: str,
    ) -> None:
        thread_count = self._get_thread_count()
        bwa_align_command = (
            f'"{self.BWA}" mem -Y -t {thread_count} -R "{read_group_string}" "{local_reference_genome_path}" '
            f'"{local_read1_fastq_path}" "{local_read2_fastq_path}"'
        )
        sam_to_bam_command = f'"{self.SAMBAMBA}" view -f "bam" -S -l 0 "/dev/stdin"'
        bam_sort_command = f'"{self.SAMBAMBA}" sort -o "{local_output_bam_path}" "/dev/stdin"'
        combined_command = " | ".join([bwa_align_command, sam_to_bam_command, bam_sort_command])

        create_parent_dir_if_not_exists(local_output_bam_path)
        self._run_bash_command(combined_command)

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
            f'"{self.SAMBAMBA}" flagstat -t "{thread_count}" "{local_input_bam_path}" > "{local_output_flagstat_path}"'
        )
        create_parent_dir_if_not_exists(local_output_flagstat_path)
        self._run_bash_command(flagstat_command)

    def count_mapping_coords(self, local_input_bam_path: Path, local_output_path: Path) -> None:
        thread_count = self._get_thread_count()
        view_command = f'"{self.SAMBAMBA}" view -t "{thread_count}" "{local_input_bam_path}"'
        select_command = 'awk \'{print $3 "\t" $4}\''
        uniqueness_command = 'sort -u'
        count_command = f'wc -l'
        combined_command = " | ".join([view_command, select_command, uniqueness_command, count_command])
        combined_command_with_output = f'{combined_command} > "{local_output_path}"'
        create_parent_dir_if_not_exists(local_output_path)
        self._run_bash_command(combined_command_with_output)

    def _get_thread_count(self) -> int:
        return multiprocessing.cpu_count()

    def _run_bash_command(self, command: str) -> BashCommandResults:
        # Source: https://stackoverflow.com/questions/46117715/python-subprocess-call-and-pipes
        # Other source: https://stackoverflow.com/questions/9655841/python-subprocess-how-to-use-pipes-thrice
        logging.info(f"Running bash command:\n{command}")
        command_list = command.split(" | ")

        if not command_list:
            raise SyntaxError(f"No command found to run: {command}")

        processes: List[subprocess.Popen[bytes]] = []
        try:
            for command in command_list:
                args = shlex.split(command)
                if not processes:
                    process = subprocess.Popen(args, stdout=subprocess.PIPE)
                else:
                    process = subprocess.Popen(
                        args,
                        stdin=processes[-1].stdout,
                        stdout=subprocess.PIPE,
                    )
                processes.append(process)

            for process in reversed(processes[:-1]):
                if process.stdout is not None:
                    process.stdout.close()

            output_bytes, errors_bytes = processes[-1].communicate()
            output = None if output_bytes is None else output_bytes.decode(self.OUTPUT_ENCODING)
            errors = None if errors_bytes is None else errors_bytes.decode(self.OUTPUT_ENCODING)

            status = processes[-1].returncode
        except Exception as e:
            output = ""
            errors = f"Exception: {e}"
            status = -1

        if status != 0:
            raise ValueError(f"Bash pipeline failed: status={status}, errors={errors}")

        return BashCommandResults(output, errors, status)
