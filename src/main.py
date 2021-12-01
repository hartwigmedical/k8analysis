import logging
import sys
from pathlib import Path
from typing import List

from arg_parser import Parser
from service_provider import ServiceProvider
from util import set_up_logging

LOCAL_GCP_FILE_CACHE = Path.home() / "gcp_local_file_cache"


def main(arguments: List[str]) -> None:
    set_up_logging()

    logging.info("Starting k8analysis.")

    logging.info("Extracting jobs from arguments.")
    jobs = Parser.extract_jobs(arguments)

    if jobs:
        service_provider = ServiceProvider(LOCAL_GCP_FILE_CACHE)
        for job in jobs:
            job.execute(service_provider)
    else:
        logging.warning("No jobs detected.")

    logging.info("Finished k8analysis.")


if __name__ == '__main__':
    main(sys.argv[1:])
