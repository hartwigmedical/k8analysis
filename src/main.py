import logging
import sys
from typing import List

from gcp_client import GCPClient
from parser import Parser


def main(arguments: List[str]) -> None:
    set_up_logging()

    logging.info("Starting k8analysis.")

    logging.info("Extracting jobs from arguments.")
    jobs = Parser.extract_jobs(arguments)

    if jobs:
        gcp_client = GCPClient()
        for job in jobs:
            job.execute(gcp_client)
    else:
        logging.warning("No jobs detected.")

    logging.info("Finished k8analysis.")


def set_up_logging() -> None:
    logging.basicConfig(
        format="%(asctime)s - [%(levelname)-8s] - %(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )


if __name__ == '__main__':
    main(sys.argv[1:])
