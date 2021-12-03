import logging
import sys
from typing import List

from config import Config
from services.service_provider import ServiceProvider
from util import set_up_logging


def main(arguments: List[str]) -> None:
    set_up_logging()

    logging.info("Starting dry run for k8analysis.")
    logging.info("Extracting jobs from arguments.")
    ServiceProvider(Config()).get_argument_parser().extract_jobs(" ".join(arguments))

    logging.info("Finished dry run for k8analysis.")


if __name__ == '__main__':
    main(sys.argv[1:])
