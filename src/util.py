import logging
import shutil
from pathlib import Path


def set_up_logging() -> None:
    logging.basicConfig(
        format="%(asctime)s - [%(levelname)-8s] - %(message)s", level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S"
    )


def create_or_cleanup_dir(directory: Path) -> None:
    if directory.is_dir():
        shutil.rmtree(directory)
    elif directory.exists():
        directory.unlink()
    directory.mkdir(parents=True)


def create_parent_dir_if_not_exists(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)