import argparse
import logging
from pathlib import Path
from typing import TypedDict

from senseurcity.zipped import download_data


class ProgramConfig(TypedDict):
    """"""
    zip_url: str
    zip_path: Path
    db_url: str
    schema_name: str
    verbose: int
    force: bool


def parse_args() -> ProgramConfig:
    """"""
    arg_parser = argparse.ArgumentParser(
        prog="SensEURCity ETL Pipeline",
        description=(
            "Downloads SensEURCity measurements (E) and transforms them"
            " (T) before uploading to a database (L) that matches the AQua"
            " schema."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    _DEFAULT_ZIP_URL = (
        "https://zenodo.org/records/7669644/files/"
        "senseurcity_data_v02.zip?download=1"
    )
    _DEFAULT_ZIP_PATH = (
        f"{Path.home() / 'Data/SensEURCity/senseurcity_data.zip'}"
    )
    _DEFAULT_DB_URL = (
        "sqlite+pysqlite://"
        f"{Path.home() / 'SensEURCity' / 'SensEURCity.db'}"
    )
    _DEFAULT_SCHEMA_NAME = "measurement"

    arg_parser.add_argument(
        "-u",
        "--url",
        type=str,
        help="Download link to SensEURCity zip file.",
        default=_DEFAULT_ZIP_URL,
        metavar="https://...",
        dest="zip_url",
    )

    arg_parser.add_argument(
        "-z",
        "--zip",
        type=str,
        help="Destination of SensEURCity zip file.",
        default=_DEFAULT_ZIP_PATH,
        dest="zip_path",
    )

    arg_parser.add_argument(
        "-d",
        "--db",
        type=str,
        help="URL to the database",
        default=_DEFAULT_DB_URL,
        dest="db_url",
    )

    arg_parser.add_argument(
        "-s",
        "--schema",
        type=str,
        help="Name of the database schema to save measurements to.",
        default=_DEFAULT_SCHEMA_NAME,
        dest="schema",
    )

    arg_parser.add_argument(
        "-f",
        "--force",
        type=str,
        help="Overwrite SensEURCity zip file if it already exists",
        default=False,
        dest="force",
    )

    arg_parser.add_argument('--verbose', '-v', action='count', default=0)

    config = arg_parser.parse_args()

    return {
        "zip_url": config.zip_url,
        "zip_path": Path(config.zip_path),
        "db_url": config.db_url,
        "schema_name": config.schema,
        "verbose": config.verbose,
        "force": config.force
    }


def set_up_logger(verbosity: int) -> logging.Logger:
    """"""
    level = logging.DEBUG if verbosity else logging.INFO 
    logger = logging.getLogger()
    logger.setLevel(level)

    format_str = (
        '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s' 
        if verbosity else '%(message)s'
    )

    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_str)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def cli() -> None:
    """"""
    config = parse_args()
    logger = set_up_logger(config["verbose"])

    logger.info("Running SensEURCity ETL Pipeline")

    if config["verbose"]:
        logger.debug("Following configuration options used:")
        for k, v in config.items():
            logger.debug("%s: %s", k, v)

    zip_path = download_data(
        config["zip_url"],
        config["zip_path"],
        ignore_file_exists=config["force"]
    )
    if zip_path is None:
        logger.error("SensEURCity data could not be downloaded, exiting.")
