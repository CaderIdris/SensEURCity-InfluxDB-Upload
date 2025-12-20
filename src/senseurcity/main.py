"""Download, transform and save data from the SensEURCity study."""
import argparse
from collections.abc import Generator
import datetime as dt
import logging
from pathlib import Path
from types import GeneratorType
from typing import Literal, TypedDict
from zipfile import ZipFile

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Engine

from senseurcity.data import (
    MeasurementRecord,
    ColocationRecord,
    HeaderRecord,
    DeviceRecord,
    DeviceHeaderRecord,
    ConversionRecord,
    get_header_records,
    get_device_records,
    get_unit_conversion_records,
    SensEURCityCSV
)
from senseurcity.engine import get_engine
import senseurcity.orm as orm
from senseurcity.zipped import download_data, Cities, get_csvs


class ProgramConfig(TypedDict):
    """Structure of the config dictionary.

    Parameters
    ----------
    zip_url : str
        Where to download the SensEURCity data from.
    zip_path : Path
        Where to save the SensEURCity data to.
    db_url : str
        Database connection string to save the data to.
    verbose : int
        Verbosity level of output
    force : bool
        Overwrite downloaded zip file.
    antwerp : bool
        Save measurements from Antwerp component.
    oslo : bool
        Save measurements from Oslo component.
    zagreb : bool
        Save measurements from Zagreb component.

    """

    zip_url: str
    zip_path: Path
    db_url: str
    verbose: int
    force: bool
    antwerp: bool
    oslo: bool
    zagreb: bool


type DBRecord = (
    MeasurementRecord |
    ColocationRecord |
    HeaderRecord |
    DeviceRecord |
    DeviceHeaderRecord |
    ConversionRecord
)

def parse_prog_args() -> ProgramConfig:
    """Parse command line arguments provided to program.

    Returns
    -------
    ProgramConfig

    """
    arg_parser = argparse.ArgumentParser(
        prog="SensEURCity ETL Pipeline",
        description=(
            "Downloads SensEURCity measurements (E) and transforms them"
            " (T) before uploading to a database (L) that matches the AQua"
            " schema."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    _default_zip_url = (
        "https://zenodo.org/records/7669644/files/"
        "senseurcity_data_v02.zip?download=1"
    )
    _default_zip_path = (
        f"{Path.home() / 'Data/SensEURCity/senseurcity_data.zip'}"
    )
    _default_db_url = (
        "duckdb:///"
        f"{(Path.home() / 'Data/SensEURCity/SensEURCity.db').resolve()}"
    )

    config_group = arg_parser.add_argument_group("configuration")
    override_group = arg_parser.add_argument_group("override")
    city_group = arg_parser.add_argument_group(
        "city",
        "Select which city to use. If none are selected, all will be used."
    )
    other_group = arg_parser.add_argument_group("other")

    config_group.add_argument(
        "-u",
        "--url",
        type=str,
        help="Download link to SensEURCity zip file.",
        default=_default_zip_url,
        metavar="https://...",
        dest="zip_url",
    )

    config_group.add_argument(
        "-p",
        "--path",
        type=str,
        help="Destination of SensEURCity zip file.",
        default=_default_zip_path,
        dest="zip_path",
    )

    config_group.add_argument(
        "-d",
        "--db",
        type=str,
        help="URL to the metadata database",
        default=_default_db_url,
        dest="db_url",
    )

    city_group.add_argument(
        "-a",
        "--antwerp",
        action="store_true",
        help="Download Antwerp data",
        dest="antwerp"
    )

    city_group.add_argument(
        "-o",
        "--oslo",
        action="store_true",
        help="Download Oslo data",
        dest="oslo"
    )

    city_group.add_argument(
        "-z",
        "--zagreb",
        action="store_true",
        help="Download Zagreb data",
        dest="zagreb"
    )

    override_group.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Overwrite SensEURCity zip file if it already exists",
        dest="force",
    )

    other_group.add_argument("--verbose", "-v", action="count", default=0)

    config = arg_parser.parse_args()

    no_city_selected = not any((config.antwerp, config.oslo, config.zagreb))

    return {
        "zip_url": config.zip_url,
        "zip_path": Path(config.zip_path),
        "db_url": config.db_url,
        "verbose": config.verbose,
        "force": config.force,
        "antwerp": config.antwerp or no_city_selected,
        "oslo": config.oslo or no_city_selected,
        "zagreb": config.zagreb or no_city_selected
    }


def set_up_logger(verbosity: int) -> logging.Logger:
    """Set up the logger.

    Parameters
    ----------
    verbosity : int
        Verbosity level.

    Returns
    -------
    logging.Logger

    """
    level = logging.DEBUG if verbosity else logging.INFO
    logger = logging.getLogger("SensEURCity-ETL")
    logger.setLevel(level)

    format_str = (
        "%(asctime)s - %(funcName)s - %(levelname)s - %(message)s"
        if verbosity else "%(asctime)s - %(message)s"
    )

    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_str)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_cities(config: ProgramConfig) -> Cities:
    """Which cities to save measurements for.

    Parameters
    ----------
    config : ProgramConfig
        Configuration of the program, dictated by command line arguments.

    Returns
    -------
    Cities

    """
    cities = Cities(0)
    if config["antwerp"]:
        cities = cities | cities.Antwerp
    if config["oslo"]:
        cities = cities | cities.Oslo
    if config["zagreb"]:
        cities = cities | cities.Zagreb
    return cities


def upload_data_sqa(
    records: Generator[DBRecord],
    table: type[orm._BaseV1],
    engine: Engine,
    batch_size: int = 25
) -> None:
    """Upload measurements to DB via SQLAlchemy.

    Parameters
    ----------
    records : Generator[DBRecord]
        A generator containing all records to be saved to database.
    table : type[orm._BaseV1]
        The table in the DB to save the measurements to.
    engine : Engine
        SQLAlchemy engine, connected to the DB.
    batch_size : int, default=25000
        The number of measurements to save at at time.

    Raises
    ------
    TypeError
        Generator is not passes into records argument. If another type of
        iterator is given, an infinite loop would happen.

    """
    if not isinstance(records, GeneratorType):
        msg = (
            "Expected a generator object for values, received a "
            f"{type(records)} object instead"
        )
        raise TypeError(msg)
    logger = logging.getLogger("SensEURCity-ETL")
    insert_statement = insert(table).on_conflict_do_nothing()
    _iteration_count = 0
    with engine.connect() as conn:
        while True:
            batch = [x for _, x in zip(range(batch_size), records, strict=False)]
            if not batch:
                break
            _iteration_count += 1
            logger.debug(
                "[%s] Batch number: %s",
                table.__tablename__,
                _iteration_count
            )
            conn.execute(
                insert_statement.values(batch)
            )
            conn.commit()


def get_processed_files(
    engine: Engine
) -> list[str]:
    """Get a list of the files which have already been processed.

    Once a file has been fully processed, it's name is uploaded to the
    meta_files_processed table. This function collects those filenames and
    returns them in a list.

    Parameters
    ----------
    engine : Engine
        SQLAlchemy engine, connected to the DB.

    Returns
    -------
    list[str]

    """
    select_statement = select(orm.MetaFilesProcessed.filename)
    with Session(bind=engine) as session:
        result = session.execute(select_statement)
        return [row[0] for row in result.all()]



def upload_csv_data(
    zip_file: ZipFile,
    cities: Cities,
    processed_files: list[str],
    engine: Engine
) -> None:
    """Iterate over csvs in zip file and upload records to SQLAlchemy.

    Parameters
    ----------
    zip_file : ZipFile
        The zip file containing the SensEURCity measurements.
    cities : Cities
        The cities to save measurements for.
    processed_files : list[str]
        The files already processed.
    engine : Engine
        The SQLAlchemy engine, connected to the DB.

    """
    logger = logging.getLogger("SensEURCity-ETL")
    file_insert_statement = insert(orm.MetaFilesProcessed)
    for city in cities:
        logger.info("(T) Beginning transformations: %s", city.name)
        for filename, csv in get_csvs(zip_file, city):
            if filename in processed_files:
                logger.info("Skipping %s", filename)
                continue
            logger.info("(T) Transforming csv file for %s", filename)
            csv_class = SensEURCityCSV.from_dataframe(filename, csv)
            logger.info("(L) Uploading data for %s", filename)
            # Device headers
            logger.info("(L) %s - Device Headers", filename)
            upload_data_sqa(
                csv_class.device_headers,
                orm.BridgeDeviceHeaders,
                engine
            )
            # Reference headers
            logger.info("(L) %s - Device Headers", filename)
            upload_data_sqa(
                csv_class.reference_headers,
                orm.BridgeDeviceHeaders,
                engine
            )
            # Device measurements
            logger.info("(L) %s - Measurements", filename)
            upload_data_sqa(
                csv_class.measurements,
                orm.FactMeasurement,
                engine
            )
            # Reference measurements
            logger.info("(L) %s - Reference Measurements", filename)
            upload_data_sqa(
                csv_class.reference_measurements,
                orm.FactMeasurement,
                engine
            )

            # Co-location
            logger.info("(L) %s - Colocation", filename)
            upload_data_sqa(
                csv_class.colocation,
                orm.DimColocation,
                engine
            )

            logger.info("(L) %s - File written", filename)
            with engine.connect() as conn:
                conn.execute(
                    file_insert_statement.values({
                        "filename": filename,
                        "timestamp": dt.datetime.now()
                    })
                )
                conn.commit()


def cli() -> Literal[0]:
    """Download (E), Transform (T) and Load (L) SensEURCity measurements to DB.

    Returns
    -------
    0 if successful

    """
    config = parse_prog_args()
    logger = set_up_logger(config["verbose"])

    logger.info("Running SensEURCity ETL Pipeline")

    if config["verbose"]:
        logger.debug("Following configuration options used:")
        for k, v in config.items():
            logger.debug("%s: %s", k, v)

    # Create engine
    logger.info("Connecting to db and creating tables")
    engine = get_engine(config["db_url"])
    orm.create_tables(engine)

    # Download data
    logger.info("(E) Downloading SensEURCity dataset")
    zip_path = download_data(
        config["zip_url"],
        config["zip_path"],
        ignore_file_exists=config["force"]
    )
    if zip_path is None:
        msg = "SensEURCity data could not be downloaded, exiting."
        raise FileNotFoundError(msg)

    # Get cities to parse data for
    cities = get_cities(config)
    if config["verbose"]:
        logger.debug("Selecting csv files for:")
        for city in cities:
            logger.debug("- %s", city.name)

    # Populate tables with static data
    logger.info("(E) Querying list of processed files")
    file_list = get_processed_files(engine)

    logger.info("(L) Uploading device information")
    upload_data_sqa(get_device_records(), orm.DimDevice, engine)

    logger.info("(L) Uploading device information")
    upload_data_sqa(get_header_records(), orm.DimHeader, engine)

    logger.info("(L) Uploading unit conversion information")
    upload_data_sqa(get_unit_conversion_records(), orm.DimUnitConversion, engine)

    senseurcity_zip = ZipFile(zip_path)

    logger.info("(T) Transforming csv data")
    upload_csv_data(senseurcity_zip, cities, file_list, engine)

    return 0

