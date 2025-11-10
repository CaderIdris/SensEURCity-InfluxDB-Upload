import argparse
from collections.abc import Generator
import datetime as dt
import logging
from pathlib import Path
from typing import TypedDict
from zipfile import ZipFile

from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Engine

from senseurcity.data import (
    MeasurementRecord,
    ValueRecord,
    FlagRecord,
    ColocationRecord,
    HeaderRecord,
    DeviceRecord,
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
    """"""
    zip_url: str
    zip_path: Path
    db_url: str
    schema_name: str
    verbose: int
    force: bool
    antwerp: bool
    oslo: bool
    zagreb: bool


type DBRecord = (
    MeasurementRecord |
    ValueRecord |
    FlagRecord |
    ColocationRecord |
    HeaderRecord |
    DeviceRecord |
    ConversionRecord
)

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
        "duckdb:///"
        f"{(Path.home() / 'Data/SensEURCity/SensEURCity.db').resolve()}"
    )
    _DEFAULT_SCHEMA_NAME = "measurement"
    
    config_group = arg_parser.add_argument_group('configuration')
    override_group = arg_parser.add_argument_group('override')
    city_group = arg_parser.add_argument_group(
        'city',
        "Select which city to use. If none are selected, all will be used."
    )
    other_group = arg_parser.add_argument_group('other')

    config_group.add_argument(
        "-u",
        "--url",
        type=str,
        help="Download link to SensEURCity zip file.",
        default=_DEFAULT_ZIP_URL,
        metavar="https://...",
        dest="zip_url",
    )

    config_group.add_argument(
        "-p",
        "--path",
        type=str,
        help="Destination of SensEURCity zip file.",
        default=_DEFAULT_ZIP_PATH,
        dest="zip_path",
    )

    config_group.add_argument(
        "-d",
        "--db",
        type=str,
        help="URL to the database",
        default=_DEFAULT_DB_URL,
        dest="db_url",
    )

    config_group.add_argument(
        "-s",
        "--schema",
        type=str,
        help="Name of the database schema to save measurements to.",
        default=_DEFAULT_SCHEMA_NAME,
        dest="schema",
    )

    city_group.add_argument(
        '-a',
        '--antwerp',
        action="store_true",
        help="Download Antwerp data",
        dest="antwerp"
    )

    city_group.add_argument(
        '-o',
        '--oslo',
        action="store_true",
        help="Download Oslo data",
        dest="oslo"
    )

    city_group.add_argument(
        '-z',
        '--zagreb',
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

    other_group.add_argument('--verbose', '-v', action='count', default=0)

    config = arg_parser.parse_args()

    no_city_selected = not any((config.antwerp, config.oslo, config.zagreb))

    return {
        "zip_url": config.zip_url,
        "zip_path": Path(config.zip_path),
        "db_url": config.db_url,
        "schema_name": config.schema,
        "verbose": config.verbose,
        "force": config.force,
        "antwerp": config.antwerp or no_city_selected,
        "oslo": config.oslo or no_city_selected,
        "zagreb": config.zagreb or no_city_selected
    }


def set_up_logger(verbosity: int) -> logging.Logger:
    """"""
    level = logging.DEBUG if verbosity else logging.INFO 
    logger = logging.getLogger('SensEURCity-AQua-ETL')
    logger.setLevel(level)

    format_str = (
        '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s' 
        if verbosity else '%(asctime)s - %(message)s'
    )

    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(format_str)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_cities(config: ProgramConfig) -> Cities:
    """"""
    cities = Cities(0)
    if config["antwerp"]:
        cities = cities | cities.Antwerp
    if config["oslo"]:
        cities = cities | cities.Oslo
    if config["zagreb"]:
        cities = cities | cities.Zagreb
    return cities


def upload_data(
    records: Generator[DBRecord],
    table: type[orm._Base_V1],
    engine: Engine,
    batch_size: int = 250000
) -> None:
    """"""
    logger = logging.getLogger('SensEURCity-AQua-ETL')
    insert_statement = insert(table).on_conflict_do_nothing()
    _iteration_count = 0
    with engine.connect() as conn:
        while True:
            batch = [x for _, x in zip(range(batch_size), records)]
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
    """"""
    select_statement = select(orm.MetaFilesProcessed.filename)
    with Session(bind=engine) as session:
        result = session.execute(select_statement)
        files = [row[0] for row in result.all()]
    return files
        



def upload_csv_data(
    zip: ZipFile,
    cities: Cities,
    engine: Engine,
    processed_files = list[str]
) -> None:
    """"""
    logger = logging.getLogger('SensEURCity-AQua-ETL')
    for city in cities:
        logger.info("(T) Beginning transformations: %s", city.name)
        for filename, csv in get_csvs(zip, city):
            if filename in processed_files:
                logger.info("Skipping %s", filename)
                continue
            logger.info("(T) Transforming csv file for %s", filename)
            csv_class = SensEURCityCSV.from_dataframe(filename, csv)
            logger.info("(L) Uploading data for %s", filename)
            # Device measurements
            logger.info("(L) %s - Measurements", filename)
            upload_data(csv_class.measurements, orm.FactMeasurement, engine)
            # Device measurement values
            logger.info("(L) %s - Values", filename)
            upload_data(csv_class.values, orm.FactValue, engine)
            # Device measurement flags
            logger.info("(L) %s - Flags", filename)
            upload_data(csv_class.flags, orm.FactFlag, engine)
            # Reference measurements
            logger.info("(L) %s - Reference Measurements", filename)
            upload_data(
                csv_class.reference_measurements,
                orm.FactMeasurement,
                engine
            )
            # Reference measurement values
            logger.info("(L) %s - Reference Values", filename)
            upload_data(csv_class.reference_values, orm.FactValue, engine)
            # Co-location
            logger.info("(L) %s - Co-location", filename)
            upload_data(csv_class.colocation, orm.DimColocation, engine)
            # File processed
            logger.info("(L) Adding %s to processed files list", filename)
            upload_data(
                ({"filename": filename, "timestamp": dt.datetime.now()} for _ in [0]),
                orm.MetaFilesProcessed,
                engine
            )






def cli() -> None:
    """"""
    config = parse_args()
    logger = set_up_logger(config["verbose"])

    logger.info("Running SensEURCity ETL Pipeline")

    if config["verbose"]:
        logger.debug("Following configuration options used:")
        for k, v in config.items():
            logger.debug("%s: %s", k, v)

    # Create engine
    logger.info("Connecting to db and creating tables")
    engine = get_engine(config["db_url"], config["schema_name"])
    orm.create_tables(engine)

    # Download data
    logger.info("(E) Downloading SensEURCity dataset")
    zip_path = download_data(
        config["zip_url"],
        config["zip_path"],
        ignore_file_exists=config["force"]
    )
    if zip_path is None:
        logger.error("SensEURCity data could not be downloaded, exiting.")
        return None

    # Get cities to parse data for
    cities = get_cities(config)
    if config["verbose"]:
        logger.debug("Selecting csv files for:")
        for k in cities:
            logger.debug("%s", k.name)

    # Populate tables with static data
    logger.info("(E) Querying list of processed files")
    file_list = get_processed_files(engine)
    
    #TODO: Devices
    logger.info("(L) Uploading device information")
    upload_data(get_device_records(), orm.DimDevice, engine)

    #TODO: Headers
    logger.info("(L) Uploading device information")
    upload_data(get_header_records(), orm.DimHeader, engine)

    #TODO: Conversion
    logger.info("(L) Uploading unit conversion information")
    upload_data(get_unit_conversion_records(), orm.DimUnitConversion, engine)


    senseurcity_zip = ZipFile(zip_path)

    logger.info("(T) Transforming csv data")
    upload_csv_data(senseurcity_zip, cities, engine, file_list)

    #TODO: EXIT
