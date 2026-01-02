import datetime as dt
import logging
from pathlib import Path
import sys
from zipfile import ZipFile

import pytest
from sqlalchemy.engine.base import Engine

import senseurcity
from senseurcity.main import (
    parse_prog_args,
    set_up_logger,
    get_cities,
    upload_data_sqa,
    get_processed_files,
    upload_csv_data,
    cli
)
from aqorm import orm
from senseurcity.zipped import Cities

from conftest import DBs


@pytest.fixture
def zip_file(tmp_path: Path) -> Path:
    """"""
    path = tmp_path / "tmp.zip"
    csv_files = [
        Path("tests/test_zipped/Antwerp_402B00.csv"),
        Path("tests/test_zipped/Oslo_64A291.csv"),
        Path("tests/test_zipped/Zagreb_64C52B.csv")
    ]
    with ZipFile(path, "a") as zipf:
        zipf.mkdir("senseurcity_data_v02")
        zipf.mkdir("senseurcity_data_v02/dataset")
        for csv in csv_files:
            with csv.open("r") as file:
                zipf.writestr(
                    f"senseurcity_data_v02/dataset/{csv.name}",
                    file.read()
                )
    return path


def test_parse_args_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    """
    tests = {}
    monkeypatch.setattr(sys, "argv", ["main.py"])
    config = parse_prog_args()

    tests["Zip URL is string"] = isinstance(config["zip_url"], str)
    tests["Zip path is path"] = isinstance(config["zip_path"], Path)
    tests["DB Url is DuckDB"] = config["db_url"][:6] == "duckdb"
    tests["Verbose is False"] = not bool(config["verbose"])
    tests["Force is False"] = not config["force"]
    tests["Antwerp is True"] = config["antwerp"]
    tests["Oslo is True"] = config["oslo"]
    tests["Zagreb is True"] = config["zagreb"]

    for result in tests.values():
        if not result:
            pass
    assert all(tests.values())

@pytest.mark.parametrize(
    "city_flag",
    [
        ("a", "antwerp"),
        ("o", "oslo"),
        ("z", "zagreb"),
    ]
)
def test_specific_city(
    city_flag: tuple[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    """
    tests = {}
    monkeypatch.setattr(sys, "argv", ["main.py", f"-{city_flag[0]}"])
    config = parse_prog_args()
    cities = {"antwerp", "oslo", "zagreb"} - {city_flag[1]}
    tests[f"{city_flag[1]} is true"] = config[city_flag[1]]  # type: ignore[literal-required]

    for city in cities:
        tests[f"{city} not true"] = not config[city]  # type: ignore[literal-required]

    for result in tests.values():
        if not result:
            pass
    assert all(tests.values())

@pytest.mark.parametrize(
    "verbosity",
    [
        0,
        1,
        2,
        3
    ]
)
def test_verbosity(
    verbosity: int,
    monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    """
    if verbosity:
        monkeypatch.setattr(sys, "argv", ["main.py", f"-{'v' * verbosity}"])
    else:
        monkeypatch.setattr(sys, "argv", ["main.py"])
    config = parse_prog_args()
    assert config["verbose"] == verbosity

def test_logger_normal_mode(caplog: pytest.LogCaptureFixture) -> None:
    """
    """
    logger = set_up_logger(0)
    tests = {}
    with caplog.at_level(logging.DEBUG):
        logger.debug("NOTHING")
        tests["No debug"] = "NOTHING" not in caplog.text
    with caplog.at_level(logging.INFO):
        logger.info("SOMETHING")
        tests["Info"] = "SOMETHING" in caplog.text

    for result in tests.values():
        if not result:
            pass
    assert all(tests.values())


def test_logger_debug_mode(caplog: pytest.LogCaptureFixture) -> None:
    """
    """
    logger = set_up_logger(1)
    tests = {}
    with caplog.at_level(logging.DEBUG):
        logger.debug("SOMETHING")
        tests["Debug"] = "SOMETHING" in caplog.text
    for result in tests.values():
        if not result:
            pass
    assert all(tests.values())

@pytest.mark.parametrize("antwerp", [False, True])
@pytest.mark.parametrize("oslo", [False, True])
@pytest.mark.parametrize("zagreb", [False, True])
def test_get_cities(antwerp, oslo, zagreb) -> None:
    """
    """
    city_options = {
        "antwerp": antwerp,
        "oslo": oslo,
        "zagreb": zagreb
    }

    expected_cities = Cities(0)
    if antwerp:
        expected_cities = expected_cities | Cities.Antwerp
    if oslo:
        expected_cities = expected_cities | Cities.Oslo
    if zagreb:
        expected_cities = expected_cities | Cities.Zagreb

    cities = get_cities(city_options)  # type: ignore[reportArgumentType,arg-type]
    assert cities == expected_cities

@pytest.mark.parametrize("db", list(DBs))
def test_upload_to_sqa(
    connections: dict[DBs, Engine | None],
    db: DBs
) -> None:
    """"""
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()
    upload_data_sqa(
        (  # type: ignore[reportArgumentType]
            record for record in  # type: ignore[misc]
            [
                {"filename": "hello", "timestamp": dt.datetime(2020, 1, 1)},
                {"filename": "hello2", "timestamp": dt.datetime(2020, 1, 1)},
            ]
        ),
        orm.MetaFilesProcessed,
        db_engine
    )

@pytest.mark.parametrize("db", list(DBs))
def test_upload_to_sqa_not_generator(
    connections: dict[DBs, Engine | None],
    db: DBs
) -> None:
    """"""
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()
    with pytest.raises(
        TypeError,
        match="Expected a generator object for values, received a "
    ):
        upload_data_sqa(
            [  # type: ignore[arg-type]
                    {"filename": "hello", "timestamp": dt.datetime(2020, 1, 1)},
                    {"filename": "hello2", "timestamp": dt.datetime(2020, 1, 1)}
            ],
            orm.MetaFilesProcessed,
            db_engine
        )

@pytest.mark.parametrize("db", list(DBs))
def test_get_processed_files(
    connections: dict[DBs, Engine | None],
    db: DBs
) -> None:
    """"""
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    tests = {}

    upload_data_sqa(
        (  # type: ignore[reportArgumentType]
            record for record in  # type: ignore[misc]
            [
                {"filename": "get", "timestamp": dt.datetime(2020, 1, 1)},
                {"filename": "some", "timestamp": dt.datetime(2020, 1, 1)},
                {"filename": "files", "timestamp": dt.datetime(2020, 1, 1)},
            ]
        ),
        orm.MetaFilesProcessed,
        db_engine
    )
    files = get_processed_files(db_engine)
    for file in ["get", "some", "files"]:
        tests[f"{file} in files"] = file in files

    for result in tests.values():
        if not result:
            pass
    assert all(tests.values())


@pytest.mark.parametrize("db", list(DBs))
def test_file_skipped(
    connections: dict[DBs, Engine | None],
    db: DBs,
    zip_file: Path,
    caplog: pytest.LogCaptureFixture
) -> None:
    """"""
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    tests = {}

    files = [
        "Antwerp_402B00",
        "Oslo_64A291",
        "Zagreb_64C52B"
    ]

    with caplog.at_level(logging.DEBUG), ZipFile(zip_file, "r") as zipf:
        upload_csv_data(
            zipf,
            Cities.Antwerp | Cities.Oslo | Cities.Zagreb,
            files,
            db_engine
        )
    for file in files:
        tests[f"{file} skipped"] = file in caplog.text

    for result in tests.values():
        if not result:
            pass
    assert all(tests.values())


@pytest.mark.parametrize("db", list(DBs))
def test_cli_good(
    connections: dict[DBs, Engine | None],
    db: DBs,
    zip_file: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """"""
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    def mock_download_data(*args, **kwargs):
        return zip_file

    def mock_get_engine(*args, **kwargs):
        return db_engine

    def mock_get_processed_files_empty(*args, **kwargs):
        return []

    monkeypatch.setattr(sys, "argv", ["main.py", "-v"])
    monkeypatch.setattr(senseurcity.main, "get_engine", mock_get_engine)
    monkeypatch.setattr(senseurcity.main, "download_data", mock_download_data)
    monkeypatch.setattr(
        senseurcity.main,
        "get_processed_files",
        mock_get_processed_files_empty
    )

    result = cli()
    assert result == 0


@pytest.mark.parametrize("db", list(DBs))
def test_cli_no_file(
    connections: dict[DBs, Engine | None],
    db: DBs,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """"""
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    def mock_download_data(*args, **kwargs) -> None:
        return None

    def mock_get_engine(*args, **kwargs):
        return db_engine

    def mock_get_processed_files_empty(*args, **kwargs):
        return []

    monkeypatch.setattr(sys, "argv", ["main.py", "-v"])
    monkeypatch.setattr(senseurcity.main, "get_engine", mock_get_engine)
    monkeypatch.setattr(senseurcity.main, "download_data", mock_download_data)
    monkeypatch.setattr(
        senseurcity.main,
        "get_processed_files",
        mock_get_processed_files_empty
    )

    with pytest.raises(
        FileNotFoundError,
        match="SensEURCity data could not be downloaded"
    ):
        _ = cli()
