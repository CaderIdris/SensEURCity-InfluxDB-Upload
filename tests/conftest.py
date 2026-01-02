from enum import auto, Flag
from pathlib import Path
import zipfile

import pytest
from sqlalchemy.engine.base import Engine

from aqorm import engine, orm

try:
    from docker.errors import DockerException
    from testcontainers.postgres import PostgresContainer
    postgres = PostgresContainer("postgres:18-alpine", driver="psycopg")
except (ImportError, DockerException):
    postgres = None

if postgres is not None:
    @pytest.fixture(scope="session", autouse=True)
    def setup(request) -> None:
        """Setup the postgres docker container."""
        postgres.start()

        def remove_container() -> None:
            postgres.stop()

        request.addfinalizer(remove_container)  # noqa: PT021


@pytest.fixture(scope="session")
def data_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("zip_download_test")


@pytest.fixture(scope="session")
def sec_mock_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    zip_dir = tmp_path_factory.mktemp("mock_senseurcity_data")
    zip_path = zip_dir / "SensEURCity.zip"
    example_data = Path("./tests/test_zipped/")
    with zipfile.ZipFile(zip_path, "a") as zip_file:
        zip_file.mkdir("senseurcity_data_v02/")
        zip_file.mkdir("senseurcity_data_v02/dataset/")
        for csv_file in example_data.glob("*.csv"):
            with csv_file.open("r") as test_data:
                zip_file.writestr(
                    f"senseurcity_data_v02/dataset/{csv_file.name}",
                    test_data.read()
                )
    return zip_path


class DBs(Flag):
    """DBs to test with the ORM."""

    SQLite = auto()
    DuckDB = auto()
    PostgreSQL = auto()


@pytest.fixture(scope="session")
def db_path(tmp_path_factory):
    """Path to the test database."""
    return tmp_path_factory.mktemp("db")


@pytest.fixture(scope="session")
def connections(db_path: Path) -> dict[DBs, Engine]:
    db_path_sqlite = db_path / "sqlite.db"
    db_path_duckdb = db_path / "duckdb.db"
    engines = {
        DBs.SQLite: engine.get_engine(f"sqlite+pysqlite:///{db_path_sqlite}"),
        DBs.DuckDB: engine.get_engine(f"duckdb:///{db_path_duckdb}"),
    }
    if postgres is not None:
        engines[DBs.PostgreSQL] = engine.get_engine(
            postgres.get_connection_url()
        )
    for db_engine in engines.values():
        orm.create_tables(db_engine)
    return engines


