from pathlib import Path
import zipfile

import pytest


@pytest.fixture()
def sql_types():
    return {
        "SQLite": {
            "string": "VARCHAR",
            "int": "INTEGER",
            "float": "FLOAT",
            "datetime": "DATETIME",
            "boolean": "BOOLEAN",
            "json": "JSON"
        },
        "PostgreSQL": {
            "string": "character varying",
            "int": "integer",
            "float": "double precision",
            "datetime": "timestamp without time zone",
            "boolean": "boolean",
            "json": "json"
        }
    }


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

