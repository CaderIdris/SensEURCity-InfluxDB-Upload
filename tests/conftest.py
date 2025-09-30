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


