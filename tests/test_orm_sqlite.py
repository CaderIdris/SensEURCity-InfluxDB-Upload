"""Tests the orm.py module with a SQLite3 DB.

Tests
-----
- Are tables created?
    - Tests whether all tables are created in db
- Are table schemas valid?
    - `dim_device`
    - `dim_header`
    - `dim_flag`
    - `fact_measurement`
- `dim_device`
    - Does it accept valid measurements?
    - Is duplicate data rejected?
    - Are null values rejected?
- `dim_header`
    - Does it accept valid measurements?
    - Is duplicate data rejected?
    - Are null values rejected?
- `dim_flag`
    - Does it accept valid measurements?
    - Are null values rejected?
    - Are bad foreign keys rejected?
- `fact_measurement`
    - Does it accept valid measurements?
    - Is duplicate data rejected?
    - Are null values rejected?
    - Are bad foreign keys rejected?
"""
import datetime as dt
import sqlite3 as sql3

import pytest
from sqlalchemy import insert
import sqlalchemy.exc as sqlexc

from senseurcity import orm
from senseurcity import engine


@pytest.fixture(scope="session")
def db_path(tmp_path_factory):
    return tmp_path_factory.mktemp("db") / "test_orm.db"


@pytest.fixture(autouse=True)
def sqlite_connection(db_path):
    db_engine = engine.get_engine(f"sqlite+pysqlite:///{db_path}")
    orm._Base_V1.metadata.create_all(db_engine)
    return db_engine


def get_table_cols(cursor, expected_cols, table_name):
    tests = {}
    cursor.execute(f"PRAGMA table_xinfo({table_name});")
    result = cursor.fetchall()
    formatted_result = {
        col[1]: col[2:] for col in result
    }
    for col, config in expected_cols.items():
        tests[f"{col} OK"] = config == formatted_result[col]

    tests[f"{len(expected_cols)} cols present"] = (
        len(expected_cols) == len(formatted_result)
    )
    return tests


def get_foreign_keys(cursor, expected_keys, table_name):
    tests = {}
    cursor.execute(f"PRAGMA foreign_key_list({table_name});")
    result = cursor.fetchall()
    formatted_result = {
        f"{col[2]}_{col[1]}": col[3:] for col in result
    }
    for col, config in expected_keys.items():
        tests[f"{col} OK"] = config == formatted_result[col]

    tests[f"{len(expected_keys)} foreign keys present"] = (
        len(expected_keys) == len(formatted_result)
    )
    return tests


def get_indices(cursor, expected_indices, table_name):
    tests = {}
    cursor.execute(f"PRAGMA index_list({table_name});")
    result = cursor.fetchall()
    formatted_result = {
        col[1]: col[2:] for col in result
    }
    print(formatted_result)
    for col, config in expected_indices.items():
        tests[f"{col} OK"] = config == formatted_result[col]

    tests[f"{len(expected_indices)} indices present"] = (
        len(expected_indices) == len(formatted_result)
    )
    return tests


@pytest.mark.orm
def test_create_tables(db_path):
    tests = {}
    expected_tables = (
        "dim_device",
        "dim_header",
        "dim_flag",
        "fact_measurement"
    )
    conn = sql3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT name FROM sqlite_schema
        WHERE type='table' AND name NOT LIKE 'sqlite_%';
        """
    )
    tables_in_db = [i[0] for i in cursor.fetchall()]
    for table in expected_tables:
        tests[f"{table} is in db"] = table in tables_in_db
    tests[f"{len(expected_tables)} tables in db"] = (
        len(expected_tables) == len(tables_in_db)
    )
    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")
    assert all(tests.values())


@pytest.mark.orm
def test_dim_device_schema(db_path, sql_types):
    table_name = "dim_device"
    expected_col_structure = {
        "code": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "dataset": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "name": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "short_name": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "reference": (sql_types["SQLite"]["boolean"], 1, None, 0, 0),
        "other": (sql_types["SQLite"]["json"], 0, None, 0, 0),
    }
    expected_indices = {
        "sqlite_autoindex_dim_device_1": (1, "pk", 0),
        "sqlite_autoindex_dim_device_2": (1, "u", 0),
        "sqlite_autoindex_dim_device_3": (1, "u", 0)
    }

    conn = sql3.connect(db_path)
    cursor = conn.cursor()
    tests = get_table_cols(
        cursor,
        expected_col_structure,
        table_name
    )
    tests = tests | get_indices(
        cursor,
        expected_indices,
        table_name
    )
    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")
    assert all(tests.values())


@pytest.mark.orm
def test_dim_header_schema(db_path, sql_types):
    table_name = "dim_header"
    expected_col_structure = {
        "header": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "parameter": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "other": (sql_types["SQLite"]["json"], 0, None, 0, 0),
    }
    expected_indices = {
        "sqlite_autoindex_dim_header_1": (1, "pk", 0)
    }

    conn = sql3.connect(db_path)
    cursor = conn.cursor()
    tests = get_table_cols(
        cursor,
        expected_col_structure,
        table_name
    )
    tests = tests | get_indices(
        cursor,
        expected_indices,
        table_name
    )

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")
    assert all(tests.values())


@pytest.mark.orm
def test_dim_flag_schema(db_path, sql_types):
    table_name = "dim_flag"
    expected_col_structure = {
        "id": (sql_types["SQLite"]["int"], 1, None, 1, 0),
        "point_hash": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "flag": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "value": (sql_types["SQLite"]["string"], 1, None, 0, 0),
    }
    
    expected_foreign_keys = {
        "fact_measurement_0": ("point_hash", "point_hash", "NO ACTION", "NO ACTION", "NONE")
    }

    conn = sql3.connect(db_path)
    cursor = conn.cursor()
    tests = get_table_cols(cursor, expected_col_structure, table_name)
    tests = tests | get_foreign_keys(cursor, expected_foreign_keys, table_name)

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")
    assert all(tests.values())


@pytest.mark.orm
def test_fact_measurement_schema(db_path, sql_types):
    table_name = "fact_measurement"
    expected_col_structure = {
        "measurement_hash": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "point_hash": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "timestamp": (sql_types["SQLite"]["datetime"], 1, None, 0, 0),
        "code": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "header": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "value": (sql_types["SQLite"]["float"], 1, None, 0, 0),
    }
    expected_foreign_keys = {
        "dim_header_0": ("header", "header", "NO ACTION", "NO ACTION", "NONE"),
        "dim_device_0": ("code", "code", "NO ACTION", "NO ACTION", "NONE")
    }
    expected_indices = {
        "ix_measurement": (1, "c", 0),
        "ix_point_hash": (1, "c", 0),
        "sqlite_autoindex_fact_measurement_1": (1, "pk", 0)
    }

    conn = sql3.connect(db_path)
    cursor = conn.cursor()
    tests = get_table_cols(cursor, expected_col_structure, table_name)
    tests = tests | get_foreign_keys(cursor, expected_foreign_keys, table_name)
    tests = tests | get_indices(
        cursor,
        expected_indices,
        table_name
    )

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")
    assert all(tests.values())


@pytest.mark.orm
def test_dim_device_good(sqlite_connection):
    tests = {}
    good_data = [
        {
            "code": "ANT_123456",
            "dataset": "test",
            "name": "Antwerp 1",
            "short_name": "A1",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "code": "ANT_123567",
            "dataset": "test",
            "name": "Antwerp 2",
            "short_name": "A2",
            "reference": False,
            "other": None
        },
        {
            "code": "ANT_131245",
            "dataset": "test",
            "name": "Antwerp 3",
            "short_name": "A3",
            "reference": False,
            "other": None
        },
        {
            "code": "ANT_R000",
            "dataset": "test",
            "name": "Antwerp Ref 1",
            "short_name": "AR1",
            "reference": True,
            "other": {"key": "value"}
        },
        {
            "code": "ANT_R001",
            "dataset": "test",
            "name": "Antwerp Ref 2",
            "short_name": "AR2",
            "reference": True,
            "other": None
        }
    ]
    expected_pks = (
        ('ANT_123456',),
        ('ANT_123567',),
        ('ANT_131245',),
        ('ANT_R000',),
        ('ANT_R001',)
    )

    insert_statement = insert(orm.DimDevice)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.parametrize(
    "dupe_data", [
            {
                "code": "ANT_123456",
                "name": "Antwerp 1",
                "short_name": "A1",
                "dataset": "test",
                "reference": False,
                "other": None
            },
            {
                "code": "ANT_123456",
                "name": "Antwerp 4",
                "short_name": "A4",
                "dataset": "test",
                "reference": False,
                "other": None
            },
            {
                "code": "ANT_123457",
                "name": "Antwerp 1",
                "short_name": "A4",
                "dataset": "test",
                "reference": False,
                "other": None
            },
            {
                "code": "ANT_123457",
                "name": "Antwerp 4",
                "short_name": "A1",
                "dataset": "test",
                "reference": False,
                "other": None
            },
    ]
)
def test_dim_lcs_dupe(sqlite_connection, dupe_data):
    insert_statement = insert(orm.DimDevice)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"UNIQUE constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.orm
@pytest.mark.parametrize(
    "col_to_null", ["code", "name", "short_name"]
)
def test_dim_lcs_null(sqlite_connection, col_to_null):
    raw_data: dict[str, str | dt.datetime | int | float | None] = {
        "code": "ANT_123457",
        "name": "Antwerp 4",
        "short_name": "A4"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimDevice)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"NOT NULL constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
def test_dim_header_good(sqlite_connection):
    tests = {}
    good_data = [
        {
            "header": "ox_test",
            "parameter": "ox",
            "unit": "nA",
            "other": None
        },
        {
            "header": "no_test",
            "parameter": "no",
            "unit": "nA",
            "other": {"key": "value"}
        },
        {
            "header": "opc_test",
            "type": "OPC",
            "parameter": "pm2.5",
            "unit": "µg/m³",
            "other": None
        }
    ]
    expected_pks = (('ox_test',), ('no_test',), ('opc_test',))

    insert_statement = insert(orm.DimHeader)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
def test_dim_header_dupe(sqlite_connection):
    dupe_data = {
        "header": "ox_test",
        "parameter": "ox",
        "unit": "nA",
        "other": None
    }
    insert_statement = insert(orm.DimHeader)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"UNIQUE constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.orm
@pytest.mark.parametrize(
    "col_to_null", [
        "header",
        "parameter",
        "unit"
    ]
)
def test_dim_header_null(sqlite_connection, col_to_null):
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
        "header": "ox_test_2",
        "parameter": "ox",
        "unit": "nA",
        "other": {"key": "value"}
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimHeader)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"NOT NULL constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
def test_fact_measurement_good(sqlite_connection):
    tests = {}
    good_data = [
        {
            "measurement_hash": "test1",
            "point_hash": "othertest1",
            "timestamp": dt.datetime(2020, 1, 1),
            "code": "ANT_123456",
            "header": "ox_test",
            "value": 0.1
        },
        {
            "measurement_hash": "test2",
            "point_hash": "othertest2",
            "timestamp": dt.datetime(2020, 1, 2),
            "code": "ANT_131245",
            "header": "no_test",
            "value": 0.2
        },
        {
            "measurement_hash": "test3",
            "point_hash": "othertest3",
            "timestamp": dt.datetime(2020, 1, 3),
            "code": "ANT_123567",
            "header": "opc_test",
            "value": 0.3
        },
    ]
    expected_pks = (('test1',), ('test2',), ('test3',))

    insert_statement = insert(orm.FactMeasurement)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
def test_fact_measurement_dupe(sqlite_connection):
    dupe_data = {
            "measurement_hash": "test1",
            "point_hash": "othertest6",
            "timestamp": dt.datetime(2020, 1, 1),
            "code": "ANT_123456",
            "header": "ox_test",
            "value": 0.1
    }
    insert_statement = insert(orm.FactMeasurement)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"UNIQUE constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.orm
@pytest.mark.parametrize(
    "bad_key", [
        "code",
        "header"
    ]
)
def test_fact_measurement_bad_foreign_key(sqlite_connection, bad_key):
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
            "measurement_hash": "test4",
            "point_hash": "othertest4",
            "timestamp": dt.datetime(2020, 1, 3),
            "code": "ANT_123567",
            "header": "opc_test",
            "value": 0.3
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.FactMeasurement)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"FOREIGN KEY constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.parametrize(
    "col_to_null", [
        "measurement_hash",
        "point_hash",
        "timestamp",
        "code",
        "header",
        "value"
    ]
)
def test_fact_measurement_null(sqlite_connection, col_to_null):
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
            "measurement_hash": "test6",
            "point_hash": "othertest6",
            "timestamp": dt.datetime(2020, 1, 1),
            "code": "ANT_123456",
            "header": "ox_test",
            "value": 0.1
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.FactMeasurement)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"NOT NULL constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
def test_dim_flag_good(sqlite_connection):
    tests = {}
    good_data = [
        {
            "point_hash": "othertest1",
            "flag": "ox_test",
            "value": "a"
        },
        {
            "point_hash": "othertest2",
            "flag": "no_test",
            "value": "b"
        },
        {
            "point_hash": "othertest3",
            "flag": "opc_test",
            "value": "c"
        },
    ]
    expected_pks = ((None,), (None,), (None,))

    insert_statement = insert(orm.DimFlag)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.parametrize(
    "bad_key", [
        "point_hash"
    ]
)
def test_dim_flag_bad_foreign_key(sqlite_connection, bad_key):
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
        "point_hash": "othertest4",
        "flag": "ox_test",
        "value": "a"
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.DimFlag)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"FOREIGN KEY constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.parametrize(
    "col_to_null", [
        "point_hash",
        "flag",
        "value"
    ]
)
def test_dim_lcs_flags_null(sqlite_connection, col_to_null):
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
        "point_hash": "othertest5",
        "flag": "ox_test",
        "value": "a"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimFlag)
    with sqlite_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"NOT NULL constraint failed"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )
