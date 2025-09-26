import datetime as dt
import sqlite3 as sql3

import pandas as pd
import pytest
from sqlalchemy import create_engine, insert
import sqlalchemy.exc as sqlexc
from typing import Any

from senseurcity import orm
from senseurcity import engine


@pytest.fixture(scope="session")
def db_path(tmp_path_factory):
    return tmp_path_factory.mktemp("db") / "test_orm.db"


@pytest.fixture(autouse=True)
def sqlite_connection(db_path):
    db_engine = engine.get_engine(f"sqlite+pysqlite:///{db_path}")
    orm._Base.metadata.create_all(db_engine)
    return db_engine


@pytest.fixture
def sql_types():
    return {
        "SQLite": {
            "string": "VARCHAR",
            "int": "INTEGER",
            "float": "FLOAT",
            "datetime": "DATETIME"
        }
    }


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
    for col, config in expected_indices.items():
        tests[f"{col} OK"] = config == formatted_result[col]

    tests[f"{len(expected_indices)} indices present"] = (
        len(expected_indices) == len(formatted_result)
    )
    return tests


def test_create_tables(db_path):
    tests = {}
    expected_tables = (
        "dim_lcs",
        "dim_ref",
        "dim_header",
        "fact_lcs",
        "fact_ref",
        "dim_lcs_flags",
        "dim_lcs_colocation"
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


def test_dim_lcs(db_path, sql_types):
    table_name = "dim_lcs"
    expected_col_structure = {
        "code": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "name": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "short_name": (sql_types["SQLite"]["string"], 1, None, 0, 0),
    }
    expected_indices = {
        "sqlite_autoindex_dim_lcs_1": (1, "pk", 0),
        "sqlite_autoindex_dim_lcs_2": (1, "u", 0),
        "sqlite_autoindex_dim_lcs_3": (1, "u", 0)
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


def test_dim_header(db_path, sql_types):
    table_name = "dim_header"
    expected_col_structure = {
        "header": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "supplier": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "sensor": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "type": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "parameters": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "units": (sql_types["SQLite"]["string"], 1, None, 0, 0),
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


def test_dim_lcs_flags(db_path, sql_types):
    table_name = "dim_lcs_flags"
    expected_col_structure = {
        "id": (sql_types["SQLite"]["int"], 1, None, 1, 0),
        "point_hash": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "flag": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "value": (sql_types["SQLite"]["string"], 1, None, 0, 0),
    }
    expected_indices = {
        "sqlite_autoindex_dim_lcs_flags_1": (1, "u", 0),
    }
    expected_foreign_keys = {
        "fact_lcs_0": ("point_hash", "point_hash", "NO ACTION", "NO ACTION", "NONE")
    }

    conn = sql3.connect(db_path)
    cursor = conn.cursor()
    tests = get_table_cols(cursor, expected_col_structure, table_name)
    tests = tests | get_indices(
        cursor,
        expected_indices,
        table_name
    )
    tests = tests | get_foreign_keys(cursor, expected_foreign_keys, table_name)

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")
    assert all(tests.values())


def test_dim_lcs_colocation(db_path, sql_types):
    table_name = "dim_lcs_colocation"
    expected_col_structure = {
        "point_hash": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "location_id": (sql_types["SQLite"]["string"], 1, None, 0, 0),
    }
    expected_indices = {
        "sqlite_autoindex_dim_lcs_colocation_1": (1, "pk", 0),
    }
    expected_foreign_keys = {
        "fact_lcs_0": ("point_hash", "point_hash", "NO ACTION", "NO ACTION", "NONE"),
        "dim_ref_0": ("location_id", "location_id", "NO ACTION", "NO ACTION", "NONE")
    }

    conn = sql3.connect(db_path)
    cursor = conn.cursor()
    tests = get_table_cols(cursor, expected_col_structure, table_name)
    tests = tests | get_indices(
        cursor,
        expected_indices,
        table_name
    )
    tests = tests | get_foreign_keys(cursor, expected_foreign_keys, table_name)

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")
    assert all(tests.values())


def test_dim_ref(db_path, sql_types):
    table_name = "dim_ref"
    expected_col_structure = {
        "location_id": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "name": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "short_name": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "city": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "latitude_dd": (sql_types["SQLite"]["float"], 1, None, 0, 0),
        "longitude_dd": (sql_types["SQLite"]["float"], 1, None, 0, 0),
        "distance_to_road_m": (sql_types["SQLite"]["int"], 1, None, 0, 0),
        "average_hourly_traffic_intensity_number_per_h": (
            (sql_types["SQLite"]["string"], 1, None, 0, 0)
        ),
        "notes": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "co_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "co_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "co2_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "co2_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "no_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "no_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "no2_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "no2_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "o3_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "o3_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "pm10_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "pm10_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "pm25_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "pm25_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "pm1_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "pm1_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "other_pm10_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "other_pm10_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "other_pm25_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "other_pm25_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "other_pm1_equipment": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "other_pm1_unit": (sql_types["SQLite"]["string"], 1, None, 0, 0)
    }
    expected_indices = {
        "sqlite_autoindex_dim_ref_1": (1, "pk", 0),
        "sqlite_autoindex_dim_ref_2": (1, "u", 0),
        "sqlite_autoindex_dim_ref_3": (1, "u", 0)
    }

    conn = sql3.connect(db_path)
    cursor = conn.cursor()
    tests = get_table_cols(cursor, expected_col_structure, table_name)
    tests = tests | get_indices(
        cursor,
        expected_indices,
        table_name
    )

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")
    assert all(tests.values())


def test_fact_lcs(db_path, sql_types):
    table_name = "fact_lcs"
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
        "dim_lcs_0": ("code", "code", "NO ACTION", "NO ACTION", "NONE")
    }
    expected_indices = {
        "ix_lcs_measurements": (1, "c", 0),
        "ix_lcs_point_hash": (1, "c", 0),
        "sqlite_autoindex_fact_lcs_1": (1, "pk", 0)
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


def test_fact_ref(db_path, sql_types):
    table_name = "fact_ref"
    expected_col_structure = {
        "measurement_hash": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "timestamp": (sql_types["SQLite"]["datetime"], 1, None, 0, 0),
        "location_id": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "measurement": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "value": (sql_types["SQLite"]["float"], 1, None, 0, 0),
    }
    expected_foreign_keys = {
        "dim_ref_0": ("location_id", "location_id", "NO ACTION", "NO ACTION", "NONE"),
    }
    expected_indices = {
        "ix_ref_measurements": (1, "c", 0),
        "sqlite_autoindex_fact_ref_1": (1, "pk", 0)
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


def test_dim_lcs_good(sqlite_connection):
    tests = {}
    good_data = [
        {"code": "ANT_123456", "name": "Antwerp 1", "short_name": "A1"},
        {"code": "ANT_123567", "name": "Antwerp 2", "short_name": "A2"},
        {"code": "ANT_131245", "name": "Antwerp 3", "short_name": "A3"}
    ]
    expected_pks = (('ANT_123456',), ('ANT_123567',), ('ANT_131245',))

    insert_statement = insert(orm.DimLCS)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.parametrize(
    "dupe_data", [
            {"code": "ANT_123456", "name": "Antwerp 1", "short_name": "A1"},
            {"code": "ANT_123456", "name": "Antwerp 4", "short_name": "A4"},
            {"code": "ANT_123457", "name": "Antwerp 1", "short_name": "A4"},
            {"code": "ANT_123457", "name": "Antwerp 4", "short_name": "A1"},
    ]
)
def test_dim_lcs_dupe(sqlite_connection, dupe_data):
    insert_statement = insert(orm.DimLCS)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.parametrize(
    "col_to_null", ["code", "name", "short_name"]
)
def test_dim_lcs_null(sqlite_connection, col_to_null):
    raw_data = {
        "code": "ANT_123457",
        "name": "Antwerp 4",
        "short_name": "A4"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimLCS)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


def test_dim_header_good(sqlite_connection):
    tests = {}
    good_data = [
        {
            "header": "ox_test",
            "supplier": "Test1",
            "sensor": "ox_test",
            "type": "Electrochemical",
            "parameters": "ox",
            "units": "nA"
        },
        {
            "header": "no_test",
            "supplier": "Test1",
            "sensor": "no_test",
            "type": "Electrochemical",
            "parameters": "no",
            "units": "nA"
        },
        {
            "header": "opc_test",
            "supplier": "Test2",
            "sensor": "opc_test",
            "type": "OPC",
            "parameters": "pm2.5",
            "units": "µg/m³"
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


def test_dim_header_dupe(sqlite_connection):
    dupe_data = {
        "header": "ox_test",
        "supplier": "Test1",
        "sensor": "ox_test",
        "type": "Electrochemical",
        "parameters": "ox",
        "units": "nA"
    }
    insert_statement = insert(orm.DimHeader)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.parametrize(
    "col_to_null", [
        "header",
        "supplier",
        "sensor",
        "type",
        "parameters",
        "units"
    ]
)
def test_dim_header_null(sqlite_connection, col_to_null):
    raw_data = {
            "header": "ox_test_2",
            "supplier": "Test1",
            "sensor": "ox_test",
            "type": "Electrochemical",
            "parameters": "ox",
            "units": "nA"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimHeader)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


def test_fact_lcs_good(sqlite_connection):
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

    insert_statement = insert(orm.FactLCS)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


def test_fact_lcs_dupe(sqlite_connection):
    dupe_data = {
            "measurement_hash": "test1",
            "point_hash": "othertest6",
            "timestamp": dt.datetime(2020, 1, 1),
            "code": "ANT_123456",
            "header": "ox_test",
            "value": 0.1
    }
    insert_statement = insert(orm.FactLCS)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.parametrize(
    "bad_key", [
        "code",
        "header"
    ]
)
def test_fact_lcs_bad_foreign_key(sqlite_connection, bad_key):
    raw_data = {
            "measurement_hash": "test4",
            "point_hash": "othertest4",
            "timestamp": dt.datetime(2020, 1, 3),
            "code": "ANT_123567",
            "header": "opc_test",
            "value": 0.3
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.FactLCS)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


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
def test_fact_lcs_null(sqlite_connection, col_to_null):
    raw_data = {
            "measurement_hash": "test6",
            "point_hash": "othertest6",
            "timestamp": dt.datetime(2020, 1, 1),
            "code": "ANT_123456",
            "header": "ox_test",
            "value": 0.1
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.FactLCS)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


def test_dim_lcs_flags_good(sqlite_connection):
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

    insert_statement = insert(orm.DimLCSFlags)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.parametrize(
    "bad_key", [
        "point_hash"
    ]
)
def test_dim_lcs_flags_bad_foreign_key(sqlite_connection, bad_key):
    raw_data = {
        "point_hash": "othertest4",
        "flag": "ox_test",
        "value": "a"
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.DimLCSFlags)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.parametrize(
    "col_to_null", [
        "point_hash",
        "flag",
        "value"
    ]
)
def test_dim_lcs_flags_null(sqlite_connection, col_to_null):
    raw_data = {
        "point_hash": "othertest5",
        "flag": "ox_test",
        "value": "a"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimLCSFlags)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


def test_dim_ref_good(sqlite_connection):
    tests = {}
    good_data = [
        {
            "location_id": "TEST_1",
            "name": "Testland site",
            "short_name": "TName",
            "city": "testland",
            "latitude_dd": 51.1739,
            "longitude_dd": -1.82237,
            "distance_to_road_m": 125,
            "average_hourly_traffic_intensity_number_per_h": "Lots",
            "notes": "Test note",
            "co_equipment": "test co",
            "co_unit": "ppm",
            "co2_equipment": "test co2",
            "co2_unit": "ppm",
            "no_equipment": "test no",
            "no_unit": "ppb",
            "no2_equipment": "test no2",
            "no2_unit": "ppb",
            "o3_equipment": "test o3",
            "o3_unit": "ppb",
            "pm10_equipment": "test pm10",
            "pm10_unit": "µg/m³",
            "pm25_equipment": "test pm25",
            "pm25_unit": "µg/m³",
            "pm1_equipment": "test pm1",
            "pm1_unit": "µg/m³",
            "other_pm10_equipment": "other test pm10",
            "other_pm10_unit": "µg/m³",
            "other_pm25_equipment": "other test pm25",
            "other_pm25_unit": "µg/m³",
            "other_pm1_equipment": "other test pm1",
            "other_pm1_unit": "µg/m³",
        },
        {
            "location_id": "TEST_2",
            "name": "Testberg site",
            "short_name": "TBerg",
            "city": "testberg",
            "latitude_dd": 52.219023,
            "longitude_dd": 3.65553,
            "distance_to_road_m": 2000,
            "average_hourly_traffic_intensity_number_per_h": "None",
            "notes": "Test note",
            "co_equipment": "test co",
            "co_unit": "ppm",
            "co2_equipment": "test co2",
            "co2_unit": "ppm",
            "no_equipment": "test no",
            "no_unit": "ppb",
            "no2_equipment": "test no2",
            "no2_unit": "ppb",
            "o3_equipment": "test o3",
            "o3_unit": "ppb",
            "pm10_equipment": "test pm10",
            "pm10_unit": "µg/m³",
            "pm25_equipment": "test pm25",
            "pm25_unit": "µg/m³",
            "pm1_equipment": "test pm1",
            "pm1_unit": "µg/m³",
            "other_pm10_equipment": "other test pm10",
            "other_pm10_unit": "µg/m³",
            "other_pm25_equipment": "other test pm25",
            "other_pm25_unit": "µg/m³",
            "other_pm1_equipment": "other test pm1",
            "other_pm1_unit": "µg/m³",
        },
        {
            "location_id": "TEST_3",
            "name": "Test City site",
            "short_name": "TCity",
            "city": "test city",
            "latitude_dd": 53.17948,
            "longitude_dd": -4.06126,
            "distance_to_road_m": 0,
            "average_hourly_traffic_intensity_number_per_h": "Lots",
            "notes": "Lots of bugs here",
            "co_equipment": "test co",
            "co_unit": "ppm",
            "co2_equipment": "test co2",
            "co2_unit": "ppm",
            "no_equipment": "test no",
            "no_unit": "ppb",
            "no2_equipment": "test no2",
            "no2_unit": "ppb",
            "o3_equipment": "test o3",
            "o3_unit": "ppb",
            "pm10_equipment": "test pm10",
            "pm10_unit": "µg/m³",
            "pm25_equipment": "test pm25",
            "pm25_unit": "µg/m³",
            "pm1_equipment": "test pm1",
            "pm1_unit": "µg/m³",
            "other_pm10_equipment": "other test pm10",
            "other_pm10_unit": "µg/m³",
            "other_pm25_equipment": "other test pm25",
            "other_pm25_unit": "µg/m³",
            "other_pm1_equipment": "other test pm1",
            "other_pm1_unit": "µg/m³",
        }
    ]
    expected_pks = (("TEST_1",), ("TEST_2",), ("TEST_3",))

    insert_statement = insert(orm.DimRef)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


def test_dim_ref_dupe(sqlite_connection):
    dupe_data = {
            "location_id": "TEST_1",
            "name": "Testland site",
            "short_name": "TName",
            "city": "testland",
            "latitude_dd": 51.1739,
            "longitude_dd": -1.82237,
            "distance_to_road_m": 125,
            "average_hourly_traffic_intensity_number_per_h": "Lots",
            "notes": "Test note",
            "co_equipment": "test co",
            "co_unit": "ppm",
            "co2_equipment": "test co2",
            "co2_unit": "ppm",
            "no_equipment": "test no",
            "no_unit": "ppb",
            "no2_equipment": "test no2",
            "no2_unit": "ppb",
            "o3_equipment": "test o3",
            "o3_unit": "ppb",
            "pm10_equipment": "test pm10",
            "pm10_unit": "µg/m³",
            "pm25_equipment": "test pm25",
            "pm25_unit": "µg/m³",
            "pm1_equipment": "test pm1",
            "pm1_unit": "µg/m³",
            "other_pm10_equipment": "other test pm10",
            "other_pm10_unit": "µg/m³",
            "other_pm25_equipment": "other test pm25",
            "other_pm25_unit": "µg/m³",
            "other_pm1_equipment": "other test pm1",
            "other_pm1_unit": "µg/m³",
    }
    insert_statement = insert(orm.DimRef)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.parametrize(
    "col_to_null", [
        "location_id",
        "name",
        "short_name",
        "city",
        "latitude_dd",
        "longitude_dd"
    ]
)
def test_dim_ref_null(sqlite_connection, col_to_null):
    raw_data = {
            "location_id": "TEST_4",
            "name": "Testland site",
            "short_name": "TName",
            "city": "testland",
            "latitude_dd": 51.1739,
            "longitude_dd": -1.82237,
            "distance_to_road_m": 125,
            "average_hourly_traffic_intensity_number_per_h": "Lots",
            "notes": "Test note",
            "co_equipment": "test co",
            "co_unit": "ppm",
            "co2_equipment": "test co2",
            "co2_unit": "ppm",
            "no_equipment": "test no",
            "no_unit": "ppb",
            "no2_equipment": "test no2",
            "no2_unit": "ppb",
            "o3_equipment": "test o3",
            "o3_unit": "ppb",
            "pm10_equipment": "test pm10",
            "pm10_unit": "µg/m³",
            "pm25_equipment": "test pm25",
            "pm25_unit": "µg/m³",
            "pm1_equipment": "test pm1",
            "pm1_unit": "µg/m³",
            "other_pm10_equipment": "other test pm10",
            "other_pm10_unit": "µg/m³",
            "other_pm25_equipment": "other test pm25",
            "other_pm25_unit": "µg/m³",
            "other_pm1_equipment": "other test pm1",
            "other_pm1_unit": "µg/m³",
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimRef)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


def test_dim_lcs_colocation_good(sqlite_connection):
    tests = {}
    good_data = [
        {
            "point_hash": "othertest1",
            "location_id": "TEST_1"
        },
        {
            "point_hash": "othertest2",
            "location_id": "TEST_2"
        },
    ]
    expected_pks = (("othertest1",), ("othertest2",))

    insert_statement = insert(orm.DimLCSColocation)
    with sqlite_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


def test_dim_lcs_colocation_dupe(sqlite_connection):
    dupe_data = {
        "point_hash": "othertest1",
        "location_id": "TEST_1"
    }
    insert_statement = insert(orm.DimLCSColocation)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.parametrize(
    "col_to_null", [
        "point_hash",
        "location_id"
    ]
)
def test_dim_lcs_colocation_null(sqlite_connection, col_to_null):
    raw_data = {
        "point_hash": "othertest3",
        "location_id": "TEST_3"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimLCSColocation)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.parametrize(
    "bad_key", [
        "point_hash",
        "location_id"
    ]
)
def test_dim_lcs_flags_bad_foreign_key(sqlite_connection, bad_key):
    raw_data = {
        "point_hash": "othertest3",
        "location_id": "TEST_3"
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.DimLCSColocation)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                raw_data
            )
