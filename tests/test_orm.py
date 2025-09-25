import sqlite3 as sql3

import pandas as pd
import pytest
from sqlalchemy import create_engine, insert
import sqlalchemy.exc as sqlexc
from typing import Any

from senseurcity import orm


@pytest.fixture(scope="session")
def db_path(tmp_path_factory):
    return tmp_path_factory.mktemp("db") / "test_orm.db"


@pytest.fixture(autouse=True)
def sqlite_connection(db_path):
    engine = create_engine(f"sqlite+pysqlite:///{db_path}")
    orm._Base.metadata.create_all(engine)
    return engine


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
    print(formatted_result)
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
        "dim_lcs_flags"
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
        "measurement_hash": (sql_types["SQLite"]["string"], 1, None, 1, 0),
        "flag": (sql_types["SQLite"]["string"], 1, None, 0, 0),
        "value": (sql_types["SQLite"]["string"], 1, None, 0, 0),
    }
    expected_indices = {
        "sqlite_autoindex_dim_lcs_flags_1": (1, "pk", 0),
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
    tests = {}

    insert_statement = insert(orm.DimLCS)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )

@pytest.mark.parametrize(
    "dupe_data", [
            {"code": None, "name": None, "short_name": None},
            {"code": None, "name": "Antwerp 4", "short_name": "A4"},
            {"code": "ANT_123457", "name": None, "short_name": "A4"},
            {"code": "ANT_123457", "name": "Antwerp 4", "short_name": None},
    ]
)
def test_dim_lcs_null(sqlite_connection, dupe_data):
    tests = {}

    insert_statement = insert(orm.DimLCS)
    with sqlite_connection.connect() as conn:
        with pytest.raises(sqlexc.IntegrityError):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )
