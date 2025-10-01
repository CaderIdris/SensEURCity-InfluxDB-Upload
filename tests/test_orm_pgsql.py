"""Tests the orm.py module with a PostgreSQL db.

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

import pytest
from sqlalchemy import insert
import sqlalchemy.exc as sqlexc

from senseurcity import orm
from senseurcity import engine

try:
    import psycopg
    from testcontainers.postgres import PostgresContainer
except ImportError:
    pytest.skip(
        "TestContainers not set up, please use the dev-postgres group",
        allow_module_level=True
    )

postgres = PostgresContainer("postgres:18-alpine", driver="psycopg")


@pytest.fixture(scope="session", autouse=True)
def setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)


@pytest.fixture(scope="module")
def db_url():
    return postgres.get_connection_url()


@pytest.fixture(scope="module", autouse=True)
def pgsql_connection(db_url):
    db_engine = engine.get_engine(db_url)
    orm._Base.metadata.create_all(db_engine)
    return db_engine


def get_table_cols(cursor, expected_cols, table_name):
    tests = {}
    cursor.execute(
        f"""
        SELECT
            column_name,
            is_nullable,
            data_type,
            character_maximum_length
        FROM information_schema.columns 
        WHERE table_schema = 'measurement'
        AND table_name = '{table_name}'
        """)
    result = cursor.fetchall()
    formatted_result = {
        col[0]: col[1:] for col in result
    }
    print(formatted_result)
    for col, config in expected_cols.items():
        tests[f"{col} OK"] = config == formatted_result[col]

    tests[f"{len(expected_cols)} cols present"] = (
        len(expected_cols) == len(formatted_result)
    )
    return tests


def get_foreign_keys(cursor, expected_keys, table_name):
    tests = {}
    cursor.execute(
        # Taken from https://stackoverflow.com/a/1152321
        f"""
        SELECT
            tc.constraint_name, 
            tc.table_name, 
            kcu.column_name, 
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name 
        FROM information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema='measurement'
            AND tc.table_name='{table_name}';
        """)
    result = cursor.fetchall()
    formatted_result = {
        col[0]: col[1:] for col in result
    }
    print(formatted_result)
    for col, config in expected_keys.items():
        tests[f"{col} OK"] = config == formatted_result[col]

    tests[f"{len(expected_keys)} foreign keys present"] = (
        len(expected_keys) == len(formatted_result)
    )
    return tests


def get_indices(cursor, expected_indices, table_name):
    tests = {}
    cursor.execute(
        f"""
        SELECT * 
        FROM pg_indexes
        WHERE tablename = '{table_name}';
        """
    )
    result = cursor.fetchall()
    formatted_result = [
        col[2] for col in result
    ]
    print(*formatted_result, sep='\n')
    for col in expected_indices:
        tests[f"{col} OK"] = col in expected_indices

    tests[f"{len(expected_indices)} indices present"] = (
        len(expected_indices) == len(formatted_result)
    )
    return tests


@pytest.mark.orm
def test_create_tables(db_url):
    print(db_url)
    tests = {}
    expected_tables = (
        "dim_device",
        "dim_header",
        "dim_flag",
        "fact_measurement"
    )
    with psycopg.connect(db_url.replace("+psycopg", "")) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * 
                FROM information_schema.tables
                WHERE table_schema = 'measurement';
                """
            )
            result = cur.fetchall()
    tables_in_db = [i[2] for i in result]
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
def test_dim_device_schema(db_url, sql_types):
    table_name = "dim_device"
    expected_col_structure = {
        "code": ('NO',sql_types["PostgreSQL"]["string"], None),
        "dataset": ('NO',sql_types["PostgreSQL"]["string"], None),
        "name": ('NO',sql_types["PostgreSQL"]["string"], None),
        "short_name": ('NO',sql_types["PostgreSQL"]["string"], None),
        "reference": ('NO',sql_types["PostgreSQL"]["boolean"], None),
        "other": ('YES',sql_types["PostgreSQL"]["json"], None),
    }
    expected_indices = (
        "dim_device_pkey",
        "dim_device_name_key",
        "dim_device_short_name_key"
    )

    with psycopg.connect(db_url.replace("+psycopg", "")) as conn:
        with conn.cursor() as cursor:
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
#
#
@pytest.mark.orm
def test_dim_header_schema(db_url, sql_types):
    table_name = "dim_header"
    expected_col_structure = {
        "header": ('NO', sql_types["PostgreSQL"]["string"], None),
        "parameter": ('NO', sql_types["PostgreSQL"]["string"], None),
        "unit": ('NO', sql_types["PostgreSQL"]["string"], None),
        "other": ('YES', sql_types["PostgreSQL"]["json"], None)
    }
    expected_indices = [
        "dim_flag_pkey"
    ]

    with psycopg.connect(db_url.replace("+psycopg", "")) as conn:
        with conn.cursor() as cursor:
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
def test_dim_flag_schema(db_url, sql_types):
    table_name = "dim_flag"
    expected_col_structure = {
        "id": ('NO', sql_types["PostgreSQL"]["int"], None),
        "point_hash": ('NO', sql_types["PostgreSQL"]["string"], None),
        "flag": ('NO', sql_types["PostgreSQL"]["string"], None),
        "value": ('NO', sql_types["PostgreSQL"]["string"], None)
    }
    expected_indices = [
        "dim_flag_pkey"
    ]
    expected_foreign_keys = {
        "dim_flag_point_hash_fkey": ("dim_flag", "point_hash", "fact_measurement", "point_hash")
    }

    with psycopg.connect(db_url.replace("+psycopg", "")) as conn:
        with conn.cursor() as cursor:
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


@pytest.mark.orm
def test_fact_measurement_schema(db_url, sql_types):
    table_name = "fact_measurement"
    expected_col_structure = {
        "measurement_hash": ('NO', sql_types["PostgreSQL"]["string"], None),
        "point_hash": ('NO', sql_types["PostgreSQL"]["string"], None),
        "timestamp": ('NO', sql_types["PostgreSQL"]["datetime"], None),
        "code": ('NO', sql_types["PostgreSQL"]["string"], None),
        "header": ('NO', sql_types["PostgreSQL"]["string"], None),
        "value": ('NO', sql_types["PostgreSQL"]["float"], None),
    }
    expected_foreign_keys = {
        "fact_measurement_code_fkey": ('fact_measurement', 'code', 'dim_device', 'code'),
        "fact_measurement_header_fkey": ('fact_measurement', 'header', 'dim_header', 'header'),
    }
    expected_indices = [
        "fact_measurement_pkey",
        "ix_measurement",
        "ix_point_hash"
    ]

    with psycopg.connect(db_url.replace("+psycopg", "")) as conn:
        with conn.cursor() as cursor:
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
def test_dim_device_good(pgsql_connection):
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
    with pgsql_connection.connect() as conn:
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
def test_dim_lcs_dupe(pgsql_connection, dupe_data):
    insert_statement = insert(orm.DimDevice)
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates unique constraint"
        ):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.orm
@pytest.mark.parametrize(
    "col_to_null", ["code", "name", "short_name"]
)
def test_dim_lcs_null(pgsql_connection, col_to_null):
    raw_data: dict[str, str | dt.datetime | int | float | None] = {
        "code": "ANT_123457",
        "name": "Antwerp 4",
        "short_name": "A4"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimDevice)
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
def test_dim_header_good(pgsql_connection):
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
    with pgsql_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
def test_dim_header_dupe(pgsql_connection):
    dupe_data = {
        "header": "ox_test",
        "parameter": "ox",
        "unit": "nA",
        "other": None
    }
    insert_statement = insert(orm.DimHeader)
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates unique constraint"
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
def test_dim_header_null(pgsql_connection, col_to_null):
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
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
def test_fact_measurement_good(pgsql_connection):
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
    with pgsql_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
def test_fact_measurement_dupe(pgsql_connection):
    dupe_data = {
            "measurement_hash": "test1",
            "point_hash": "othertest6",
            "timestamp": dt.datetime(2020, 1, 1),
            "code": "ANT_123456",
            "header": "ox_test",
            "value": 0.1
    }
    insert_statement = insert(orm.FactMeasurement)
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates unique constraint"
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
def test_fact_measurement_bad_foreign_key(pgsql_connection, bad_key):
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
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates foreign key constraint"
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
def test_fact_measurement_null(pgsql_connection, col_to_null):
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
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
def test_dim_flag_good(pgsql_connection):
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
    with pgsql_connection.connect() as conn:
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
def test_dim_flag_bad_foreign_key(pgsql_connection, bad_key):
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
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates foreign key constraint"
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
def test_dim_lcs_flags_null(pgsql_connection, col_to_null):
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
    with pgsql_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )
