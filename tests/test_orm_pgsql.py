"""Tests the orm.py module with a PostgreSQL db.

Tests
-----
- Are tables created?
    - Tests whether all tables are created in db
- Are table schemas valid?
    - `dim_device`
    - `dim_header`
    - `dim_colocation`
    - `fact_measurement`
    - `fact_value`
    - `fact_flag`
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
#TODO: Sort ^
import datetime as dt

import pytest
from sqlalchemy import insert
import sqlalchemy.exc as sqlexc

from senseurcity import orm
from senseurcity import engine

try:
    from docker.errors import DockerException
    import psycopg
    from testcontainers.postgres import PostgresContainer
except ImportError:
    pytest.skip(
        "TestContainers not set up, please use the dev-postgres group",
        allow_module_level=True
    )

try:
    postgres = PostgresContainer("postgres:18-alpine", driver="psycopg")
except DockerException:
    pytest.skip(
        "Docker permissions issue",
        allow_module_level=True
    )


@pytest.fixture(scope="session", autouse=True)
def setup(request):
    """Setup the postgres docker container."""
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)


@pytest.fixture(scope="module")
def db_url():
    """Get the postgres connection url for the container."""
    return postgres.get_connection_url()


@pytest.fixture(scope="module", autouse=True)
def postgres_connection(db_url):
    """SQLAlchemy connection to the postgres db."""
    db_engine = engine.get_engine(db_url)
    orm._Base_V1.metadata.create_all(db_engine)
    return db_engine


@pytest.fixture(scope="module", autouse=True)
def postgres_connection_alt_schema(db_url):
    """SQLAlchemy connection to the postgres db with an alternative schema."""
    db_engine = engine.get_engine(db_url, schema_name="test_alt_schema")
    orm.create_tables(db_engine)
    return db_engine


def get_table_cols(cursor, expected_cols, table_name):
    """Check if expected columns are in table."""
    tests: dict[str, bool] = {}
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
    for col, config in expected_cols.items():
        tests[f"{col} OK"] = config == formatted_result[col]

    tests[f"{len(expected_cols)} cols present"] = (
        len(expected_cols) == len(formatted_result)
    )
    return tests


def get_foreign_keys(cursor, expected_keys, table_name):
    """Check if expected foreign keys are associated with table."""
    tests: dict[str, bool] = {}
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
    for col, config in expected_keys.items():
        tests[f"{col} OK"] = config == formatted_result[col]

    tests[f"{len(expected_keys)} foreign keys present"] = (
        len(expected_keys) == len(formatted_result)
    )
    return tests


def get_indices(cursor, expected_indices, table_name):
    """Check if expected indices are associated with table."""
    tests: dict[str, bool] = {}
    cursor.execute(
        f"""
        SELECT * 
        FROM pg_indexes
        WHERE tablename = '{table_name}'
        AND schemaname = 'measurement';
        """
    )
    result = cursor.fetchall()
    formatted_result = [
        col[2] for col in result
    ]
    for col in expected_indices:
        tests[f"{col} OK"] = col in expected_indices

    tests[f"{len(expected_indices)} indices present"] = (
        len(expected_indices) == len(formatted_result)
    )
    return tests


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize("schema_name", ["measurement", "test_alt_schema"])
def test_create_tables(db_url, schema_name):
    """Test whether the tables were created.

    Tests
    -----
    - Each expected table is in db.
    - Correct number of tables.
    """
    tests: dict[str, bool] = {}
    expected_tables = (
        "dim_device",
        "dim_header",
        "dim_colocation",
        "dim_unit_conversion",
        "fact_measurement",
        "fact_value",
        "fact_flag"
    )
    with psycopg.connect(db_url.replace("+psycopg", "")) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT * 
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}';
                """
            )
            result = cur.fetchall()

    tables_in_db = [i[2] for i in result]

    for table in expected_tables:
        tests[f"{table} is in db"] = (table in tables_in_db)

    tests[f"{len(expected_tables)} tables in db"] = (
        len(expected_tables) == len(tables_in_db)
    )

    for test, outcome in tests.items():
        if not outcome:
            print(f"{test}: {outcome}")

    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_dim_device_schema(db_url, sql_types):
    """Test whether `dim_device` is set up correctly.

    Tests
    -----
    - Each expected column present with proper type and configuration.
    - Correct number of columns.
    - Each expected index present.
    - Correct number of indexes.
    """
    table_name = "dim_device"
    expected_col_structure = {
        "key": ('NO',sql_types["PostgreSQL"]["string"], None),
        "name": ('NO',sql_types["PostgreSQL"]["string"], None),
        "short_name": ('NO',sql_types["PostgreSQL"]["string"], None),
        "dataset": ('NO',sql_types["PostgreSQL"]["string"], None),
        "reference": ('NO',sql_types["PostgreSQL"]["boolean"], None),
        "other": ('YES',sql_types["PostgreSQL"]["json"], None),
    }
    expected_indices = (
        "dim_device_pkey",
        "dim_device_name_key",
        "dim_device_short_name"
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


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_dim_header_schema(db_url, sql_types):
    """Test whether `dim_header` is set up correctly.

    Tests
    -----
    - Each expected column present with proper type and configuration.
    - Correct number of columns.
    - Each expected index present.
    - Correct number of indexes.
    """
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
@pytest.mark.postgres
@pytest.mark.base_v1
def test_fact_measurement_schema(db_url, sql_types):
    """Test whether `fact_measurement` is set up correctly.

    Tests
    -----
    - Each expected column present with proper type and configuration.
    - Correct number of columns.
    - Each expected foreign key present
    - Correct number of foreign keys.
    - Each expected index present.
    - Correct number of indexes.
    """
    table_name = "fact_measurement"
    expected_col_structure = {
        "point_hash": ('NO', sql_types["PostgreSQL"]["string"], None),
        "timestamp": ('NO', sql_types["PostgreSQL"]["datetime"], None),
        "device_key": ('NO', sql_types["PostgreSQL"]["string"], None)
    }
    expected_foreign_keys = {
        "fact_measurement_device_key_fkey": ('fact_measurement', 'device_key', 'dim_device', 'key'),
    }
    expected_indices = [
        "fact_measurement_pkey",
        "fact_measurement_timestamp_device_key_key"
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
@pytest.mark.postgres
@pytest.mark.base_v1
def test_fact_value_schema(db_url, sql_types):
    """Test whether `fact_value` is set up correctly.

    Tests
    -----
    - Each expected column present with proper type and configuration.
    - Correct number of columns.
    - Each expected foreign key present
    - Correct number of foreign keys.
    """
    table_name = "fact_value"
    expected_col_structure = {
        "id": ('NO', sql_types["PostgreSQL"]["int"], None),
        "point_hash": ('NO', sql_types["PostgreSQL"]["string"], None),
        "header": ('NO', sql_types["PostgreSQL"]["string"], None),
        "value": ('NO', sql_types["PostgreSQL"]["float"], None)
    }
    expected_indices = [
        "fact_value_pkey",
        "fact_value_point_hash_header_key",
    ]
    expected_foreign_keys = {
        "fact_value_point_hash_fkey": ("fact_value", "point_hash", "fact_measurement", "point_hash"),
        "fact_value_header_fkey": ("fact_value", "header", "dim_header", "header")
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
@pytest.mark.postgres
@pytest.mark.base_v1
def test_fact_flag_schema(db_url, sql_types):
    """Test whether `fact_flag` is set up correctly.

    Tests
    -----
    - Each expected column present with proper type and configuration.
    - Correct number of columns.
    - Each expected foreign key present
    - Correct number of foreign keys.
    """
    table_name = "fact_flag"
    expected_col_structure = {
        "id": ('NO', sql_types["PostgreSQL"]["int"], None),
        "point_hash": ('NO', sql_types["PostgreSQL"]["string"], None),
        "flag": ('NO', sql_types["PostgreSQL"]["string"], None),
        "value": ('NO', sql_types["PostgreSQL"]["string"], None)
    }
    expected_indices = [
        "dim_flag_pkey",
        "fact_value_point_hash_flag_key",
    ]
    expected_foreign_keys = {
        "fact_flag_point_hash_fkey": ("fact_flag", "point_hash", "fact_measurement", "point_hash")
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
@pytest.mark.postgres
@pytest.mark.base_v1
def test_dim_device_good(postgres_connection):
    """Test whether `dim_device` accepts correctly formatted data.

    Tests
    -----
    - Data uploaded successfully
    """
    tests: dict[str, bool] = {}
    good_data = [
        {
            "key": "ANT_123456",
            "dataset": "test",
            "name": "Antwerp 1",
            "short_name": "A1",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": "ANT_123567",
            "dataset": "test",
            "name": "Antwerp 2",
            "short_name": "A2",
            "reference": False,
            "other": None
        },
        {
            "key": "ANT_131245",
            "dataset": "test",
            "name": "Antwerp 3",
            "short_name": "A3",
            "reference": False,
            "other": None
        },
        {
            "key": "ANT_R000",
            "dataset": "test",
            "name": "Antwerp Ref 1",
            "short_name": "AR1",
            "reference": True,
            "other": {"key": "value"}
        },
        {
            "key": "ANT_R001",
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
    with postgres_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "dupe_data", [
            {
                "key": "ANT_123456",
                "name": "Antwerp 1",
                "short_name": "A1",
                "dataset": "test",
                "reference": False,
                "other": None
            },
            {
                "key": "ANT_123456",
                "name": "Antwerp 4",
                "short_name": "A4",
                "dataset": "test",
                "reference": False,
                "other": None
            },
            {
                "key": "ANT_123457",
                "name": "Antwerp 1",
                "short_name": "A4",
                "dataset": "test",
                "reference": False,
                "other": None
            },
            {
                "key": "ANT_123457",
                "name": "Antwerp 4",
                "short_name": "A1",
                "dataset": "test",
                "reference": False,
                "other": None
            },
    ]
)
def test_dim_device_dupe(postgres_connection, dupe_data):
    """Test whether `dim_device` rejects duped data in unique columns.

    Tests
    -----
    - Unique constraint raises error when duplicate value added
    """
    insert_statement = insert(orm.DimDevice)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates unique constraint"
        ):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "col_to_null", ["key", "name", "short_name"]
)
def test_dim_device_null(postgres_connection, col_to_null):
    """Test whether `dim_device` rejects null values from specific columns.

    Tests
    -----
    - Not null constraint raises error when null value added
    """
    raw_data: dict[str, str | dt.datetime | int | float | None] = {
        "key": "ANT_123457",
        "name": "Antwerp 4",
        "short_name": "A4"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimDevice)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_dim_header_good(postgres_connection):
    """Test whether `dim_header` accepts correctly formatted data.

    Tests
    -----
    - Data uploaded successfully
    """
    tests: dict[str, bool] = {}
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
    with postgres_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_dim_header_dupe(postgres_connection):
    """Test whether `dim_header` rejects duped data in unique columns.

    Tests
    -----
    - Unique constraint raises error when duplicate value added
    """
    dupe_data = {
        "header": "ox_test",
        "parameter": "ox",
        "unit": "nA",
        "other": None
    }
    insert_statement = insert(orm.DimHeader)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates unique constraint"
        ):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "col_to_null", [
        "header",
        "parameter",
        "unit"
    ]
)
def test_dim_header_null(postgres_connection, col_to_null):
    """Test whether `dim_header` rejects null values from specific columns.

    Tests
    -----
    - Not null constraint raises error when null value added
    """
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
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_dim_unit_conversion_good(postgres_connection):
    """Test whether `dim_unit_conversion` accepts correctly formatted data.

    Tests
    -----
    - Data uploaded successfully
    """
    tests = {}
    good_data = [
        {
            "unit_in": "ppm",
            "unit_out": "ppb",
            "parameter": "Test",
            "scale": 0.77
        },
        {
            "unit_in": "ppm",
            "unit_out": "ppb",
            "parameter": "Test2",
            "scale": 0.66
        },
        {
            "unit_in": "ppb",
            "unit_out": "ppm",
            "parameter": "Test",
            "scale": 0.55
        },
    ]
    expected_pks = ((None,), (None,), (None,))

    insert_statement = insert(orm.DimUnitConversion)
    with postgres_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_dim_unit_conversion_dupe(postgres_connection):
    """Test whether `dim_unit_conversion` rejects duped data in unique columns.

    Tests
    -----
    - Unique constraint raises error when duplicate value added
    """
    dupe_data = {
        "unit_in": "ppb",
        "unit_out": "ppm",
        "parameter": "Test",
        "scale": 0.44
    }
    insert_statement = insert(orm.DimUnitConversion)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates unique constraint"
        ):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "col_to_null", [
        "unit_in",
        "unit_out",
        "parameter",
        "scale"
    ]
)
def test_dim_unit_conversion_null(postgres_connection, col_to_null):
    """Test whether `dim_unit_conversion` rejects null values from specific columns.

    Tests
    -----
    - Not null constraint raises error when null value added
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
        "unit_in": "ppt",
        "unit_out": "ppm",
        "parameter": "Test3",
        "scale": 0.87
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimUnitConversion)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_fact_measurement_good(postgres_connection):
    """Test whether `fact_measurement` accepts correctly formatted data.

    Tests
    -----
    - Data uploaded successfully
    """
    tests: dict[str, bool] = {}
    good_data = [
        {
            "point_hash": "test1",
            "timestamp": dt.datetime(2020, 1, 1),
            "device_key": "ANT_123456",
        },
        {
            "point_hash": "test2",
            "timestamp": dt.datetime(2020, 1, 2),
            "device_key": "ANT_131245",
        },
        {
            "point_hash": "test3",
            "timestamp": dt.datetime(2020, 1, 3),
            "device_key": "ANT_123567",
        },
    ]
    expected_pks = (('test1',), ('test2',), ('test3',))

    insert_statement = insert(orm.FactMeasurement)
    with postgres_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_fact_measurement_dupe(postgres_connection):
    """Test whether `fact_measurement` rejects duped data in unique columns.

    Tests
    -----
    - Unique constraint raises error when duplicate value added
    """
    dupe_data = {
        "point_hash": "test3",
        "timestamp": dt.datetime(2020, 1, 1),
        "device_key": "ANT_123456",
    }
    insert_statement = insert(orm.FactMeasurement)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates unique constraint"
        ):
            _ = conn.execute(
                insert_statement,
                dupe_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "bad_key", [
        "device_key",
    ]
)
def test_fact_measurement_bad_foreign_key(postgres_connection, bad_key):
    """Test whether `fact_measurement` rejects invalid foreign keys.

    Tests
    -----
    - Fkey constraint raises error when bad key added.
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
            "point_hash": "test4",
            "timestamp": dt.datetime(2020, 1, 3),
            "device_key": "ANT_123567"
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.FactMeasurement)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates foreign key constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "col_to_null", [
        "point_hash",
        "timestamp",
        "device_key"
    ]
)
def test_fact_measurement_null(postgres_connection, col_to_null):
    """Test whether `fact_measurement` rejects null values from specific columns.

    Tests
    -----
    - Not null constraint raises error when null value added
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
            "point_hash": "test5",
            "timestamp": dt.datetime(2020, 1, 1),
            "device_key": "ANT_123456"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.FactMeasurement)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_fact_value_good(postgres_connection):
    """Test whether `fact_value` accepts correctly formatted data.

    Tests
    -----
    - Data uploaded successfully
    """
    tests: dict[str, bool] = {}
    good_data = [
        {
            "point_hash": "test1",
            "header": "ox_test",
            "value": 0.1
        },
        {
            "point_hash": "test2",
            "header": "no_test",
            "value": 0.2
        },
        {
            "point_hash": "test3",
            "header": "opc_test",
            "value": 0.3
        },
    ]
    expected_pks = ((None,), (None,), (None,))

    insert_statement = insert(orm.FactValue)
    with postgres_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "bad_key", [
        "point_hash",
        "header"
    ]
)
def test_fact_value_bad_foreign_key(postgres_connection, bad_key):
    """Test whether `fact_value` rejects invalid foreign keys.

    Tests
    -----
    - Fkey constraint raises error when bad key added.
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
        "point_hash": "test4",
        "header": "ox_test",
        "value": 0.1
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.FactValue)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates foreign key constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "col_to_null", [
        "point_hash",
        "header",
        "value"
    ]
)
def test_fact_value_null(postgres_connection, col_to_null):
    """Test whether `fact_value` rejects null values from specific columns.

    Tests
    -----
    - Not null constraint raises error when null value added
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
        "point_hash": "test5",
        "header": "ox_test",
        "value": 0.1
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.FactValue)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_fact_flag_good(postgres_connection):
    """Test whether `fact_flag` accepts correctly formatted data.

    Tests
    -----
    - Data uploaded successfully
    """
    tests: dict[str, bool] = {}
    good_data = [
        {
            "point_hash": "test1",
            "flag": "ox_test",
            "value": "a"
        },
        {
            "point_hash": "test2",
            "flag": "no_test",
            "value": "b"
        },
        {
            "point_hash": "test3",
            "flag": "opc_test",
            "value": "c"
        },
    ]
    expected_pks = ((None,), (None,), (None,))

    insert_statement = insert(orm.FactFlag)
    with postgres_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "bad_key", [
        "point_hash"
    ]
)
def test_fact_flag_bad_foreign_key(postgres_connection, bad_key):
    """Test whether `fact_flag` rejects invalid foreign keys.

    Tests
    -----
    - Fkey constraint raises error when bad key added.
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
        "point_hash": "test4",
        "flag": "ox_test",
        "value": "a"
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.FactFlag)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates foreign key constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "col_to_null", [
        "point_hash",
        "flag",
        "value"
    ]
)
def test_fact_flag_null(postgres_connection, col_to_null):
    """Test whether `fact_flag` rejects null values from specific columns.

    Tests
    -----
    - Not null constraint raises error when null value added
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
        "point_hash": "test5",
        "flag": "ox_test",
        "value": "a"
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.FactFlag)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
def test_dim_colocation_good(postgres_connection):
    """Test whether `dim_colocation` accepts correctly formatted data.

    Tests
    -----
    - Data uploaded successfully
    """
    tests: dict[str, bool] = {}
    good_data = [
        {
            "device_key": "ANT_123456",
            "other_key": "ANT_R000",
            "start_date": dt.datetime(2020, 1, 1),
            "end_date": dt.datetime(2020, 3, 1)
        },
        {
            "device_key": "ANT_123456",
            "other_key": "ANT_R001",
            "start_date": dt.datetime(2020, 2, 1),
            "end_date": dt.datetime(2020, 4, 1)
        },
        {
            "device_key": "ANT_131245",
            "other_key": "ANT_R000",
            "start_date": dt.datetime(2020, 7, 1),
            "end_date": dt.datetime(2020, 8, 1)
        },
    ]
    expected_pks = ((None,), (None,), (None,))

    insert_statement = insert(orm.DimColocation)
    with postgres_connection.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks
    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "bad_key", [
        "device_key",
        "other_key"
    ]
)
def test_dim_colocation_bad_foreign_key(postgres_connection, bad_key):
    """Test whether `dim_colocation` rejects invalid foreign keys.

    Tests
    -----
    - Fkey constraint raises error when bad key added.
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
            "device_key": "ANT_131245",
            "other_key": "ANT_R000",
            "start_date": dt.datetime(2020, 7, 1),
            "end_date": dt.datetime(2020, 8, 1)
    }
    raw_data[bad_key] = "BADKEY"

    insert_statement = insert(orm.DimColocation)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates foreign key constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )


@pytest.mark.orm
@pytest.mark.postgres
@pytest.mark.base_v1
@pytest.mark.parametrize(
    "col_to_null", [
        "device_key",
        "other_key",
        "start_date",
        "end_date"
    ]
)
def test_dim_colocation_null(postgres_connection, col_to_null):
    """Test whether `dim_colocation` rejects null values from specific columns.

    Tests
    -----
    - Not null constraint raises error when null value added
    """
    raw_data: dict[
        str,
        str | dt.datetime | int | float | dict[str, str] | None
    ] = {
            "device_key": "ANT_131245",
            "other_key": "ANT_R000",
            "start_date": dt.datetime(2020, 7, 1),
            "end_date": dt.datetime(2020, 8, 1)
    }
    raw_data[col_to_null] = None

    insert_statement = insert(orm.DimColocation)
    with postgres_connection.connect() as conn:
        with pytest.raises(
            sqlexc.IntegrityError,
            match=r"violates not-null constraint"
        ):
            _ = conn.execute(
                insert_statement,
                raw_data
            )

