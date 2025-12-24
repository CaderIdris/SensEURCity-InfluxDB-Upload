"""Tests the orm.py module with a PostgreSQL db.

Tests
-----
- Are tables created?
    - Tests whether all tables are created in db
- Are table schemas valid?
    - `dim_device`
    - `dim_header`
    - `dim_unit_conversion`
    - `fact_measurement`
    - `dim_colocation`
    - `meta_files_processed`
- `dim_device`
    - Does it accept valid entries?
    - Is duplicate data rejected?
    - Are null values rejected?
- `dim_header`
    - Does it accept valid entries?
    - Is duplicate data rejected?
    - Are null values rejected?
- `dim_unit_conversion`
    - Does it accept valid entries?
    - Is duplicate data rejected?
    - Are null values rejected?
- `fact_measurement`
    - Does it accept valid measurements?
    - Is duplicate data rejected?
    - Are null values rejected?
    - Are bad foreign keys rejected?
- `dim_colocation`
    - Does it accept valid periods?
    - Are null values rejected?
    - Are bad foreign keys rejected?
"""
import datetime as dt
from pathlib import Path

import pytest
from sqlalchemy import insert, inspect
from sqlalchemy.engine.base import Engine
import sqlalchemy.exc as sqlexc

from senseurcity import orm
from senseurcity import engine

from conftest import DBs


@pytest.fixture(scope="session")
def violation_messages():
    return {
        DBs.SQLite: {
            "Unique": "UNIQUE constraint failed: ",
            "Null": "NOT NULL constraint failed",
            "Foreign": "FOREIGN KEY constraint failed"
        },
        DBs.DuckDB: {
            "Unique": (
                "violates primary key constraint|"
                "violates unique constraint|"
                "Constraint Error: PRIMARY KEY or UNIQUE constraint"
            ),
            "Null": "NOT NULL constraint failed",
            "Foreign": "Violates foreign key constraint"
        },
        DBs.PostgreSQL: {
            "Unique": "violates unique constraint",
            "Null": "violates not-null constraint",
            "Foreign": "violates foreign key constraint"
        }
    }

@pytest.mark.orm
def test_alt_schema(db_path: Path) -> None:
    tests: dict[str, bool] = {}
    db_path_duckdb = db_path / "duckdb-test.db"
    db_engine = engine.get_engine(
        f"duckdb:///{db_path_duckdb}",
        schema_name="alt_schema"
    )
    orm.create_tables(db_engine)
    inspector = inspect(db_engine)
    tables = inspector.get_table_names(schema="alt_schema")

    expected_tables = (
        "dim_device",
        "dim_header",
        "dim_unit_conversion",
        "fact_measurement",
        "dim_colocation",
        "meta_files_processed",
        "bridge_device_header"
    )

    for table in expected_tables:
        tests[f"{table} is in db"] = (table in tables)

    tests[f"{len(expected_tables)} tables in db"] = (
        len(expected_tables) == len(tables)
    )

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())

@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_create_tables(db: DBs, connections: dict[DBs, Engine | None]) -> None:
    """Test whether the tables were created.

    Tests
    -----
    - Each expected table is in db.
    - Correct number of tables.
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()
    tests: dict[str, bool] = {}

    inspector = inspect(db_engine)
    has_measurement_schema = any(
        "measurement" in i for i in inspector.get_schema_names()
    )
    tables = inspector.get_table_names(
        schema="measurement" if has_measurement_schema else "main"
    )

    expected_tables = (
        "dim_device",
        "dim_header",
        "dim_unit_conversion",
        "fact_measurement",
        "dim_colocation",
        "meta_files_processed",
        "bridge_device_header"
    )

    for table in expected_tables:
        tests[f"{table} is in db"] = (table in tables)

    tests[f"{len(expected_tables)} tables in db"] = (
        len(expected_tables) == len(tables)
    )

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_dim_device(
    db: DBs,
    connections: dict[DBs, Engine | None],
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    tests: dict[str, bool] = {}
    good_data = [
        {
            "key": "ANT_123456_TEST",
            "dataset": "test_TEST",
            "name": "Antwerp 1_TEST",
            "short_name": "A1_TEST",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": "ANT_123567_TEST",
            "dataset": "test_TEST",
            "name": "Antwerp 2_TEST",
            "short_name": "A2_TEST",
            "reference": False,
            "other": None
        },
        {
            "key": "ANT_131245_TEST",
            "dataset": "test_TEST",
            "name": "Antwerp 3_TEST",
            "short_name": "A3_TEST",
            "reference": False,
            "other": None
        },
        {
            "key": "ANT_R000_TEST",
            "dataset": "test_TEST",
            "name": "Antwerp Ref 1_TEST",
            "short_name": "AR1_TEST",
            "reference": True,
            "other": {"key": "value"}
        },
        {
            "key": "ANT_R001_TEST",
            "dataset": "test_TEST",
            "name": "Antwerp Ref 2_TEST",
            "short_name": "AR2_TEST",
            "reference": True,
            "other": None
        }
    ]
    expected_pks = (
        ("ANT_123456_TEST",),
        ("ANT_123567_TEST",),
        ("ANT_131245_TEST",),
        ("ANT_R000_TEST",),
        ("ANT_R001_TEST",)
    )
    insert_statement = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())

@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "dupe_key",
    [
        "key",
        "name",
        "short_name"
    ]
)
def test_dim_device_dupe(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]],
    dupe_key: str
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()


    initial_value = {
        "key": f"ANT_123461{dupe_key}_TEST",
        "name": f"Antwerp 6{dupe_key}_TEST",
        "short_name": f"A6{dupe_key}_TEST",
        "dataset": "test",
        "reference": False,
        "other": {"key": "value"}
    }
    bad_example = {
        "key": "ANT_123460_TEST",
        "name": "Antwerp 5_TEST",
        "short_name": "A5_TEST",
        "dataset": "test",
        "reference": False,
        "other": {"key": "value"}
    }

    # Duplicated data
    insert_statement = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            insert_statement,
            [initial_value]
        )
        conn.commit()
    with db_engine.connect() as conn:
        duped = bad_example.copy()
        duped[dupe_key] = initial_value[dupe_key]
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Unique"]
        ):
            _ = conn.execute(
                insert_statement,
                [duped]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "null_key",
    [
        "key",
        "name",
        "short_name",
        "dataset",
        "reference",
    ]
)
def test_dim_device_null(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]],
    null_key: str
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()
    bad_example = {
        "key": "ANT_123460_TEST",
        "name": "Antwerp 5_TEST",
        "short_name": "A5_TEST",
        "dataset": "test_TEST",
        "reference": False,
        "other": {"key": "value"}
    }

    # Null data
    insert_statement = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        nulled = bad_example.copy()
        nulled.pop(null_key)
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Null"]
        ):
            _ = conn.execute(
                insert_statement,
                [nulled]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_dim_header_good(
    db: DBs,
    connections: dict[DBs, Engine | None]
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    tests: dict[str, bool] = {}
    good_data: list[dict[str, str | None | dict[str, str]]] = [
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
    expected_pks = (
        ("ox_test",),
        ("no_test",),
        ("opc_test",)
    )
    insert_statement = insert(orm.DimHeader)
    with db_engine.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "dupe_key",
    [
        "header"
    ]
)
def test_dim_header_dupe(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]],
    dupe_key: str
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()
    initial_value = {
        "header": "no2_test",
        "parameter": "no2",
        "unit": "nA",
        "other": None
    }
    bad_example = {
        "header": "nox_test",
        "parameter": "nox",
        "unit": "nA",
        "other": None
    }
    # Duplicated data
    insert_statement = insert(orm.DimHeader)
    with db_engine.connect() as conn:
        _ = conn.execute(
            insert_statement,
            [initial_value]
        )
        conn.commit()
    with db_engine.connect() as conn:
        duped = bad_example.copy()
        duped[dupe_key] = initial_value[dupe_key]
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Unique"]
        ):
            _ = conn.execute(
                insert_statement,
                [duped]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "null_key",
    [
        "header",
        "parameter",
        "unit"
    ]
)
def test_dim_header_null(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]],
    null_key: str
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()
    bad_example = {
        "header": "nox_test",
        "parameter": "nox",
        "unit": "nA",
        "other": None
    }
    # Null data
    insert_statement = insert(orm.DimHeader)
    with db_engine.connect() as conn:
        nulled = bad_example.copy()
        nulled.pop(null_key)
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Null"]
        ):
            _ = conn.execute(
                insert_statement,
                [nulled]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_bridge_device_header(
    db: DBs,
    connections: dict[DBs, Engine | None],
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    tests: dict[str, bool] = {}
    ddevice_prep = [
        {
            "key": "ANT_123456_TEST_BDH",
            "dataset": "test_TEST_BDH",
            "name": "Antwerp 1_TEST_BDH",
            "short_name": "A1_TEST_BDH",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": "ANT_123567_TEST_BDH",
            "dataset": "test_TEST_BDH",
            "name": "Antwerp 2_TEST_BDH",
            "short_name": "A2_TEST_BDH",
            "reference": False,
            "other": None
        },
    ]
    ddev_insert = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            ddev_insert,
            ddevice_prep
        )
        conn.commit()

    dheader_prep: list[dict[str, str | None | dict[str, str]]] = [
        {
            "header": "ox_test_BDH",
            "parameter": "ox",
            "unit": "nA",
            "other": None
        },
        {
            "header": "no_test_BDH",
            "parameter": "no",
            "unit": "nA",
            "other": {"key": "value"}
        }
    ]
    dhead_insert = insert(orm.DimHeader)
    with db_engine.connect() as conn:
        _ = conn.execute(
            dhead_insert,
            dheader_prep
        )
        conn.commit()

    good_data: list[dict[str, str | None]] = [
        {
            "device_key": "ANT_123456_TEST_BDH",
            "header": "ox_test_BDH",
            "flag": "ox_test_BDH_flag"
        },
        {
            "device_key": "ANT_123456_TEST_BDH",
            "header": "no_test_BDH",
            "flag": None
        },
        {
            "device_key": "ANT_123567_TEST_BDH",
            "header": "ox_test_BDH",
            "flag": None
        }
    ]
    expected_pks = (
        ("ANT_123456_TEST_BDH", "ox_test_BDH"),
        ("ANT_123456_TEST_BDH", "no_test_BDH"),
        ("ANT_123567_TEST_BDH", "ox_test_BDH")
    )
    insert_statement = insert(orm.BridgeDeviceHeader)
    with db_engine.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        conn.commit()
        pks = tuple(result.inserted_primary_key_rows)

    tests["Rows inserted"] = pks == expected_pks

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_bridge_device_header_dupe(
    db: DBs,
    violation_messages: dict[DBs, dict[str, str]],
    connections: dict[DBs, Engine | None]
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    ddevice_prep = [
        {
            "key": "bdh_dupe_test",
            "dataset": "test_TEST_BDH",
            "name": "bdh_dupe_test",
            "short_name": "bdh_dupe_test",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    ddev_insert = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            ddev_insert,
            ddevice_prep
        )
        conn.commit()

    dheader_prep: list[dict[str, str | None | dict[str, str]]] = [
        {
            "header": "bdh_dupe_test",
            "parameter": "ox",
            "unit": "nA",
            "other": None
        },
    ]
    dhead_insert = insert(orm.DimHeader)
    with db_engine.connect() as conn:
        _ = conn.execute(
            dhead_insert,
            dheader_prep
        )
        conn.commit()

    ins_data = [
        {
            "device_key": "bdh_dupe_test",
            "header": "bdh_dupe_test",
            "flag": "ox_test_BDH_flag"
        },
        {
            "device_key": "bdh_dupe_test",
            "header": "bdh_dupe_test",
            "flag": "ox_test_BDH_flag"
        },
    ]
    insert_statement = insert(orm.BridgeDeviceHeader)
    with db_engine.connect() as conn, pytest.raises(
        sqlexc.IntegrityError,
        match=violation_messages[db]["Unique"]
    ):
        _ = conn.execute(
            insert_statement,
            ins_data
        )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize("col_to_null", ["device_key", "header"])
def test_bridge_device_header_null(
    db: DBs,
    violation_messages: dict[DBs, dict[str, str]],
    col_to_null: str,
    connections: dict[DBs, Engine | None],
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    ddevice_prep = [
        {
            "key": f"bdh_null_test_{col_to_null}",
            "dataset": f"test_TEST_BDH_{col_to_null}",
            "name": f"bdh_null_test_{col_to_null}",
            "short_name": f"bdh_null_test_{col_to_null}",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    ddev_insert = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            ddev_insert,
            ddevice_prep
        )
        conn.commit()

    dheader_prep: list[dict[str, str | None | dict[str, str]]] = [
        {
            "header": f"bdh_null_test_{col_to_null}",
            "parameter": "ox",
            "unit": "nA",
            "other": None
        },
    ]
    dhead_insert = insert(orm.DimHeader)
    with db_engine.connect() as conn:
        _ = conn.execute(
            dhead_insert,
            dheader_prep
        )
        conn.commit()

    ins_data = {
        "device_key": f"bdh_null_test_{col_to_null}",
        "header": f"bdh_null_test_{col_to_null}",
        "flag": None
    }
    ins_data[col_to_null] = None
    insert_statement = insert(orm.BridgeDeviceHeader)
    with db_engine.connect() as conn, pytest.raises(
        sqlexc.IntegrityError,
        match=violation_messages[db]["Null"]
    ):
        _ = conn.execute(
            insert_statement,
            ins_data
        )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize("col_to_change", ["device_key", "header"])
def test_bridge_device_header_fkey(
    db: DBs,
    violation_messages: dict[DBs, dict[str, str]],
    col_to_change: str,
    connections: dict[DBs, Engine | None],
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    ddevice_prep = [
        {
            "key": f"bdh_fkey_test_{col_to_change}",
            "dataset": f"test_TEST_BDH_{col_to_change}",
            "name": f"bdh_fkey_test_{col_to_change}",
            "short_name": f"bdh_fkey_test_{col_to_change}",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    ddev_insert = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            ddev_insert,
            ddevice_prep
        )
        conn.commit()

    dheader_prep: list[dict[str, str | None | dict[str, str]]] = [
        {
            "header": f"bdh_fkey_test_{col_to_change}",
            "parameter": "ox",
            "unit": "nA",
            "other": None
        },
    ]
    dhead_insert = insert(orm.DimHeader)
    with db_engine.connect() as conn:
        _ = conn.execute(
            dhead_insert,
            dheader_prep
        )
        conn.commit()

    ins_data = {
        "device_key": f"bdh_fkey_test_{col_to_change}",
        "header": f"bdh_fkey_test_{col_to_change}",
        "flag": None
    }
    ins_data[col_to_change] = "BAD KEY"
    insert_statement = insert(orm.BridgeDeviceHeader)
    with db_engine.connect() as conn, pytest.raises(
        sqlexc.IntegrityError,
        match=violation_messages[db]["Foreign"]
    ):
        _ = conn.execute(
            insert_statement,
            ins_data
        )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_dim_unit_conversion(
    db: DBs,
    connections: dict[DBs, Engine | None],
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    tests: dict[str, bool] = {}
    good_data = [
        {
            "unit_in": "mg",
            "unit_out": "ng",
            "parameter": "test1",
            "scale": 1000
        },
        {
            "unit_in": "ng",
            "unit_out": "mg",
            "parameter": "test1",
            "scale": 0.001
        },
        {
            "unit_in": "g",
            "unit_out": "kg",
            "parameter": "test2",
            "scale": 0.001
        },
    ]

    expected_pks = (
        ("mg", "ng", "test1"),
        ("ng", "mg", "test1"),
        ("g", "kg", "test2"),
    )
    insert_statement = insert(orm.DimUnitConversion)
    with db_engine.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_dim_unit_conversion_dupe(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]]
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    initial_value = {
        "unit_in": "kg",
        "unit_out": "g",
        "parameter": "test2",
        "scale": 1000
    }
    bad_example = {
        "unit_in": "A",
        "unit_out": "B",
        "parameter": "test3",
        "scale": 2
    }

    # Duplicated data
    insert_statement = insert(orm.DimUnitConversion)
    with db_engine.connect() as conn:
        _ = conn.execute(
            insert_statement,
            [initial_value]
        )
        conn.commit()
    with db_engine.connect() as conn:
        duped = bad_example.copy()
        duped["unit_in"] = "g"
        duped["unit_out"] = "kg"
        duped["parameter"] = "test2"
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Unique"]
        ):
            _ = conn.execute(
                insert_statement,
                [duped]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "null_key",
    [
        "unit_in",
        "unit_out",
        "parameter",
        "scale"
    ]
)
def test_dim_unit_conversion_null(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]],
    null_key: str
) -> None:
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    bad_example = {
        "unit_in": "A",
        "unit_out": "B",
        "parameter": "test3",
        "scale": 2
    }

    # Null data
    insert_statement = insert(orm.DimUnitConversion)
    with db_engine.connect() as conn:
        nulled = bad_example.copy()
        nulled.pop(null_key)
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Null"]
        ):
            _ = conn.execute(
                insert_statement,
                [nulled]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_dim_colocation(
    db: DBs,
    connections: dict[DBs, Engine | None],
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    fkey_data = [
        {
            "key": "DIMCOLOC1",
            "dataset": "test",
            "name": "DCOL 1",
            "short_name": "D1",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": "DIMCOLOC2",
            "dataset": "test",
            "name": "DCOL 2",
            "short_name": "D2",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": "DIMCOLOC3",
            "dataset": "test",
            "name": "DCOL 3",
            "short_name": "D3",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": "DIMCOLOC4",
            "dataset": "test",
            "name": "DCOL 4",
            "short_name": "D4",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    fkeys = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            fkeys,
            fkey_data
        )
        conn.commit()

    tests: dict[str, bool] = {}
    good_data = [
        {
            "device_key": "DIMCOLOC1",
            "other_key": "DIMCOLOC2",
            "start_date": dt.datetime(2020, 1, 1),
            "end_date": dt.datetime(2020, 12, 1)
        },
        {
            "device_key": "DIMCOLOC3",
            "other_key": "DIMCOLOC4",
            "start_date": dt.datetime(2020, 1, 1),
            "end_date": dt.datetime(2020, 12, 1)
        },
    ]

    expected_pks = (
        ("DIMCOLOC1", "DIMCOLOC2", dt.datetime(2020, 1, 1), dt.datetime(2020, 12, 1)),
        ("DIMCOLOC3", "DIMCOLOC4", dt.datetime(2020, 1, 1), dt.datetime(2020, 12, 1)),
    )
    insert_statement = insert(orm.DimColocation)
    with db_engine.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_dim_colocation_dupe(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    fkey_data = [
        {
            "key": "DIMCOLOC5",
            "dataset": "test",
            "name": "DCOL 5",
            "short_name": "D5",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": "DIMCOLOC6",
            "dataset": "test",
            "name": "DCOL 6",
            "short_name": "D6",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    fkeys = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            fkeys,
            fkey_data
        )
        conn.commit()

    example = {
        "device_key": "DIMCOLOC5",
        "other_key": "DIMCOLOC6",
        "start_date": dt.datetime(2020, 1, 1),
        "end_date": dt.datetime(2020, 12, 1)
    }

    # Duplicated data
    insert_statement = insert(orm.DimColocation)
    with db_engine.connect() as conn:
        _ = conn.execute(
            insert_statement,
            [example]
        )
        conn.commit()
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Unique"]
        ):
            _ = conn.execute(
                insert_statement,
                [example]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "null_key",
    [
        "device_key",
        "other_key",
        "start_date",
        "end_date"
    ]
)
def test_dim_colocation_null(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]],
    null_key: str
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    fkey_data = [
        {
            "key": f"DIMCOLOC7{null_key}",
            "dataset": "test",
            "name": f"DCOL 7{null_key}",
            "short_name": f"D7{null_key}",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": f"DIMCOLOC8{null_key}",
            "dataset": "test",
            "name": f"DCOL 8{null_key}",
            "short_name": f"D8{null_key}",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    fkeys = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            fkeys,
            fkey_data
        )
        conn.commit()

    bad_example = {
        "device_key": f"DIMCOLOC7{null_key}",
        "other_key": f"DIMCOLOC8{null_key}",
        "start_date": dt.datetime(2020, 1, 1),
        "end_date": dt.datetime(2020, 12, 1)
    }

    insert_statement = insert(orm.DimColocation)
    with db_engine.connect() as conn:
        nulled = bad_example.copy()
        nulled.pop(null_key)
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Null"]
        ):
            _ = conn.execute(
                insert_statement,
                [nulled]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "bad_fkey",
    [
        "device_key",
        "other_key"
    ]
)
def test_dim_colocation_bad_fkey(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages,
    bad_fkey
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    fkey_data = [
        {
            "key": f"DIMCOLOC9{bad_fkey}",
            "dataset": "test",
            "name": f"DCOL 9{bad_fkey}",
            "short_name": f"D9{bad_fkey}",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": f"DIMCOLOC0{bad_fkey}",
            "dataset": "test",
            "name": f"DCOL 0{bad_fkey}",
            "short_name": f"D0{bad_fkey}",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    fkeys = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            fkeys,
            fkey_data
        )
        conn.commit()
    bad_example = {
        "device_key": f"DIMCOLOC9{bad_fkey}",
        "other_key": f"DIMCOLOC0{bad_fkey}",
        "start_date": dt.datetime(2020, 1, 1),
        "end_date": dt.datetime(2020, 12, 1)
    }

    # Null data
    insert_statement = insert(orm.DimColocation)
    with db_engine.connect() as conn:
        bad_value = bad_example.copy()
        bad_value[bad_fkey] = "INVALID"
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Foreign"]
        ):
            _ = conn.execute(
                insert_statement,
                [bad_value]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_fact_measurement(
    db: DBs,
    connections: dict[DBs, Engine | None],
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    fkey_data = [
        {
            "key": "FACTMEAS1",
            "dataset": "test",
            "name": "FMEAS 1",
            "short_name": "F1",
            "reference": False,
            "other": {"key": "value"}
        },
        {
            "key": "FACTMEAS2",
            "dataset": "test",
            "name": "FMEAS 2",
            "short_name": "F2",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    fkeys = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            fkeys,
            fkey_data
        )
        conn.commit()

    tests: dict[str, bool] = {}
    good_data = [
        {
            "time": dt.datetime(2020, 1, 1),
            "device_key": "FACTMEAS1",
            "measurements": {"A": 1, "B": 2},
            "flags": {"A_flag": "Valid", "B_flag": "w"},
            "meta": None
        },
        {
            "time": dt.datetime(2020, 1, 2),
            "device_key": "FACTMEAS2",
            "measurements": {"A": 1, "B": 2},
            "flags": None,
            "meta": {"A_meta": 1, "B_meta": 2}
        },
    ]

    expected_pks = (
        (dt.datetime(2020, 1, 1), "FACTMEAS1"),
        (dt.datetime(2020, 1, 2), "FACTMEAS2"),
    )
    insert_statement = insert(orm.FactMeasurement)
    with db_engine.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_fact_measurement_dupe(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    fkey_data = [
        {
            "key": "FACTMEAS3",
            "dataset": "test",
            "name": "FMEAS 3",
            "short_name": "F3",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    fkeys = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            fkeys,
            fkey_data
        )
        conn.commit()

    example = {
        "time": dt.datetime(2020, 1, 3),
        "device_key": "FACTMEAS3",
        "measurements": {"A": 1, "B": 2},
        "flags": {"A_flag": "Valid", "B_flag": "w"}
    }

    # Duplicated data
    insert_statement = insert(orm.FactMeasurement)
    with db_engine.connect() as conn:
        _ = conn.execute(
            insert_statement,
            [example]
        )
        conn.commit()
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Unique"]
        ):
            _ = conn.execute(
                insert_statement,
                [example]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "null_key",
    [
        "time",
        "device_key",
        "measurements"
    ]
)
def test_fact_measurement_null(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]],
    null_key: str
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    fkey_data = [
        {
            "key": f"FACTMEAS4{null_key}",
            "dataset": "test",
            "name": f"FMEAS 4{null_key}",
            "short_name": f"F4{null_key}",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    fkeys = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            fkeys,
            fkey_data
        )
        conn.commit()

    bad_example = {
        "time": dt.datetime(2020, 1, 3),
        "device_key": f"FACTMEAS3{null_key}",
        "measurements": {"A": 1, "B": 2},
        "flags": {"A_flag": "Valid", "B_flag": "w"}
    }

    insert_statement = insert(orm.FactMeasurement)
    with db_engine.connect() as conn:
        nulled = bad_example.copy()
        nulled.pop(null_key)
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Null"]
        ):
            _ = conn.execute(
                insert_statement,
                [nulled]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "bad_fkey",
    [
        "device_key",
    ]
)
def test_fact_measurement_bad_fkey(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages,
    bad_fkey
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    fkey_data = [
        {
            "key": f"FACTMEAS5{bad_fkey}",
            "dataset": "test",
            "name": f"FMEAS 5{bad_fkey}",
            "short_name": f"F5{bad_fkey}",
            "reference": False,
            "other": {"key": "value"}
        },
    ]
    fkeys = insert(orm.DimDevice)
    with db_engine.connect() as conn:
        _ = conn.execute(
            fkeys,
            fkey_data
        )
        conn.commit()
    bad_example = {
        "time": dt.datetime(2020, 1, 3),
        "device_key": f"FACTMEAS5{bad_fkey}",
        "measurements": {"A": 1, "B": 2},
        "flags": {"A_flag": "Valid", "B_flag": "w"}
    }

    # Null data
    insert_statement = insert(orm.FactMeasurement)
    with db_engine.connect() as conn:
        bad_value = bad_example.copy()
        bad_value[bad_fkey] = "INVALID"
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Foreign"]
        ):
            _ = conn.execute(
                insert_statement,
                [bad_value]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_meta_files_processed(
    db: DBs,
    connections: dict[DBs, Engine | None],
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    tests: dict[str, bool] = {}
    good_data = [
        {
            "filename": "Test1",
            "timestamp": dt.datetime(2020, 1, 1)
        },
        {
            "filename": "Test2",
            "timestamp": dt.datetime(2020, 1, 2)
        },
    ]

    expected_pks = (
        ("Test1",),
        ("Test2",)
    )
    insert_statement = insert(orm.MetaFilesProcessed)
    with db_engine.connect() as conn:
        result = conn.execute(
            insert_statement,
            good_data
        )
        pks = tuple(result.inserted_primary_key_rows)
        conn.commit()
    tests["Rows inserted"] = pks == expected_pks

    for outcome in tests.values():
        if not outcome:
            pass

    assert all(tests.values())


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
def test_meta_files_processed_dupe(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    example = {
        "filename": "Test3",
        "timestamp": dt.datetime(2020, 1, 3)
    }

    # Duplicated data
    insert_statement = insert(orm.MetaFilesProcessed)
    with db_engine.connect() as conn:
        _ = conn.execute(
            insert_statement,
            [example]
        )
        conn.commit()
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Unique"]
        ):
            _ = conn.execute(
                insert_statement,
                [example]
            )


@pytest.mark.orm
@pytest.mark.base_v1
@pytest.mark.parametrize("db", list(DBs))
@pytest.mark.parametrize(
    "null_key",
    [
        "filename",
        "timestamp"
    ]
)
def test_meta_files_processed_null(
    db: DBs,
    connections: dict[DBs, Engine | None],
    violation_messages: dict[DBs, dict[str, str]],
    null_key: str
) -> None:
    """
    """
    db_engine = connections.get(db)
    if db_engine is None:
        pytest.skip()

    bad_example = {
        "filename": "Test4",
        "timestamp": dt.datetime(2020, 1, 4)
    }

    insert_statement = insert(orm.MetaFilesProcessed)
    with db_engine.connect() as conn:
        nulled = bad_example.copy()
        nulled.pop(null_key)
        with pytest.raises(
            sqlexc.IntegrityError,
            match=violation_messages[db]["Null"]
        ):
            _ = conn.execute(
                insert_statement,
                [nulled]
            )


def test_bad_engine() -> None:
    """"""
    with pytest.raises(
        ValueError,
        match=r"Invalid database url provided."
    ):
        engine.get_database("Bad")
