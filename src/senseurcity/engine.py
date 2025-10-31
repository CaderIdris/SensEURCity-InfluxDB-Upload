from enum import auto, Flag
import logging

from sqlalchemy import create_engine, event
from sqlalchemy.engine.base import Engine
from sqlalchemy import schema

_logger = logging.getLogger(f"__main__.{__name__}")


class Database(Flag):
    """Which database is being used?"""
    SQLite = auto()
    PostgreSQL = auto()


class DatabaseConfig(Flag):
    """What does the database support?"""
    SupportsSchema = auto()


def get_database(db_url: str) -> Database:
    """Determine database type from `db_url`.

    Returns
    -------
    `Database` enum representing which database is being used.
    """
    db = Database(0)
    if db_url[:6] == "sqlite":
        _logger.debug("SQLite database selected")
        db = db | Database.SQLite
    elif db_url[:10] == "postgresql":
        _logger.debug("PostgreSQL database selected")
        db = db | Database.PostgreSQL
    else:
        raise ValueError("Invalid database url provided.")
    return db


def get_database_config(db: Database) -> DatabaseConfig:
    """Create enum representing database configuration options.

    Returns
    -------
    `DatabaseConfig` enum representing the configuration options available
    for the database (e.g SupportsSchema).

    Returns
    -------
    SQLAlchemy Engine
    """
    db_config = DatabaseConfig(0)
    if db == Database.PostgreSQL:
        _logger.debug("Setting configuration options relevant for PostgreSQL")
        db_config = (
            db_config |
            DatabaseConfig.SupportsSchema
        )
    return db_config


def configure_db(
    db: Database,
    db_config: DatabaseConfig,
    engine: Engine,
    schema_name: str
) -> Engine:
    """Configure the database based on its type.

    Configuration steps:
    1. **SQLite**
        - Remove schema as SQLite does not support them
        - Enable foreign key constraints, disabled by default in SQLite
    2. **Supports schemas**
        - Set a custom schema name if one is given
        - Create the schema

    Parameters
    ----------
    db : Database
        Which database is being used?
    db_config : DatabaseConfig
        Configuration options for database.
    engine : Engine
        The SQLAlchemy engine.
    schema_name : str
        The name of the schema to use for the tables.

    Returns
    -------
    SQLAlchemy Engine
    """
    if db == Database.SQLite:
        _logger.debug("Configuration: SQLite specific settings")
        engine = engine.execution_options(
            schema_translate_map = {
                "measurement": None
            }
        )
        event.listen(
            engine,
            'connect',
            lambda e, _: e.execute('pragma foreign_keys=on')
        )
    if DatabaseConfig.SupportsSchema in db_config:
        _logger.debug("Configuration: Schema options")
        if schema_name != "measurement":
            engine = engine.execution_options(
                schema_translate_map = {
                    "measurement": schema_name
                }
            )
        with engine.connect() as conn:
            conn.execute(schema.CreateSchema(schema_name))
            conn.commit()
    return engine


def get_engine(
    db_url: str,
    schema_name: str = "measurement",
) -> Engine:
    """Create the SQLAlchemy engine and configure it.

    Parameters
    ----------
    db_url : str
        The database URL.
    schema_name: str, default="measurement"
        The schema name to use for the tables.

    Returns
    -------
    SQLAlchemy Engine
    """
    _logger.info("Connecting to %s", db_url)
    engine = create_engine(db_url)
    db = get_database(db_url)
    db_config = get_database_config(db)
    engine = configure_db(db, db_config, engine, schema_name)
    return engine

