"""SQLAlchemy configuration.

Configure the schemas for the required tables and model all relationships
between tables. Includes unique, foreign key and not null constraints
to the tables to maintain data integrity.
"""
from datetime import datetime
from typing import Any, ClassVar

from sqlalchemy import (
    ForeignKeyConstraint,
    MetaData,
    UniqueConstraint
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.types import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _BaseV1(DeclarativeBase):
    type_annotation_map: ClassVar[dict[type, Any]] = {
        dict[str, Any]: JSON
    }
    metadata = MetaData(schema="measurement")



class DimDevice(_BaseV1):
    """Declarative mapping of dimension table representing LCS.

    This table is used to store information on all devices whose measurements
    are recorded within the database.

    Schema name: **measurement**

    Table name: **dim_device**

    Schema
    ------
    - *code* [str, pk]: The provided name for the sensor.
    - *dataset* [str, not null]: Which dataset the sensor comes from.
    - *name* [str, unique, not null]: A more human readable name.
    - *short_name* [str, unique, not null]: A shorter name to use.
    - *reference* [bool, not null]: Is it a reference or equivalent device?.
    - *other* [json]: Misc fields.
    """

    __tablename__ = "dim_device"

    key: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(nullable=False, unique=True)
    short_name: Mapped[str] = mapped_column(nullable=False, unique=True)
    dataset: Mapped[str] = mapped_column(nullable=False)
    reference: Mapped[bool] = mapped_column(nullable=False)
    other: Mapped[dict[str, Any]] = mapped_column(nullable=True)


class DimHeader(_BaseV1):
    """Declarative mapping of measurement headers dimension table.

    This table is used to store information on all measurement headers that
    represent measurements made within the database. Headers (e.g. ox_a431)
    represent a measurement made by an ox_a431 sensor. The parameter it
    measures and the unit of measurement are stored with it.

    Schema name: **measurement**

    Table name: **dim_header**

    Schema
    ------
    - *header* [str, pk]: The measurement header
    - *parameter* [str, not null]: What the device is measuring.
    - *unit* [str, not null]: The unit of measurement
    - *other* [json]: Misc fields.
    """

    __tablename__ = "dim_header"

    header: Mapped[str] = mapped_column(primary_key=True)
    parameter: Mapped[str] = mapped_column(nullable=False)
    unit: Mapped[str] = mapped_column(nullable=False)
    other: Mapped[dict[str, Any]] = mapped_column(nullable=True)

class BridgeDeviceHeaders(_BaseV1):
    """Declarative mapping of the headers corresponding to a device table.

    This table is used to store all headers that a device uses. This can be
    used as a lookup to see which headers represent which measured pollutant.

    **Relationship between device and header used to measure specific parameter:**
    ```mermaid
    graph TD;
        A[dim_device];
        B[bridge_device_headers];
        C[dim_header];
        A--dim_device.key = bridge_device_headers.device_key-->B;
        B--bridge_device_headers.header = dim_header.header-->C;

        style A fill:#458588,stroke:#0d0e0f,color:#dedede;
        style B fill:#7fa2ac,stroke:#0d0e0f,color:#dedede;
        style C fill:#458588,stroke:#0d0e0f,color:#dedede;

    ```
    This relationship can be used to look up:
    - Which devices measure a certain parameter.
    - Inversely, which parameters a device measures.
    - Which header is used to measure a certain parameter.

    Schema name: **measurement**

    Table name: **bridge_device_headers**

    Schema
    ------
    - *device_key* [str, pk]: The device.
    - *header* [str, pk]: The measurement header.
    - *flag* [str]: A flag associated with the header.
    """

    __tablename__ = "bridge_device_headers"

    device_key: Mapped[str] = mapped_column(primary_key=True)
    header: Mapped[str] = mapped_column(primary_key=True)
    flag: Mapped[str] = mapped_column(nullable=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["device_key"],
            ["dim_device.key"]
        ),
        ForeignKeyConstraint(
            ["header"],
            ["dim_header.header"]
        )
    )

class DimUnitConversion(_BaseV1):
    """Declarative mapping of unit_conversion dimension table.

    This table is used as a lookup for how to convert between different units.

    Schema name: **measurement**

    Table name: **dim_unit_conversion**

    Schema
    ------
    - id [int, pk]: The row id.
    - unit_in [str, not null]: The initial unit.
    - unit_out [str, not null]: The unit to be converted to.
    - parameter [str, not null]: The parameter being measured
    - scale [float, not null]: Value to multiply the measurement by for \
    conversion.
    """

    __tablename__ = "dim_unit_conversion"

    unit_in: Mapped[str] = mapped_column(primary_key=True)
    unit_out: Mapped[str] = mapped_column(primary_key=True)
    parameter: Mapped[str] = mapped_column(primary_key=True)
    scale: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        UniqueConstraint("unit_in", "unit_out", "parameter"),
    )


class FactMeasurement(_BaseV1):
    """Declarative mapping of measurements table.

    Schema name: **measurement**

    Table name: **fact_measurement**

    Schema
    ------
    time [datetime, pk]: Time a measurement was made
    device_key [str, pk]: The device making the measurement
    measurements [JSON, not null]: Measurements recorded
    flags [JSON]: Flags corresponding to the measurement
    meta [JSON]: Other information
    """

    __tablename__ = "fact_measurement"

    time: Mapped[datetime] = mapped_column(primary_key=True)
    device_key: Mapped[str] = mapped_column(primary_key=True)
    measurements: Mapped[dict[str, Any]] = mapped_column(nullable=False)
    flags: Mapped[dict[str, Any]] = mapped_column(nullable=True)
    meta: Mapped[dict[str, Any]] = mapped_column(nullable=True)
    __table_args__ = (
        ForeignKeyConstraint(
            ["device_key"],
            ["dim_device.key"]
        ),
    )


class DimColocation(_BaseV1):
    """Declarative mapping of co-location dimension table.

    This table is used to store information on periods a sensor is co-located
    with a reference monitor.

    Schema name: **measurement**

    Table name: **dim_colocation**

    Schema
    ------
    - *id* [int, pk]: Row index
    - *device_key* [str, not null]: Which device is co-located.
    - *other_key* [str, not null]: The other device it is co-located with.
    - *start_date* [datetime, not null]: When the co-location started.
    - *timestamp* [datetime, not null]: When the co-location ended.
    """

    __tablename__ = "dim_colocation"

    device_key: Mapped[str] = mapped_column(primary_key=True)
    other_key: Mapped[str] = mapped_column(primary_key=True)
    start_date: Mapped[datetime] = mapped_column(primary_key=True)
    end_date: Mapped[datetime] = mapped_column(primary_key=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["device_key"],
            ["dim_device.key"]
        ),
        ForeignKeyConstraint(
            ["other_key"],
            ["dim_device.key"]
        ),
    )


class MetaFilesProcessed(_BaseV1):
    """Declarative mapping of table containing files processed.

    This table is used to store information on which files have already had
    their measurements saved to the DB.

    Schema name: **measurement**

    Table name: **meta_files_processed**

    Schema
    ------
    - *filename* [filename, pk]: The file that has been processed.
    - *timestamp* [datetime]: When it was processed.
    """

    __tablename__ = "meta_files_processed"

    filename: Mapped[str] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime]


def create_tables(engine: Engine) -> None:
    """Create all tables in _BaseV1 ORM.

    Parameters
    ----------
    engine : Engine
        The SQLAlchemy engine connected to the DB.

    """
    _BaseV1.metadata.create_all(engine)
