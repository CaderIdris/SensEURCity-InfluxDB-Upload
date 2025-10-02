"""SQLAlchemy configuration.

Configure the ORMs for the database.
"""
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKeyConstraint, Index, MetaData
from sqlalchemy.types import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _Base(DeclarativeBase):
    type_annotation_map = {
        dict[str, Any]: JSON
    }
    metadata = MetaData(schema="measurement")


class DimDevice(_Base):
    """Declarative mapping of dimension table representing LCS.

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

    code: Mapped[str] = mapped_column(primary_key=True)
    dataset: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False, unique=True)
    short_name: Mapped[str] = mapped_column(nullable=False, unique=True)
    reference: Mapped[bool] = mapped_column(nullable=False)
    other: Mapped[dict[str, Any]] = mapped_column(nullable=True)


class DimHeader(_Base):
    """Declarative mapping of measurement headers dimension table.

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


class FactMeasurement(_Base):
    """Declarative mapping of measurement fact table.

    Schema name: **measurement**

    Table name: **fact_measurement**

    Schema
    ------
    - *measurement_hash* [str, pk]: Hash of the *timestamp*, *code* and \
    *header* columns.
    - *point_hash* [str, not null] Hash of the *timestamp* and *code* columns.
    - *timestamp* [datetime, not null]: The time the measurement was made.
    - *code* [str, not null]: The code representing the measurement device.
    - *header* [str, not null]: The code of the field.
    - *value* [float, not null]: The value of the measurement.

    Indexes
    -------
    - ix_measurement: Unique index encompassing the *timestamp*, *code* and \
    *header* of a measurement, representing a single measurement at a single \
    point in time
    - ix_point_hash: A hashed version of ix_measurement used to join \
    **dim_flag**

    Foreign Keys
    ------------
    - code: References the *code* index column in **dim_device**
    - header: References the *header* index column in **dim_header**
    """

    __tablename__ = "fact_measurement"

    measurement_hash: Mapped[str] = mapped_column(primary_key=True)
    point_hash: Mapped[str] = mapped_column(nullable=False)
    timestamp: Mapped[datetime] = mapped_column(nullable=False)
    code: Mapped[str] = mapped_column(nullable=False)
    header: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        Index(
            "ix_measurement",
            "timestamp",
            "code",
            "header",
            unique=True
        ),
        Index(
            "ix_point_hash",
            "point_hash",
            unique=True
        ),
        ForeignKeyConstraint(
            ["code"],
            ["dim_device.code"]
        ),
        ForeignKeyConstraint(
            ["header"],
            ["dim_header.header"]
        )
    )


class DimFlag(_Base):
    """Declarative mapping of measurement flags dimension table.

    Schema name: **measurement**

    Table name: **dim_flag**

    Schema
    ------
    - *id* [int, pk]: Autoincrementing row number.
    - *point_hash* [str, not null]: Hash of the timestamp and sensor name.
    - *flag* [str, not null]: The code of the flag.
    - *value* [str, not null]: The value of the flag.

    Foreign Keys
    ------------
    - point_hash: Reference the *point_hash* column in **fact_measurement** \
        *N.B. This is a many-many relationship, one or both tables should be \
        filtered prior to a join to reduce to a one-to-one or one-to-many.*
    """

    __tablename__ = "dim_flag"

    id: Mapped[int] = mapped_column(primary_key=True)
    point_hash: Mapped[str] = mapped_column(nullable=False)
    flag: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["point_hash"],
            ["fact_measurement.point_hash"],
        ),
    )
