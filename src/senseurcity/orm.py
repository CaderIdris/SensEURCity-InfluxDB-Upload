"""SQLAlchemy configuration.

Configure the schemas for the required tables and model all relationships
between tables. Includes unique, foreign key and not null constraints
to the tables to maintain data integrity.
"""
from datetime import datetime
from typing import Any

from sqlalchemy import ForeignKeyConstraint, MetaData
from sqlalchemy.types import JSON
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _Base_V1(DeclarativeBase):
    type_annotation_map = {
        dict[str, Any]: JSON
    }
    metadata = MetaData(schema="measurement")


class DimDevice(_Base_V1):
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


class DimHeader(_Base_V1):
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


class DimColocation(_Base_V1):
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

    id: Mapped[int] = mapped_column(primary_key=True)
    device_key: Mapped[str] = mapped_column(nullable=False)
    other_key: Mapped[str] = mapped_column(nullable=False)
    start_date: Mapped[datetime] = mapped_column(nullable=False)
    end_date: Mapped[datetime] = mapped_column(nullable=False)

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


class FactMeasurement(_Base_V1):
    """Declarative mapping of measurement point fact table.

    This table is used to log when a measurement is made by a single device at
    a single point in time. The measurements made and any flags associated with
    them are separated into **fact_value** and **fact_flag** respectively.

    Schema name: **measurement**

    Table name: **fact_measurement**

    Schema
    ------
    - *point_hash* [str, pk] Hash of the *timestamp* and *code* columns.
    - *timestamp* [datetime, not null]: The time the measurement was made.
    - *code* [str, not null]: The code representing the measurement device.
    """

    __tablename__ = "fact_measurement"

    point_hash: Mapped[str] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(nullable=False)
    device_key: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["device_key"],
            ["dim_device.key"]
        ),
    )


class FactValue(_Base_V1):
    """Declarative mapping of measurement values fact table.

    Schema name: **measurement**

    Table name: **fact_flag**

    Schema
    ------
    - *id* [int, pk]: Autoincrementing row number.
    - *point_hash* [str, not null]: Hash of the timestamp and sensor name.
    - *flag* [str, not null]: The code of the flag.
    - *value* [str, not null]: The value of the flag.

    Foreign Keys
    ------------
    - point_hash: References the *point_hash* column in **fact_measurement** \
        *N.B. This is a many-many relationship, one or both tables should be \
        filtered prior to a join to reduce to a one-to-one or one-to-many.*
    - header: References the *header* column in **dim_header**
    """

    __tablename__ = "fact_value"

    id: Mapped[int] = mapped_column(primary_key=True)
    point_hash: Mapped[str] = mapped_column(nullable=False)
    header: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["point_hash"],
            ["fact_measurement.point_hash"]
        ),
        ForeignKeyConstraint(
            ["header"],
            ["dim_header.header"]
        )
    )


class FactFlag(_Base_V1):
    """Declarative mapping of measurement flags fact table.

    Schema name: **measurement**

    Table name: **fact_flag**

    Schema
    ------
    - *id* [int, pk]: Autoincrementing row number.
    - *point_hash* [str, not null]: Hash of the timestamp and sensor name.
    - *flag* [str, not null]: The code of the flag.
    - *value* [str, not null]: The value of the flag.

    Foreign Keys
    ------------
    - point_hash: References the *point_hash* column in **fact_measurement** \
        *N.B. This is a many-many relationship, one or both tables should be \
        filtered prior to a join to reduce to a one-to-one or one-to-many.*
    """

    __tablename__ = "fact_flag"

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
