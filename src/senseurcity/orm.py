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

    Models a table with the following columns:
    - code [str]: The provided name for the sensor.
    - dataset [str]: Which dataset the sensor comes from.
    - name [str]: A more human readable name.
    - short_name [str]: A shorter name to use.
    - reference [bool]: Is it a reference or equivalent device?.
    - other [json]: Misc fields.
    """

    __tablename__ = "dim_device"

    code: Mapped[str] = mapped_column(primary_key=True, unique=True)
    dataset: Mapped[str] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(nullable=False, unique=True)
    short_name: Mapped[str] = mapped_column(nullable=False, unique=True)
    reference: Mapped[bool] = mapped_column(nullable=False)
    other: Mapped[dict[str, Any]] = mapped_column(nullable=True)


class DimHeader(_Base):
    """

    - other [json]: Misc fields.
    """

    __tablename__ = "dim_header"

    header: Mapped[str] = mapped_column(primary_key=True, unique=True)
    parameter: Mapped[str] = mapped_column(nullable=False)
    unit: Mapped[str] = mapped_column(nullable=False)
    other: Mapped[dict[str, Any]] = mapped_column(nullable=True)


class FactMeasurement(_Base):
    """Declarative mapping of measurement fact table.

    Models a table with the following columns:
    - measurement_hash [str]: Hash of the timestamp, sensor name and \
    measurement header.
    - point_hash [str] Hash of the timestamp and sensor name.
    - timestamp [datetime]: The time the measurement was made.
    - name [str]: The name of the sensor.
    - header [str]: The code of the field.
    - value [float]: The value of the measurement.
    """

    __tablename__ = "fact_measurement"

    measurement_hash: Mapped[str] = mapped_column(primary_key=True, unique=True)
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

    Models a table with the following columns:
    - id [int]: Autoincrementing row number.
    - point_hash [str]: Hash of the timestamp and sensor name.
    - flag [str]: The code of the flag.
    - value [str]: The value of the flag.
    """

    __tablename__ = "dim_flag"

    id: Mapped[int] = mapped_column(primary_key=True, unique=True)
    point_hash: Mapped[str] = mapped_column(nullable=False)
    flag: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["point_hash"],
            ["fact_measurement.point_hash"],
        ),
    )
