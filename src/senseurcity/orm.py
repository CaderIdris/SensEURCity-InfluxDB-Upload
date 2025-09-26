"""SQLAlchemy configuration.

Configure the ORMs for the database.
"""
from datetime import datetime

from sqlalchemy import ForeignKeyConstraint, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _Base(DeclarativeBase):
    pass


class DimLCS(_Base):
    """Declarative mapping of dimension table representing LCS.

    Models a table with the following columns:
    - id [int]: Row id, autoincrementing. Not set manually.
    - code[str]: The provided name for the sensor.
    - name[str]: A more human readable name.
    """

    __tablename__ = "dim_lcs"

    code: Mapped[str] = mapped_column(primary_key=True, unique=True)
    name: Mapped[str] = mapped_column(nullable=False, unique=True)
    short_name: Mapped[str] = mapped_column(nullable=False, unique=True)


class DimHeader(_Base):
    """"""

    __tablename__ = "dim_header"

    header: Mapped[str] = mapped_column(primary_key=True, unique=True)
    supplier: Mapped[str] = mapped_column(nullable=False)
    sensor: Mapped[str] = mapped_column(nullable=False)
    type: Mapped[str] = mapped_column(nullable=False)
    parameters: Mapped[str] = mapped_column(nullable=False)
    units: Mapped[str] = mapped_column(nullable=False)


class FactLCS(_Base):
    """Declarative mapping of LCS Measurements table.

    Models a table with the following columns:
    - measurement_hash [str]: Hash of the timestamp, sensor name and \
    measurement header.
    - point_hash [str] Hash of the timestamp and sensor name.
    - timestamp [datetime]: The time the measurement was made.
    - code [str]: The provided name of the sensor.
    - header [str]: The code of the field.
    - value [float]: The value of the measurement.
    """

    __tablename__ = "fact_lcs"

    measurement_hash: Mapped[str] = mapped_column(primary_key=True, unique=True)
    point_hash: Mapped[str] = mapped_column(nullable=False)
    timestamp: Mapped[datetime] = mapped_column(nullable=False)
    code: Mapped[str] = mapped_column(nullable=False)
    header: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        Index(
            "ix_lcs_measurements",
            "timestamp",
            "code",
            "header",
            unique=True
        ),
        Index(
            "ix_lcs_point_hash",
            "point_hash",
            unique=True
        ),
        ForeignKeyConstraint(
            ["code"],
            ["dim_lcs.code"]
        ),
        ForeignKeyConstraint(
            ["header"],
            ["dim_header.header"]
        )
    )


class DimLCSFlags(_Base):
    """Declarative mapping of LCS Measurement Flags table.

    Models a table with the following columns:
    - id [int]: Autoincrementing row number.
    - point_hash [str]: Hash of the timestamp and sensor name.
    - flag [str]: The code of the flag.
    - value [str]: The value of the flag.
    """

    __tablename__ = "dim_lcs_flags"

    id: Mapped[int] = mapped_column(primary_key=True, unique=True)
    point_hash: Mapped[str] = mapped_column(nullable=False)
    flag: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[str] = mapped_column(nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["point_hash"],
            ["fact_lcs.point_hash"],
        ),
    )


class DimLCSColocation(_Base):
    """Declarative mapping of LCS Co-Location table.

    Models a table with the following columns:
    - point_hash [str]: Hash of the timestamp and sensor name
    - location_id [str]: Where the sensor was co-located
    """

    __tablename__ = "dim_lcs_colocation"

    point_hash: Mapped[str] = mapped_column(primary_key=True, unique=True)
    location_id: Mapped[str]

    __table_args__ = (
        ForeignKeyConstraint(
            ["point_hash"],
            ["fact_lcs.point_hash"],
        ),
        ForeignKeyConstraint(
            ["location_id"],
            ["dim_ref.location_id"],
        ),
    )


class DimRef(_Base):
    """Declarative mapping of dimension table representing reference sites.

    Models a table with the following columns:
    - location_id [str]: Site ID.
    - name [str]: Name to use for reference site
    - short_name [str]: Shortened name for reference site
    - city [str]: The city the reference site is in.
    - latitude_dd [float]: Location (latitude).
    - longitude_dd [float]: Location (longitude).
    - distance_to_road_m [int]: Distance from road in metres.
    - average_hourly_traffic_intensity_number_per_h [str]: Traffic intensity. \
    Not always an integer...
    - notes [str]: Notes on reference site.
    - co_equipment [str]: The CO monitoring equipment.
    - co_unit [str]: The CO measurement unit.
    - co2_equipment [str]: The CO^2^ monitoring equipment.
    - co2_unit [str]: The CO^2^ measurement unit.
    - no_equipment [str]: The NO monitoring equipment.
    - no_unit [str]: The NO measurement unit.
    - no2_equipment [str]: The NO^2^ monitoring equipment.
    - no2_unit [str]: The NO^2^ measurement unit.
    - o3_equipment [str]: The O^3^ monitoring equipment.
    - o3_unit [str]: The O^3^ measurement unit.
    - pm10_equipment [str]: The PM~10~ monitoring equipment.
    - pm10_unit [str]: The PM~10~ measurement unit.
    - pm25_equipment [str]: The PM~2.5~ monitoring equipment.
    - pm25_unit [str]: The PM~2.5~ measurement unit.
    - pm1_equipment [str]: The PM~1~ monitoring equipment.
    - pm1_unit [str]: The PM~1~ measurement unit.
    - other_pm10_equipment [str]: Secondary PM~10~ monitoring equipment.
    - other_pm10_unit [str]: Secondary PM~10~ measurement unit.
    - other_pm25_equipment [str]: Secondary PM~2.5~ monitoring equipment.
    - other_pm25_unit [str]: Secondary PM~2.5~ measurement unit.
    - other_pm1_equipment [str]: Secondary PM~1~ monitoring equipment.
    - other_pm1_unit [str]: Secondary PM~1~ measurement unit.
    """

    __tablename__ = "dim_ref"

    location_id: Mapped[str] = mapped_column(primary_key=True, unique=True)
    name: Mapped[str] =  mapped_column(nullable=False, unique=True)
    short_name: Mapped[str] = mapped_column(nullable=False, unique=True)
    city: Mapped[str] = mapped_column(nullable=False)
    latitude_dd: Mapped[float] = mapped_column(nullable=False)
    longitude_dd: Mapped[float] = mapped_column(nullable=False)
    distance_to_road_m: Mapped[int]
    average_hourly_traffic_intensity_number_per_h: Mapped[str]
    notes: Mapped[str]
    co_equipment: Mapped[str]
    co_unit: Mapped[str]
    co2_equipment: Mapped[str]
    co2_unit: Mapped[str]
    no_equipment: Mapped[str]
    no_unit: Mapped[str]
    no2_equipment: Mapped[str]
    no2_unit: Mapped[str]
    o3_equipment: Mapped[str]
    o3_unit: Mapped[str]
    pm10_equipment: Mapped[str]
    pm10_unit: Mapped[str]
    pm25_equipment: Mapped[str]
    pm25_unit: Mapped[str]
    pm1_equipment: Mapped[str]
    pm1_unit: Mapped[str]
    other_pm10_equipment: Mapped[str]
    other_pm10_unit: Mapped[str]
    other_pm25_equipment: Mapped[str]
    other_pm25_unit: Mapped[str]
    other_pm1_equipment: Mapped[str]
    other_pm1_unit: Mapped[str]


class FactRef(_Base):
    """Declarative mapping of Ref Measurements table.

    Models a table with the following columns:
    - measurement_hash [str]: Hash of the timestamp, sensor name and \
    measurement header
    - timestamp [datetime]: The time the measurement was made.
    - name [str]: The name of the reference monitor.
    - measurement [str]: The field code.
    - value [float]: The value of the measurement.

    It adds a unique constraint across timestamp, code and measurement to
    ensure no duplicates.

    It adds an index column to timestamp, code and measurement to improve
    query performance.
    """

    __tablename__ = "fact_ref"

    measurement_hash: Mapped[str] = mapped_column(primary_key=True, unique=True)
    timestamp: Mapped[datetime] = mapped_column(nullable=False)
    location_id: Mapped[str] = mapped_column(nullable=False)
    measurement: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[float] = mapped_column(nullable=False)

    __table_args__ = (
        Index(
            "ix_ref_measurements",
            "timestamp",
            "location_id",
            "measurement",
            unique=True
        ),
        ForeignKeyConstraint(
            ["location_id"],
            ["dim_ref.location_id"]
        )
    )

