"""Transform the SensEURCity data."""
from collections.abc import Generator
from dataclasses import dataclass
import datetime as dt
from importlib.resources import files
import json
import logging
import re
from typing import Self, TypedDict

import numpy as np
import pandas as pd

_logger = logging.getLogger("SensEURCity-ETL")

class MeasurementRecord(TypedDict):
    """Structure of a dictionary returning a single measurement period.

    Attributes
    ----------
    time : dt.datetime
        The time the measurement was taken.
    device_key : str
        The device making the measurement.
    measurements : dict[str, float]
        The measurements made.
    flags : dict[str, str]
        The flags associated with the measurement.
    meta : dict[str, float]
        The metadata associated with the measurement.

    """

    time: dt.datetime
    device_key: str
    measurements: dict[str, float]
    flags: dict[str, str] | None
    meta: dict[str, float] | None


class ColocationRecord(TypedDict):
    """Structure of a dictionary returned as a single co-location period.

    Attributes
    ----------
    device_key : str
        The co-located device.
    other_key : str
        The device it was co-located with.
    start_date : pd.Timestamp
        The start of the co-location (inclusive).
    end_date : pd.Timestamp
        The end of the co-location (inclusive).

    """

    device_key: str
    other_key: str
    start_date: pd.Timestamp
    end_date: pd.Timestamp


class HeaderRecord(TypedDict):
    """Structure of a dictionary returned as a single header record.

    Attributes
    ----------
    header : str
        The measurement header.
    parameter : str
        The parameter it's measuring (NO, T etc).
    unit : str
        The unit of measurement.
    other : dict[str, float | int | str]
        Any other information.

    """

    header: str
    parameter: str
    unit: str
    other: dict[str, float | int | str]


class DeviceRecord(TypedDict):
    """Expected structure of a dictionary returned as a single device record.

    Attributes
    ----------
    key : str
        The device key.
    name : str
        The full name of the device.
    short_name : str
        A short name to use for the device.
    dataset : str
        The dataset the device originates from.
    reference : bool
        Is it a reference device?
    other: dict[str, float | int | str]
        Any other information.

    """

    key: str
    name: str
    short_name: str
    dataset: str
    reference: bool
    other: dict[str, float | int | str]


class DeviceHeaderRecord(TypedDict):
    """Structure of a dictionary returned as a single header record.

    Attributes
    ----------
    header : str
        The measurement header.
    device_key : str
        The device measuring.
    flag : str
        An associated flag

    """

    header: str
    device_key: str
    flag: str | None


class ConversionRecord(TypedDict):
    """Expected structure of a dictionary returned as a single conversion record.

    Attributes
    ----------
    unit_in : str
        The initial unit.
    unit_out : str
        The unit to be converted to.
    parameter : str
        The parameter of the measurement (e.g. O3).
    scale : float
        How to convert the unit from one to the other.

    """

    unit_a: str
    unit_b: str
    parameter: str
    scale: float


@dataclass
class SensEURCityCSV:
    """Dataclass for getting measurements from a SensEURCity csv file.

    Attributes
    ----------
    name : str
        The name of the device (taken from the filename).
    csv : pd.DataFrame
        The contents of the csv file.
    measurement_cols : set[str]
        A collection of the column names that represent the lcs measurements.
        Can vary depending on the csv file so should be dynamically generated
        per csv.
    flag_cols : set[str]
        A collection of the column names that represent flags associated with
        the lcs measurements. Can vary depending on the csv file so should be
        dynamically generated per csv.
    reference_cols : set[str]
        A collection of the column names that represent the reference
        measurements.

    """

    name: str
    csv: pd.DataFrame
    measurement_cols: set[str]
    metadata_cols: set[str]
    flag_cols: set[str]
    reference_cols: set[str]
    date_col: str
    location_col: str

    @classmethod
    def from_dataframe(
        cls,
        name: str,
        csv: pd.DataFrame,
        date_col: str = "date",
        location_col: str = "Location.ID",
    ) -> Self:
        """Create an instance of the class from a DataFrame.

        Parameters
        ----------
        name : str
            The name of the sensor.
        csv : pd.DataFrame
            The contents of the csv file.
        date_col : str, default='date'
            The name of the date column.
        location_col : str, default='Location.ID'
            The name of the location column.

        Raises
        ------
        ValueError
            If the specified date or location column are not present in the
            DataFrame.

        """
        # Check keyword arguments to ensure column is in csv and isn't protected
        if date_col not in csv.columns:
            err_msg = (
                f"'{date_col}' is not present in {name}.csv. Expected a "
                "valid name for the date column."
            )
            raise ValueError(err_msg)

        if location_col not in csv.columns:
            err_msg = (
                f"'{location_col}' is not present in {name}.csv. Expected a "
                "valid name for the location column."
            )
            raise ValueError(err_msg)

        _logger.info("Parsing records from %s", name)
        # Fix strange space in some columns
        csv[location_col] = (
            csv[location_col]
            .str.strip()
            .str.replace("/", "_")
            .str.replace("-", "_")
        )

        # Split columns into reference and flag columns
        reference_cols = {
            col for col in csv.columns
            if col[:4] == "Ref."
        } - {
            location_col,
            "Ref.Lat",
            "Ref.Long",
        }
        _logger.info("%s reference columns found", len(reference_cols))
        _logger.debug("Reference columns: %s", f'"{", ".join(reference_cols)}"')

        flag_cols = {
            col for col in csv.columns
            if col[-5:] == "_flag"
        }
        _logger.info("%s flag columns found", len(flag_cols))
        _logger.debug("Flag columns: %s", f'"{", ".join(flag_cols)}"')


        # Determine measurement columns
        measurement_cols = {
            col[:-5] for col in flag_cols if col[:-5] in csv.columns
        }
        _logger.info("%s measurement columns found", len(measurement_cols))
        _logger.debug("Measurement columns: %s", f'"{", ".join(measurement_cols)}"')



        # Parse date and calculate hash columns
        csv[date_col] = pd.to_datetime(csv[date_col], format="%Y-%m-%dT%H:%M:%SZ")
        csv = csv.set_index(date_col)
        csv.index.name = "date"

        metadata_cols = (
            set(csv.columns) -
            reference_cols -
            flag_cols -
            measurement_cols -
            {location_col}
        )
        _logger.info("%s metadata columns found", len(metadata_cols))
        _logger.debug("Metadata columns: %s", f'"{", ".join(metadata_cols)}"')

        return cls(
            name=name,
            csv=csv,
            measurement_cols=measurement_cols,
            flag_cols=flag_cols,
            reference_cols=reference_cols,
            metadata_cols=metadata_cols,
            date_col=date_col,
            location_col=location_col
        )

    @property
    def measurements(self) -> Generator[MeasurementRecord]:
        """Set of records representing device measurements and flags.

        Represents an iterator containing dictionaries with the following keys:

        - **time** : dt.datetime
        - **device_key** : str
        - **measurements** : dict[str, float]
        - **flags** : dict[str, str]
        - **meta** : dict[str, float]
        """
        _logger.info("Querying measurements for %s", self.name)
        measurements = self.csv.loc[
            :, tuple(self.measurement_cols)
        ]
        measurements.columns = pd.MultiIndex.from_product(
            [["measurements"], measurements.columns]
        )

        flags = self.csv.loc[:, tuple(self.flag_cols)]
        flags = flags.fillna("Valid")
        flags.columns = pd.MultiIndex.from_product(
            [["flags"], flags.columns]
        )

        metadata = self.csv.loc[:, tuple(self.metadata_cols)]
        metadata.columns = pd.MultiIndex.from_product(
            [["meta"], metadata.columns]
        )


        csv_subset = measurements.join(flags)
        csv_subset = csv_subset.join(metadata)
        csv_subset[("index", "time")] = measurements.index
        csv_subset[("index", "device_key")] = self.name

        for _, record in csv_subset.iterrows():
            row: MeasurementRecord =  {
                "time": record[("index", "time")].to_pydatetime(),
                "device_key": record[("index", "device_key")],
                "measurements": record["measurements"].dropna().to_dict(),
                "flags": record["flags"].to_dict(),
                "meta": record["meta"].dropna().to_dict()
            }
            if not row["measurements"]:
                continue
            yield row

    @property
    def reference_measurements(self) -> Generator[MeasurementRecord]:
        """Set of records representing reference measurements.

        Represents an iterator containing dictionaries with the following keys:

        - **time** : dt.datetime
        - **device_key** : str
        - **measurements** : dict[str, float]
        - **flags** : None
        - **meta** : None
        """
        _logger.info("Querying reference measurements for %s", self.name)
        csv_subset = self.csv.loc[
            :, (*self.reference_cols, self.location_col)
        ].dropna(subset=self.location_col)
        repeated = csv_subset[list(self.reference_cols)].eq(
            csv_subset[list(self.reference_cols)].shift(-1)
        )
        csv_subset[repeated] = np.nan
        csv_subset["time"] = csv_subset.index
        csv_subset = csv_subset.rename(columns={self.location_col: "device_key"})
        for _, record in csv_subset.iterrows():
            row: MeasurementRecord = {
                "time": record["time"].to_pydatetime(),
                "device_key": record["device_key"],
                "measurements": record[list(self.reference_cols)].dropna().to_dict(),
                "flags": None,
                "meta": None
            }
            if not row["measurements"]:
                continue
            yield row

    @property
    def colocation(self) -> Generator[ColocationRecord]:
        """Set of records representing a single colocation period.

        Represents an iterator containing dictionaries with the following keys:

        - **device_key** : str
        - **other_key** : str
        - **start_date** : dt.datetime
        - **end_date** : dt.datetime
        """
        # Select rows where the location ID changes
        _logger.info("Querying colocation periods for %s", self.name)
        csv_subset = (
                self.csv
                .loc[:, [self.location_col]]
                .fillna("")
                .reset_index()
        )
        break_point = (
                csv_subset[self.location_col] !=
                csv_subset[self.location_col].shift(1)
        )
        changed_rows = csv_subset[break_point].reset_index()

        # Assign each change to a group
        csv_subset.loc[changed_rows["index"], "Group"] = list(
                changed_rows.index
        )
        csv_subset["Group"] = csv_subset["Group"].ffill()

        # Remove blank periods
        csv_subset = csv_subset[csv_subset[self.location_col] != ""]

        # Group and generate co-location dataset
        grouped = (
            csv_subset.groupby([self.location_col, "Group"]).agg(
                start_date=pd.NamedAgg(column="date", aggfunc="min"),
                end_date=pd.NamedAgg(column="date", aggfunc="max"),
            )
            .reset_index()
            .drop("Group", axis=1)
            .rename(
                {self.location_col: "other_key"},
                axis=1
            )
            .sort_values("start_date")
        )
        grouped["device_key"] = self.name
        # for record in csv_subset.to_dict('records'):
        #     yield record
        for record in grouped.iterrows():
            yield record[1].to_dict()

    @property
    def device_headers(self) -> Generator[DeviceHeaderRecord]:
        """Set of records representing all measurement headers for a device.

        Represents an iterator containing dictionaries with the following keys:

        - **device_key** : str
        - **header** : str
        - **flag** : str | None
        """
        _logger.info("Querying measurement headers for %s", self.name)
        measurement_headers = self.csv.loc[
            :, tuple(self.measurement_cols)
        ].dropna(axis=1, how="all").columns
        device_headers: list[DeviceHeaderRecord] = [
            {
                "device_key": self.name,
                "header": header,
                "flag": (
                    f"{header}_flag" if f"{header}_flag"
                    in self.flag_cols else None
                )
            }
            for header in measurement_headers
        ]
        yield from device_headers

    @property
    def reference_headers(self) -> Generator[DeviceHeaderRecord]:
        """Set of records representing reference headers.

        Represents an iterator containing dictionaries with the following keys:

        - **device_key** : str
        - **header** : str
        - **flag** : None
        """
        _logger.info("Querying reference measurements for %s", self.name)
        measurement_match = re.compile(r"Ref\.(?:(?:NO)|(?:CO)|(?:O)|(?:PM))")
        city_match = {
            "ANT": "ANT",
            "VIT": "ANT",
            "OSL": "OSL",
            "ZAG": "ZAG",
            "ISP": "ISP"
        }
        csv_subset = self.csv.loc[
            :, (*self.reference_cols, self.location_col)
        ].dropna(subset=self.location_col)
        device_headers: list[DeviceHeaderRecord] = []
        for loc, sub_df in csv_subset.groupby(self.location_col):
            sub_df_headers = (
                    set(sub_df.dropna(axis=1, how="all").columns) -
                    {self.location_col, }
            )
            device_headers.extend([
                {
                    "device_key": loc,
                    "header": (
                        f"{header.replace(".", "_")}_{city_match[loc[:3]]}"
                        if re.match(measurement_match, header) else
                        header.replace(".", "_")
                    ),
                    "flag": None
                }
                for header in sub_df_headers
            ])
        yield from device_headers


def get_header_records() -> Generator[HeaderRecord]:
    """Return contents of header json file.

    Yields
    ------
    HeaderRecord representing a header within the dataset.

    """
    _logger.info("Querying measurement header information")
    header_file = files("senseurcity.files.json").joinpath("header.json")
    with header_file.open("r") as header_json:
        header_info = json.load(header_json)
    for json_l in header_info:
        record: HeaderRecord = {
            "header": json_l.pop("header"),
            "parameter": json_l.pop("parameter"),
            "unit": json_l.pop("unit"),
            "other": {}
        }
        record["other"] = json_l
        yield record


def get_device_records() -> Generator[DeviceRecord]:
    """Return contents of device json file.

    Yields
    ------
    DeviceRecord representing a device within the dataset.

    """
    _logger.info("Querying device information")
    device_file = files("senseurcity.files.json").joinpath("devices.json")
    with device_file.open("r") as device_json:
        device_info = json.load(device_json)
    for json_l in device_info:
        record: DeviceRecord = {
            "key": json_l.pop("key"),
            "name": json_l.pop("name"),
            "short_name": json_l.pop("short_name"),
            "dataset": "SensEURCity",
            "reference": json_l.pop("reference", False),
            "other": {}
        }
        record["other"] = json_l
        yield record


def get_unit_conversion_records() -> Generator[ConversionRecord]:
    """Return contents of unit conversion json file.

    Yields
    ------
    ConversionRecord representing a unit conversion.

    """
    _logger.info("Querying unit conversion")
    conversion_file = (
            files("senseurcity.files.json").joinpath("conversion.json")
    )
    with conversion_file.open("r") as conversion_json:
        conversion_info = json.load(conversion_json)
    yield from conversion_info

