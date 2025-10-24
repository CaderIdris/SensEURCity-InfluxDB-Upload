from collections.abc import Generator
from dataclasses import dataclass
import datetime as dt
import hashlib
import re
from typing import Self, TypedDict

import numpy as np
import pandas as pd


class MeasurementsRecord(TypedDict):
    """Expected structure of a dictionary returning a single measurement
    period.

    Attributes
    ------
    point_hash : str
        A hash of the timestamp and sensor name.
    timestamp : dt.datetime
        The timestamp of the measurement.
    device_key : str
        The sensor name.
    """
    point_hash: str
    timestamp: dt.datetime
    device_key: str


class ValuesRecord(TypedDict):
    """Expected structure of a dictionary returning a single value from a 
    single measurement period.

    Attributes
    ------
    point_hash : str
        A hash of the timestamp and sensor name.
    header : str
        The measurement header.
    device_key : float
        The value of the measurement.
    """
    point_hash: str
    header: str
    value: float


class FlagsRecord(TypedDict):
    """Expected structure of a dictionary returning a single flag from a 
    single measurement period.

    Attributes
    ------
    point_hash : str
        A hash of the timestamp and sensor name.
    flag : str
        The flag name.
    device_key : float
        The value of the flag.
    """
    point_hash: str
    flag: str
    value: str


class ColocationRecord(TypedDict):
    """Expected structure of a dictionary returning a single co-location
    period.

    Attributes
    ------
    device_key : str
        The co-located device
    other_key : str
        The device it was co-located with
    start_date : pd.Timestamp
        The start of the co-location (inclusive)
    end_date : pd.Timestamp
        The end of the co-location (inclusive)
    """
    device_key: str
    other_key: str
    start_date: pd.Timestamp
    end_date: pd.Timestamp


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
        location_col: str = "Location.ID"
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

        if date_col in ("point_hash", "ref_point_hash"):
            err_msg = (
                f"date_col cannot be {date_col}, this is a protected name."
            )
            raise ValueError(err_msg)

        if location_col not in csv.columns:
            err_msg = (
                f"'{location_col}' is not present in {name}.csv. Expected a "
                "valid name for the location column."
            )
            raise ValueError(err_msg)

        if location_col in ("point_hash", "ref_point_hash"):
            err_msg = (
                f"location_col cannot be {location_col}, this is a protected name."
            )
            raise ValueError(err_msg)

        # Split columns into reference and flag columns
        reference_cols = {
            col for col in csv.columns
            if "Ref." == col[:4]
        } | {location_col}

        flag_cols = {
            col for col in csv.columns
            if "_flag" == col[-5:]
        }

        # Determine measurement columns from what's left
        measurement_cols = (
            set(csv.columns) -
            reference_cols -
            flag_cols -
            {date_col}
        )
        # Parse date and calculate hash columns
        csv[date_col] = pd.to_datetime(csv[date_col])
        csv["point_hash"] = [
            hashlib.sha1(
                f"{name}{ts.timestamp()}".encode('utf-8'),
                usedforsecurity=False
            ).hexdigest()
            for ts in csv[date_col].dt.to_pydatetime()
        ]
        csv["ref_point_hash"] = [
            hashlib.sha1(
                f"{row[1][location_col]}"
                f"{row[1][date_col].to_pydatetime().timestamp()}"
                .encode('utf-8'),
                usedforsecurity=False
            ).hexdigest() 
            if row[1][location_col] == row[1][location_col]
            else np.nan
            for row in csv.iterrows()
        ]
        csv = csv.set_index(date_col)
        csv.index.name = "date"

        return cls(
            name=name,
            csv=csv,
            measurement_cols=measurement_cols,
            flag_cols=flag_cols,
            reference_cols=reference_cols,
            date_col=date_col,
            location_col=location_col
        )
    
    @property
    def measurements(self) -> Generator[MeasurementsRecord]:
        """Set of records representing measurement intervals of a single
        device.

        Represents an iterator containing dictionaries with the following keys:

        - **point_hash** : str
        - **timestamp** : dt.datetime
        - **device_key** : str
        """
        csv_subset = self.csv.loc[:, ["point_hash"]]
        csv_subset["timestamp"] = csv_subset.index
        csv_subset["device_key"] = self.name

        for record in csv_subset.to_dict('records'):
            yield record

    @property
    def values(self) -> Generator[ValuesRecord]:
        """Set of records representing a single measurement at a single
        interval of a single device.

        Represents an iterator containing dictionaries with the following keys:

        - **point_hash** : str
        - **header** : str
        - **value** : float
        """
        csv_subset = self.csv.loc[:, (*self.measurement_cols, "point_hash")]
        csv_subset = csv_subset.melt(
            var_name="header",
            value_name="value",
            id_vars="point_hash"
        ).dropna(subset="value")
        csv_subset["header"] = csv_subset["header"].str.replace('.', '_')
        for record in csv_subset.to_dict('records'):
            yield record
    
    @property
    def flags(self) -> Generator[FlagsRecord]:
        """Set of records representing a single measurement flag at a single
        interval of a single device.

        Represents an iterator containing dictionaries with the following keys:

        - **point_hash** : str
        - **flag** : str
        - **value** : str
        """
        csv_subset = self.csv.loc[:, (*self.flag_cols, "point_hash")]
        csv_subset = csv_subset.melt(
            var_name="flag",
            value_name="value",
            id_vars="point_hash"
        ).dropna(subset="value")

        for record in csv_subset.to_dict('records'):
            yield record

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
        for record in grouped.to_dict('records'):
            yield record

    @property
    def reference_measurements(self) -> Generator[MeasurementsRecord]:
        """Set of records representing measurement intervals of a single
        reference device.

        Represents an iterator containing dictionaries with the following keys:

        - **point_hash** : str
        - **timestamp** : dt.datetime
        - **device_key** : str
        """
        csv_subset = (
            self.csv
            .loc[:, [self.location_col, "point_hash"]]
            .dropna(
                subset=self.location_col
            )
            .rename({
                self.location_col: "device_key",
                "ref_point_hash": "point_hash"
            }, axis=1)
        )
        csv_subset["timestamp"] = csv_subset.index

        for record in csv_subset.to_dict('records'):
            yield record

    @property
    def reference_values(self) -> Generator[ValuesRecord]:
        """Set of records representing measurement intervals of a single
        reference device.

        Represents an iterator containing dictionaries with the following keys:

        - **point_hash** : str
        - **timestamp** : dt.datetime
        - **device_key** : str
        """
        measurement_match = re.compile(r"Ref\.(?:(?:NO)|(?:CO)|(?:O)|(?:PM))")
        # One of the Antwerp devices isn't prefixed with ANT, so we need to
        # match this to Antwerp
        city_match = {
            "ANT": "ANT",
            "VIT": "ANT",
            "OSL": "OSL",
            "ZAG": "ZAG",
            "ISP": "ISP"
        }
        csv_subset = (
            self.csv
            .loc[:, [
                *self.reference_cols,
                "ref_point_hash",
            ]]
            .dropna(
                subset="ref_point_hash"
            )
            .rename({"ref_point_hash": "point_hash"}, axis=1)
        )
        csv_subset = csv_subset.melt(
            var_name="header",
            value_name="value",
            id_vars=("point_hash", self.location_col)
        ).dropna(subset="value")

        csv_subset["header"] = csv_subset.apply(
            lambda x: (
                    f"{x['header'].replace('.', '_')}_{city_match[x[self.location_col][:3]]}"
                    if re.match(measurement_match, x['header']) else
                    x['header'].replace('.', '_')
            ),
            axis=1
        )
        csv_subset = csv_subset.drop(self.location_col, axis=1)

        for record in csv_subset.to_dict('records'):
            yield record
    
