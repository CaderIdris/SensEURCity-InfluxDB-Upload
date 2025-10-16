from collections.abc import Generator
from dataclasses import dataclass
import datetime as dt
import hashlib
import re
from typing import Self, TypedDict

import numpy as np
import pandas as pd


class MeasurementsRecord(TypedDict):
    point_hash: str
    timestamp: dt.datetime
    code: str


class ValuesRecord(TypedDict):
    point_hash: str
    header: str
    value: float


class FlagsRecord(TypedDict):
    point_hash: str
    flag: str
    value: str


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
    date_name: str
    location_name: str

    @classmethod
    def from_dataframe(
        cls,
        name: str,
        csv: pd.DataFrame,
        date_name: str = "date",
        location_name: str = "Location.ID"
    ) -> Self:
        """"""

        if date_name not in csv.columns:
            err_msg = (
                f"'{date_name}' is not present in {name}.csv. Expected a "
                "valid name for the date column."
            )
            raise ValueError(err_msg)

        if location_name not in csv.columns:
            err_msg = (
                f"'{location_name}' is not present in {name}.csv. Expected a "
                "valid name for the location column."
            )
            raise ValueError(err_msg)

        reference_cols = {
            col for col in csv.columns
            if "Ref." == col[:4]
        } | {location_name}

        flag_cols = {
            col for col in csv.columns
            if "_flag" == col[-5:]
        } | {location_name}

        measurement_cols = (
            set(csv.columns) -
            reference_cols -
            flag_cols -
            {date_name}
        )

        csv[date_name] = pd.to_datetime(csv[date_name])
        csv["point_hash"] = [
            hashlib.sha1(
                f"{name}{ts.timestamp()}".encode('utf-8'),
                usedforsecurity=False
            ).hexdigest()
            for ts in csv[date_name].dt.to_pydatetime()
        ]
        csv["ref_point_hash"] = [
            hashlib.sha1(
                f"{row[1][location_name]}"
                f"{row[1][date_name].to_pydatetime().timestamp()}"
                .encode('utf-8'),
                usedforsecurity=False
            ).hexdigest() 
            if row[1][location_name] == row[1][location_name]
            else np.nan
            for row in csv.iterrows()
        ]
        csv = csv.set_index(date_name)

        return cls(
            name=name,
            csv=csv,
            measurement_cols=measurement_cols,
            flag_cols=flag_cols,
            reference_cols=reference_cols,
            date_name=date_name,
            location_name=location_name
        )
    
    @property
    def measurements(self) -> Generator[MeasurementsRecord]:
        """"""
        csv_subset = self.csv.loc[:, ["point_hash"]]
        csv_subset["timestamp"] = csv_subset.index
        csv_subset["code"] = self.name

        for record in csv_subset.to_dict('records'):
            yield record

    @property
    def values(self) -> Generator[ValuesRecord]:
        """"""
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
        """"""
        csv_subset = self.csv.loc[:, (*self.flag_cols, "point_hash")]
        csv_subset = csv_subset.rename(
            {self.location_name: "Collocation"},
            axis=1
        )
        csv_subset = csv_subset.melt(
            var_name="flag",
            value_name="value",
            id_vars="point_hash"
        ).dropna(subset="value")

        for record in csv_subset.to_dict('records'):
            yield record

    @property
    def reference_measurements(self) -> Generator[MeasurementsRecord]:
        csv_subset = (
            self.csv
            .loc[:, [self.location_name, "point_hash"]]
            .dropna(
                subset=self.location_name
            )
            .rename({
                self.location_name: "code",
                "ref_point_hash": "point_hash"
            }, axis=1)
        )
        csv_subset["timestamp"] = csv_subset.index

        for record in csv_subset.to_dict('records'):
            yield record

    @property
    def reference_values(self) -> Generator[ValuesRecord]:
        """"""
        measurement_match = re.compile(r"Ref\.(?:(?:NO)|(?:CO)|(?:O)|(?:PM))")
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
            id_vars=("point_hash", self.location_name)
        ).dropna(subset="value")

        csv_subset["header"] = csv_subset.apply(
            lambda x: (
                    f"{x['header'].replace('.', '_')}_{city_match[x[self.location_name][:3]]}"
                    if re.match(measurement_match, x['header']) else
                    x['header'].replace('.', '_')
            ),
            axis=1
        )
        csv_subset = csv_subset.drop(self.location_name, axis=1)

        for record in csv_subset.to_dict('records'):
            yield record
    
