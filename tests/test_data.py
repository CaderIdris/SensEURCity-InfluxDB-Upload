import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from senseurcity.data import (
    SensEURCityCSV,
    get_device_records,
    get_header_records,
    get_unit_conversion_records
)


def file_paths() -> list[Path]:
    """Get paths of all test csvs."""
    return list(
        Path("./tests/test_zipped").glob("*.csv")
    )


@pytest.fixture
def colocation_dataset() -> tuple[
    pd.DataFrame,
    tuple[tuple[str, str, dt.datetime, dt.datetime], ...]
]:
    """Generate a test dataset for the colocation test."""
    locations = (
        (["A"] * 300) +
        ([np.nan] * 98) +
        (["B"] * 200) +
        (["A"] * 100) +
        ([np.nan] * 300) +
        ["A", np.nan] +
        (["C"] * 100)
    )
    test_df = pd.DataFrame(
        locations,
        columns=["Location.ID"]
    )
    test_df["date"] = pd.date_range(
        dt.datetime(2020, 1, 1),
        periods=test_df.shape[0],
        freq="1h"
    )
    expected_values = (
        (
            "Test",
            "A",
            dt.datetime(2020, 1, 1),
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=299),
        ),
        (
            "Test",
            "B",
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=398),
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=597)
        ),
        (
            "Test",
            "A",
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=598),
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=697)
        ),
        (
            "Test",
            "A",
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=998),
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=998)
        ),
        (
            "Test",
            "C",
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=1000),
            dt.datetime(2020, 1, 1) + dt.timedelta(hours=1099)
        )
    )
    return test_df, expected_values


@pytest.mark.data
@pytest.mark.parametrize("csv_path", file_paths())
def test_read_zip(csv_path) -> None:
    """Read each mock csv.

    Tests
    -----
    - Dataclass sets correct name.
    - Dataframe in csv attribute has expected shape.
    - Measurement cols is not empty.
    - Flag cols is not empty.
    - Ref cols is not empty.
    - No overlap between measurement and ref cols
    - No overlap between measurement and flag cols
    - No overlap between ref and flag cols
    """
    tests = {}
    sensor_name = csv_path.name[:-4]

    csv_to_test = pd.read_csv(csv_path)
    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=sensor_name,
        csv=csv_to_test.copy()
    )

    tests["Correct name"] = csv_dataclass.name == sensor_name

    tests["csv is correct shape"] = csv_dataclass.csv.shape == (
        csv_to_test.shape[0],
        csv_to_test.shape[1] - 1
    )

    tests["Measurement cols have values"] = len(csv_dataclass.measurement_cols) > 0
    tests["Flag cols have values"] = len(csv_dataclass.flag_cols) > 0
    tests["Ref cols have values"] = len(csv_dataclass.reference_cols) > 0

    tests["No overlap: measurement_cols + ref_cols"] = not len(
            csv_dataclass.reference_cols & csv_dataclass.measurement_cols
    )
    tests["No overlap: measurement_cols + flag_cols"] = not len(
            csv_dataclass.flag_cols & csv_dataclass.measurement_cols
    )
    tests["No overlap: flag_cols + ref_cols"] = not len(
            csv_dataclass.flag_cols & csv_dataclass.reference_cols
    )

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())


@pytest.mark.data
@pytest.mark.parametrize(
    "bad_kwarg",
    [
        {"date_col": "BAD"},
        {"location_col": "BAD"}
    ]
)
def test_bad_column_names(bad_kwarg) -> None:
    """Test giving a bad argument for date or location col.

    Tests
    -----
    - ValueError raised with expected message.
    """
    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    match_str = (
        f"'BAD' is not present in {csv_path.name}. Expected a valid name for "
        f"the {next(iter(bad_kwarg.keys())).split('_')[0]} column."

    )
    csv = pd.read_csv(csv_path)
    with pytest.raises(
        ValueError,
        match=match_str
    ):
        _ = SensEURCityCSV.from_dataframe(
            name=csv_path.name[:-4],
            csv=csv.copy(),
            **bad_kwarg
        )


@pytest.mark.data
def test_get_measurements() -> None:
    """Tests whether the measurements are parsed from the file.

    Tests
    -----
    - Correct number of columns in returned data.
    - Expected columns are present.
    - No duplicated point_hash values.
    - No duplicated timestamp values.
    """
    tests = {}

    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    csv = pd.read_csv(csv_path)

    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    records = list(csv_dataclass.measurements)
    df = pd.DataFrame(records)

    tests["Correct num of cols"] = df.shape[1] == 5
    tests["No duplicated timestamp values"] = df["time"].is_unique

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())


@pytest.mark.data
def test_get_ref_measurements() -> None:
    """Tests whether the reference measurements are parsed.

    Tests
    -----
    - Correct number of columns in returned data.
    - Expected columns are present.
    - No duplicated point_hash values.
    - No duplicated timestamp values.
    """
    tests = {}

    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    csv = pd.read_csv(csv_path)

    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    records = list(csv_dataclass.reference_measurements)
    df = pd.DataFrame(records)

    tests["Correct num of cols"] = df.shape[1] == 5
    tests["No duplicated timestamp values"] = df["time"].is_unique

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())

@pytest.mark.data
def test_colocation(
    colocation_dataset: tuple[
        pd.DataFrame,
        tuple[tuple[str, str, dt.datetime, dt.datetime], ...]
    ]) -> None:
    """Test the colocation dataset.

    This is more complicated so uses a bespoke dataframe to test.

    Tests
    -----
    - All co-located records returned are as expected.
    - *The correct number being returned is technically also tested \
    with the strict argument for zip set to True*
    """
    tests = {}
    csv_dataclass = SensEURCityCSV.from_dataframe(
        name="Test",
        csv=colocation_dataset[0]
    )
    coloc = [
        (
            i["device_key"],
            i["other_key"],
            i["start_date"].to_pydatetime(),
            i["end_date"].to_pydatetime(),
        )
        for i in csv_dataclass.colocation
    ]

    for num, (generated, expected) in enumerate(
        zip(coloc, colocation_dataset[1], strict=True),
        start=1
    ):
        tests[f"{num} correct"] = generated == expected

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())

@pytest.mark.data
def test_get_device_headers() -> None:
    """Tests whether the device headers are parsed

    Tests
    -----
    - Correct number of columns in returned data.
    - Expected columns are present.
    - No duplicated point_hash values.
    - No duplicated timestamp values.
    """
    tests = {}

    csv_path = Path(f"./tests/test_zipped/Antwerp_402B00.csv")
    csv = pd.read_csv(csv_path)

    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    records = list(csv_dataclass.device_headers)
    df = pd.DataFrame(records)
    print(df)


    tests["Correct num of headers"] = df.shape[0] == 18

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())


@pytest.mark.data
def test_get_ref_headers() -> None:
    """Tests whether the device headers are parsed

    Tests
    -----
    - Correct number of columns in returned data.
    - Expected columns are present.
    - No duplicated point_hash values.
    - No duplicated timestamp values.
    """
    tests = {}

    csv_path = Path(f"./tests/test_zipped/Antwerp_402B00.csv")
    csv = pd.read_csv(csv_path)

    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    records = list(csv_dataclass.reference_headers)
    df = pd.DataFrame(records)
    r801 = df[df["device_key"] == "ANT_REF_R801"]
    r802 = df[df["device_key"] == "ANT_REF_R802"]

    tests["Correct num of headers (R801)"] = r801.shape[0] == 6
    tests["Correct num of headers (R802)"] = r802.shape[0] == 1

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())

@pytest.mark.data
def test_import_headers() -> None:
    """Test importing the headers json."""
    tests = {}
    headers = list(get_header_records())
    for i, header in enumerate(headers, start=1):
        tests[f"header in {i}"] = "header" in header
        tests[f"parameter in {i}"] = "parameter" in header
        tests[f"unit in {i}"] = "unit" in header
        tests[f"other in {i}"] = "other" in header

    tests["Correct number of headers"] = len(headers) == 97

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())


@pytest.mark.data
def test_import_devices() -> None:
    """Test importing the devices json."""
    tests = {}
    devices = list(get_device_records())
    for i, device in enumerate(devices, start=1):
        tests[f"key in {i}"] = "key" in device
        tests[f"name in {i}"] = "name" in device
        tests[f"short_name in {i}"] = "short_name" in device
        tests[f"dataset in {i}"] = "dataset" in device
        tests[f"reference in {i}"] = "reference" in device
    tests["Correct number of devices"] = len(devices) == 164

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())


@pytest.mark.data
def test_import_unit_conversion() -> None:
    """Test importing the unit conversion json."""
    tests = {}
    conversions = list(get_unit_conversion_records())
    for i, conversion in enumerate(conversions, start=1):
        tests[f"unit_in in {i}"] = "unit_in" in conversion
        tests[f"unit_out in {i}"] = "unit_out" in conversion
        tests[f"parameter in {i}"] = "parameter" in conversion
        tests[f"scale in {i}"] = "scale" in conversion

    tests["Correct number of devices"] = len(conversions) == 3

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())


@pytest.mark.data
@pytest.mark.parametrize(
    "loc_id",
    [
        "   This_is_bad    ",
        "This/is/bad",
        "This-is-bad",
        "   This/is/bad    ",
        "   This-is-bad    "
    ]
)
def test_location_id_format(loc_id) -> None:
    """Tests whether the Location.ID is properly parsed.

    Tests
    -----
    - Correct parsed location id column
    """
    tests = {}

    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    csv = pd.read_csv(csv_path)
    csv["Location.ID"] = loc_id

    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    parsed_csv = csv_dataclass.csv

    tests["Correct num of cols"] = (
            set(parsed_csv["Location.ID"].unique()) == {'This_is_bad',}
    )

    for result in tests.values():
        if not result:
            pass

    assert all(tests.values())
