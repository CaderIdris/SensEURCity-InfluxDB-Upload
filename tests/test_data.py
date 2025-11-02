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
def test_read_zip(csv_path):
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
        csv_to_test.shape[1] + 1
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

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
@pytest.mark.parametrize(
    "bad_kwarg",
    [
        {"date_col": "BAD"},
        {"location_col": "BAD"}
    ]
)
def test_bad_column_names(bad_kwarg):
    """Test giving a bad argument for date or location col.

    Tests
    -----
    - ValueError raised with expected message.
    """
    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    match_str = (
        f"'BAD' is not present in {csv_path.name}. Expected a valid name for "
        f"the {tuple(bad_kwarg.keys())[0].split('_')[0]} column."

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
@pytest.mark.parametrize(
    "bad_kwarg",
    ["date_col", "location_col"]
)
@pytest.mark.parametrize(
    "bad_name",
    ["point_hash", "ref_point_hash"]
)
def test_protected_column_name(bad_kwarg, bad_name):
    """Tests whether a protected column name is given for date or location col.

    Honestly, this should never happen but better safe than sorry right?

    Tests
    -----
    - ValueError is raised with expected message

    """
    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    match_str = (
        f"{bad_kwarg} cannot be {bad_name}, this is a protected name."

    )
    kwarg = {bad_kwarg: bad_name}
    csv = pd.read_csv(csv_path)
    csv[bad_name] = None
    with pytest.raises(
        ValueError,
        match=match_str
    ):
        _ = SensEURCityCSV.from_dataframe(
            name=csv_path.name[:-4],
            csv=csv.copy(),
            **kwarg
        )


@pytest.mark.data
def test_get_measurements():
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

    tests["Correct num of cols"] = df.shape[1] == 3
    for col_name in ("point_hash", "timestamp", "device_key"):
        tests[f"{col_name} in df"] = col_name in df.columns

    tests["No duplicated point_hash values"] = df["point_hash"].is_unique
    tests["No duplicated timestamp values"] = df["timestamp"].is_unique

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
def test_get_values():
    """Test getting all measurement values.

    Tests
    -----
    - Correct number of columns.
    - Expected columns present.
    - No unexpected headers.
    - No '.' characters in the headers.
    """
    tests = {}

    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    csv = pd.read_csv(csv_path)
    
    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    records = list(csv_dataclass.values)
    df = pd.DataFrame(records)

    tests["Correct num of cols"] = df.shape[1] == 3
    for col_name in ("point_hash", "header", "value"):
        tests[f"{col_name} in df"] = col_name in df.columns

    tests["No extra headers"] = len(
        set(df["header"].unique()) -
        csv_dataclass.measurement_cols
    ) == 0

    tests["No . in headers"] = not df["header"].str.contains(".", regex=False).any()

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
def test_get_flags():
    """Test getting all measurement flags.

    Tests
    -----
    - Correct number of columns.
    - Expected columns present.
    - No unexpected flags.
    - No '.' characters in the flags.
    """
    tests = {}

    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    csv = pd.read_csv(csv_path)
    
    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    records = list(csv_dataclass.flags)
    df = pd.DataFrame(records)

    tests["Correct num of cols"] = df.shape[1] == 3
    for col_name in ("point_hash", "flag", "value"):
        tests[f"{col_name} in df"] = col_name in df.columns

    tests["No extra flags"] = len(
        set(df["flag"].unique()) -
        csv_dataclass.flag_cols -
        {"Collocation"}
    ) == 0

    tests["No . in flags"] = not df["flag"].str.contains(".", regex=False).any()

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
def test_hashes_match():
    """Test all hashes in flags and values match measurement hashes.

    Tests
    -----
    - No hashes in flags that aren't in measurements.
    - No hashes in values that aren't in measurements.

    """
    tests = {}

    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
    csv = pd.read_csv(csv_path)
    
    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    measurement_records = list(csv_dataclass.measurements)
    flag_records = list(csv_dataclass.flags)
    value_records = list(csv_dataclass.values)

    measurement_hashes = set(pd.DataFrame(measurement_records)["point_hash"])
    flag_hashes = set(pd.DataFrame(flag_records)["point_hash"])
    value_hashes = set(pd.DataFrame(value_records)["point_hash"])

    tests["No wrong flag hashes"] = len(flag_hashes - measurement_hashes) == 0
    tests["No wrong value hashes"] = len(
        value_hashes - measurement_hashes
    ) == 0

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
def test_get_ref_measurements():
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

    tests["Correct num of cols"] = df.shape[1] == 3
    for col_name in ("point_hash", "timestamp", "device_key"):
        tests[f"{col_name} in df"] = col_name in df.columns

    tests["No duplicated point_hash values"] = df["point_hash"].is_unique
    tests["No duplicated timestamp values"] = df["timestamp"].is_unique

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
@pytest.mark.parametrize(
    "file",
    [
        ("Antwerp_402B00", "ANT"),
        ("Antwerp_402B01", "ANT"),
        ("Oslo_64A291", "OSL"),
        ("Zagreb_64C52B", "ZAG"),
    ]
)
def test_get_ref_values(file):
    """Test getting all measurement values.

    Tests
    -----
    - Correct number of columns.
    - Expected columns present.
    - No unexpected headers.
    - No '.' characters in the headers.
    """
    filename, city_prefix = file
    tests = {}

    csv_path = Path(f"./tests/test_zipped/{filename}.csv")
    csv = pd.read_csv(csv_path)
    
    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    records = list(csv_dataclass.reference_values)
    df = pd.DataFrame(records)

    tests["Correct num of cols"] = df.shape[1] == 3
    for col_name in ("point_hash", "header", "value"):
        tests[f"{col_name} in df"] = col_name in df.columns

    tests["No extra headers"] = len(
        set(df["header"].unique()) -
        {
            f"Ref_NO_{city_prefix}",
            f"Ref_NO2_{city_prefix}",
            f"Ref_O3_{city_prefix}",
            f"Ref_CO_ppm_{city_prefix}",
            f"Ref_PM2_5_{city_prefix}",
            f"Ref_PM10_{city_prefix}",
            f"Ref_PM2_5_Fidas_{city_prefix}",
            f"Ref_PM4_Fidas_{city_prefix}",
            f"Ref_PM10_Fidas_{city_prefix}",
            f"Ref_PMtot_Fidas_{city_prefix}",
            f"Ref_PM1_Fidas_{city_prefix}",
            f"Ref_PM1_{city_prefix}",
            "Ref_Press",
            "Ref_Lat",
            "Ref_Long",
            "Ref_Temp",
            "Ref_RH"
        }
    ) == 0

    tests["No . in headers"] = (
            not df["header"].str.contains(".", regex=False).any()
    )

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
@pytest.mark.parametrize(
    "file",
    [
        ("Zagreb_64C52B", "ZAG")
    ]
)
def test_get_ref_values_no_dupes(file):
    """Test getting all measurement values.

    Tests
    -----
    - Correct number of columns.
    - Expected columns present.
    - No unexpected headers.
    - No '.' characters in the headers.
    """
    filename, city_prefix = file
    tests = {}

    csv_path = Path(f"./tests/test_zipped/{filename}.csv")
    csv = pd.read_csv(csv_path)
    
    csv_dataclass = SensEURCityCSV.from_dataframe(
        name=csv_path.name[:-4],
        csv=csv.copy()
    )
    records = list(csv_dataclass.reference_values)
    df = pd.DataFrame(records)

    tests["Dupes removed"] = df.shape[0] == 2
    assert all(tests.values())

@pytest.mark.data
def test_colocation(
    colocation_dataset: tuple[
        pd.DataFrame,
        tuple[tuple[str, str, dt.datetime, dt.datetime], ...]
    ]):
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

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
def test_import_headers():
    """Test importing the headers json."""
    tests = {}
    headers = list(get_header_records())
    for i, header in enumerate(headers, start=1):
        tests[f"header in {i}"] = "header" in header
        tests[f"parameter in {i}"] = "parameter" in header
        tests[f"unit in {i}"] = "unit" in header
        tests[f"other in {i}"] = "other" in header 

    tests["Correct number of headers"] = len(headers) == 95

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
def test_import_devices():
    """Test importing the devices json."""
    tests = {}
    devices = list(get_device_records())
    for i, device in enumerate(devices, start=1):
        tests[f"key in {i}"] = "key" in device
        tests[f"name in {i}"] = "name" in device
        tests[f"short_name in {i}"] = "short_name" in device
        tests[f"dataset in {i}"] = "dataset" in device
        tests[f"reference in {i}"] = "reference" in device

    tests["Correct number of devices"] = len(devices) == 109

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
def test_import_unit_conversion():
    """Test importing the unit conversion json."""
    tests = {}
    conversions = list(get_unit_conversion_records())
    for i, conversion in enumerate(conversions, start=1):
        tests[f"unit_in in {i}"] = "unit_in" in conversion
        tests[f"unit_out in {i}"] = "unit_out" in conversion
        tests[f"parameter in {i}"] = "parameter" in conversion
        tests[f"scale in {i}"] = "scale" in conversion

    tests["Correct number of devices"] = len(conversions) == 3

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())
