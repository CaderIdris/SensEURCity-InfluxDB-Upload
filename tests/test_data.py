import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from senseurcity.data import SensEURCityCSV


def file_paths() -> list[Path]:
    return list(
        Path("./tests/test_zipped").glob("*.csv")
    )


@pytest.fixture
def colocation_dataset() -> tuple[
    pd.DataFrame,
    tuple[dict[str, str|dt.datetime], ...]
]:
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
def test_download_zip(csv_path):
    """

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
    """"""
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
    """"""
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
    """"""
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
    """"""
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
    """"""
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
    """"""
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
    """"""
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
def test_get_ref_values():
    """"""
    tests = {}

    csv_path = Path("./tests/test_zipped/Antwerp_402B00.csv")
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
            "Ref_NO_ANT",
            "Ref_NO2_ANT",
            "Ref_O3_ANT",
            "Ref_CO_ppm_ANT",
            "Ref_Lat",
            "Ref_Long",
            "Ref_Temp",
            "Ref_RH"
        }
    ) == 0
    print(df)

    tests["No . in headers"] = (
            not df["header"].str.contains(".", regex=False).any()
    )

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.data
def test_colocation(
    colocation_dataset: tuple[
        pd.DataFrame,
        tuple[dict[str, str|dt.datetime], ...]
    ]):
    """"""
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

