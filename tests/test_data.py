from pathlib import Path

import pandas as pd
import pytest

from senseurcity.data import SensEURCityCSV


def file_paths() -> list[Path]:
    return list(
        Path("./tests/test_zipped").glob("*.csv")
    )


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
        {"date_name": "BAD"},
        {"location_name": "BAD"}
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
    for col_name in ("point_hash", "timestamp", "code"):
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
    for col_name in ("point_hash", "timestamp", "code"):
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

    tests["No . in headers"] = not df["header"].str.contains(".", regex=False).any()

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


