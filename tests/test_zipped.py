import io
import logging
from pathlib import Path
import zipfile

import pytest
import requests

from senseurcity.zipped import Cities, download_data, SensEURCityZipFile


@pytest.fixture(scope="session")
def empty_mock_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    zip_dir = tmp_path_factory.mktemp("empty_senseurcity_data")
    zip_path = zip_dir / "SensEURCity.zip"
    with zipfile.ZipFile(zip_path, "a") as zip_file:
        zip_file.mkdir("dataset")
    return zip_path


def mock_data_list():
    return [
        ("a.txt", 'testing a'),
        ("b.txt", 'testing b'),
        ("c.txt", 'testing c'),
        ("d.txt", 'testing d'),
        ("e.txt", 'testing e'),
    ]

def make_mock_zip_file():
    """"""
    mock_data = mock_data_list()
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a") as zip_file:
        for name, data in mock_data:
            zip_file.writestr(name, data)
    return zip_buffer.getvalue()


class MockResponseValid:
    @property
    def status_code(self):
        return 200

    @property
    def content(self):
        mock_zip = make_mock_zip_file()
        return mock_zip

    def raise_for_status(self):
        return None


class MockResponseBad:
    @property
    def status_code(self):
        return 404

    @property
    def content(self):
        return None

    def raise_for_status(self):
        raise requests.exceptions.HTTPError(
            "404 Client Error"
        )


@pytest.mark.zipfile
def test_download_zip(monkeypatch: pytest.MonkeyPatch, data_path: Path):
    """

    """
    tests = {}
    zip_path = data_path / "test.zip"
    mock_file_names = [i[0] for i in mock_data_list()]

    def mock_get(*args, **kwargs):
        return MockResponseValid()
    
    monkeypatch.setattr(requests, "get", mock_get)

    res = download_data("http://fakeurl", zip_path)

    tests["Same path"] = res == zip_path
    tests["Zip exists"] = zip_path.exists()

    with zipfile.ZipFile(zip_path, "r") as zip_file:
        file_info_list = zip_file.infolist()
    tests[f"{len(mock_file_names)} files present"] = (
        len(mock_file_names) == len(file_info_list)
    )
    for file_info in file_info_list:
        filename = file_info.filename
        tests[f"{filename} expected"] = filename in mock_file_names
        tests[f"{filename} not empty"] = file_info.file_size > 0

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.zipfile
def test_bad_download(
    monkeypatch: pytest.MonkeyPatch,
    data_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    """

    """
    tests = {}
    zip_path = data_path / "test.zip"

    def mock_get(*args, **kwargs):
        return MockResponseBad()
    
    monkeypatch.setattr(requests, "get", mock_get)

    with caplog.at_level(logging.ERROR):
        res = download_data("http://fakeurl", zip_path)

    tests["Nothing returns"] = res is None
    tests["Error is logged"] = "HTTP Error: 404 Client Error" in caplog.text

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.zipfile
@pytest.mark.parametrize(
    "valid_cities",
    [
        (Cities.Antwerp, 2),
        (Cities.Oslo, 1),
        (Cities.Zagreb, 3)
    ]
)
def test_senseurcity_zip_class(
    sec_mock_path: Path,
    valid_cities: tuple[Cities, int],
):
    """"""
    tests = {}
    with SensEURCityZipFile(sec_mock_path) as sec_zip:
        dfs = list(sec_zip.get_csvs(valid_cities[0]))
        print(dfs)
        tests["Correct number of csvs"] = len(dfs) == valid_cities[1]

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


@pytest.mark.zipfile
def test_no_such_city_fail(
    sec_mock_path: Path
):
    with pytest.raises(
        ValueError,
        match="Unexpected city given. Please choose from"
    ):
        with SensEURCityZipFile(sec_mock_path) as sec_zip:
            next(sec_zip.get_csvs(Cities(0)))


@pytest.mark.zipfile
def test_multiple_city_fail(
    sec_mock_path: Path
):
    with pytest.raises(
        ValueError,
        match="Multiple cities given. Please choose one."
    ):
        with SensEURCityZipFile(sec_mock_path) as sec_zip:
            next(sec_zip.get_csvs(Cities.Antwerp | Cities.Zagreb))


@pytest.mark.zipfile
def test_bad_zip(
        data_path: Path
):
    """"""
    zip_path = data_path / "test.zip"
    with pytest.raises(
        FileNotFoundError,
        match="'dataset' folder missing from provided zip file. "
    ):
        with SensEURCityZipFile(zip_path) as sec_zip:
            next(sec_zip.get_csvs(Cities.Antwerp))


@pytest.mark.zipfile
@pytest.mark.parametrize(
    "valid_cities",
    [
        (Cities.Antwerp, "Antwerp"),
        (Cities.Oslo, "Oslo"),
        (Cities.Zagreb, "Zagreb")
    ]
)
def test_empty_zip(
    empty_mock_path: Path,
    valid_cities: tuple[Cities, int],
    caplog: pytest.LogCaptureFixture
):
    """"""
    tests = {}
    with caplog.at_level(logging.WARNING):
        with SensEURCityZipFile(empty_mock_path) as sec_zip:
            list(sec_zip.get_csvs(valid_cities[0]))

    tests["Issue is logged"] = (
        f"No csv files found for {valid_cities[1]}" in caplog.text
    )

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())
