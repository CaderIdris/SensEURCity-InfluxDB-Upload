import io
import logging
from pathlib import Path
import zipfile

import pytest
import requests

from senseurcity.zipped import Cities, download_data, get_csvs


@pytest.fixture(scope="session")
def empty_mock_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create an empty zip file."""
    zip_dir = tmp_path_factory.mktemp("empty_senseurcity_data")
    zip_path = zip_dir / "SensEURCity.zip"
    with zipfile.ZipFile(zip_path, "a") as zip_file:
        zip_file.mkdir("senseurcity_data_v01/")
        zip_file.mkdir("senseurcity_data_v01/dataset")
    return zip_path


@pytest.fixture(scope="session")
def double_dataset_mock_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create an empty zip file."""
    zip_dir = tmp_path_factory.mktemp("empty_senseurcity_data")
    zip_path = zip_dir / "SensEURCity.zip"
    with zipfile.ZipFile(zip_path, "a") as zip_file:
        zip_file.mkdir("senseurcity_data_v01/")
        zip_file.mkdir("senseurcity_data_v02/")
    return zip_path


def mock_data_list():
    """List of files and their contents for mock purposes."""
    return [
        ("a.txt", 'testing a'),
        ("b.txt", 'testing b'),
        ("c.txt", 'testing c'),
        ("d.txt", 'testing d'),
        ("e.txt", 'testing e'),
    ]

def make_mock_zip_file():
    """Make a zip file with the data from `mock_data_list` function."""
    mock_data = mock_data_list()
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "a") as zip_file:
        for name, data in mock_data:
            zip_file.writestr(name, data)
    return zip_buffer.getvalue()


class MockResponseValid:
    """Mock class for a good response from an API."""
    @property
    def status_code(self):
        """If the response is good, status_code should be 200."""
        return 200

    @property
    def content(self):
        """Return the content from the `make_mock_zip_file` function."""
        mock_zip = make_mock_zip_file()
        return mock_zip

    def raise_for_status(self):
        """The function being tested uses the `raise_for_status` function.

        This raises an error if a non-200 status code is encountered. However,
        this is a mock for a good response so we just return None.
        """
        return None


class MockResponseBad:
    """Mock class for a bad response from an API."""
    @property
    def status_code(self):
        """One of many bad status codes."""
        return 404

    @property
    def content(self):
        """Won't return anything as it's bad."""
        return None

    def raise_for_status(self):
        """If a bad status code is encountered, a HTTP error will be raised."""
        raise requests.exceptions.HTTPError(
            "404 Client Error"
        )


@pytest.mark.zipfile
def test_download_zip(monkeypatch: pytest.MonkeyPatch, data_path: Path):
    """Test downloading a zip file using the `download_data` function.

    This test does not download any actual data, it uses a mock response
    object instead.

    Tests
    -----
    - Is the file saved in the correct path.
    - Does the file exist.
    - Are the files all present?
    - Are there the correct number of files?
    - Do the files have content?
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
def test_overwrite_protection(
    monkeypatch: pytest.MonkeyPatch,
    empty_mock_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    """Test overwrite protection.

    Tests
    -----
    - Is the file skipped?
    """
    tests = {}

    def mock_get(*args, **kwargs):
        return MockResponseValid()
    
    monkeypatch.setattr(requests, "get", mock_get)

    with caplog.at_level(logging.DEBUG):
        _ = download_data("http://fakeurl", empty_mock_path)

    tests["Skip is logged"] = (
        "File already exists, skipping download." in caplog.text
    )

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
    """Test a failed download using the `download_data` function.

    This test mocks a failed download without trying to download anything.

    Tests
    -----
    - Nothing returned.
    - Error is captured.
    - The error is logged to the console.
    """
    tests = {}
    zip_path = data_path / "test.zip"

    def mock_get(*args, **kwargs):
        return MockResponseBad()
    
    monkeypatch.setattr(requests, "get", mock_get)

    with caplog.at_level(logging.ERROR):
        res = download_data("http://fakeurl", zip_path, ignore_file_exists=True)

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
    """Test the `ZipFile` class on a mock zip file.

    Tests
    -----
    - Correct number of csvs present for each city.
    """
    tests = {}
    with zipfile.ZipFile(sec_mock_path) as sec_zip:
        dfs = list(get_csvs(sec_zip, valid_cities[0]))
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
    """Test giving an invalid value for `city` argument.

    Tests
    -----
    - ValueError raised with expected message.
    """
    with pytest.raises(
        ValueError,
        match="Unexpected city given. Please choose from"
    ):
        with zipfile.ZipFile(sec_mock_path) as sec_zip:
            next(get_csvs(sec_zip, Cities(0)))


@pytest.mark.zipfile
def test_multiple_city_fail(
    sec_mock_path: Path
):
    """Test giving multiple cities for `city` argument.

    Tests
    -----
    - ValueError raised with expected message.
    """
    with pytest.raises(
        ValueError,
        match="Multiple cities given. Please choose one."
    ):
        with zipfile.ZipFile(sec_mock_path) as sec_zip:
            next(get_csvs(sec_zip, Cities.Antwerp | Cities.Zagreb))


@pytest.mark.zipfile
def test_bad_zip(
        data_path: Path
):
    """Test using a zip file with an unexpected structure.

    Tests
    -----
    - FileNotFoundError raised with expected message.
    """
    zip_path = data_path / "test.zip"
    with pytest.raises(
        FileNotFoundError,
        match="'dataset' folder missing from provided zip file. "
    ):
        with zipfile.ZipFile(zip_path) as sec_zip:
            next(get_csvs(sec_zip, Cities.Antwerp))


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
    """Test an empty zip file with a dataset folder present.
    
    Tests
    -----
    - Warning is logged that no csv files could be found for each city.
    """
    tests = {}
    with caplog.at_level(logging.WARNING):
        with zipfile.ZipFile(empty_mock_path) as sec_zip:
            list(get_csvs(sec_zip, valid_cities[0]))

    tests["Issue is logged"] = (
        f"No csv files found for {valid_cities[1]}" in caplog.text
    )

    for test, result in tests.items():
        if not result:
            print(f"{test}: {result}")

    assert all(tests.values())


def test_double_dataset_zip(
    double_dataset_mock_path: Path
):
    """"""
    with pytest.raises(
        ValueError,
        match="Zipfile contains multiple datasets: "
    ):
        with zipfile.ZipFile(double_dataset_mock_path) as sec_zip:
            list(get_csvs(sec_zip, Cities.Antwerp))
