"""Download, format and store SensEURCity measurements.

Parse measurement data from the SensEURCity project [^article],
Format them and upload to a SQL database.
The dataset can be downloaded from Zenodo [^zenodo]

[^article]: https://www.nature.com/articles/s41597-023-02135-w
[^zenodo]: https://zenodo.org/doi/10.5281/zenodo.7256405
"""

from collections.abc import Generator
from enum import auto, Flag
import logging
from pathlib import Path
from zipfile import Path as ZipPath
from zipfile import ZipFile

import pandas as pd
import requests
import requests.exceptions as rqexc

_logger = logging.getLogger(f'__main__.{__name__}')


class Cities(Flag):
    """Represent the three cities used in the study."""
    Antwerp = auto()
    Oslo = auto()
    Zagreb = auto()


def download_data(
    download_url: str,
    download_path: Path,
    *,
    ignore_file_exists: bool = False
) -> Path | None:
    """Download SensEURCity data from URL and save to specified path.

    Parameters
    ----------
    download_url : str
        Link to the SensEURCity data.
    download_path : Path
        Where to save the data to.
    ignore_file_exists : bool, default=False
        Replace the zip file if it already exists.

    Returns
    -------
    - Path object pointing to downloaded file if successful
    - None if HTTPError encountered
    """
    if download_path.exists() and not ignore_file_exists:
        _logger.info("File already exists, skipping download.")
        return download_path
    elif download_path.exists() and ignore_file_exists:
        _logger.info("File already exists, replacing file contents.")

    _logger.info("Downloading dataset from %s", download_url)
    response = requests.get(download_url)

    download_folder = download_path / ".."
    download_folder.resolve().mkdir(parents=True, exist_ok=True)
    
    try:
        response.raise_for_status()
    except rqexc.HTTPError as exc:
        err_msg = f"HTTP Error: {exc}"
        _logger.error(err_msg)
        return None
    
    with download_path.open("wb") as file:
        _logger.debug("Writing content to %s", download_path)
        file.write(response.content)

    return download_path


def get_csvs(
    zip_file: ZipFile,
    city: Cities,
) -> Generator[tuple[str, pd.DataFrame]]:
    """Iteratively return the csvs of measurements for a single city.
    
    Parameters
    ----------
    zip_file : ZipFile
        The zipfile containing the SensEURCity data.
    city : Cities
        The city to download data from.
    """
    if len(list(city)) > 1:
        raise ValueError("Multiple cities given. Please choose one.")
    elif city == Cities.Antwerp:
        _logger.info("Iterating over Antwerp data")
        prefix = "Antwerp_*.csv"
    elif city == Cities.Oslo:
        _logger.info("Iterating over Oslo data")
        prefix = "Oslo_*.csv"
    elif city == Cities.Zagreb:
        _logger.info("Iterating over Zagreb data")
        prefix = "Zagreb_*.csv"
    else:
        raise ValueError(
            "Unexpected city given. "
            "Please choose from Antwerp, Oslo or Zagreb"
        )
    zip_path = ZipPath(zip_file, at="dataset/")
    if not zip_path.exists():
        root_path = ZipPath(zip_file)
        subfolders = [
            subfolder.name for subfolder in root_path.iterdir()
            if "senseurcity_data_v" in subfolder.name 
            or "dataset" in subfolder.name
        ]
        if len(subfolders) > 1:
            err = (
                "Zipfile contains multiple datasets: "
                f"{', '.join(subfolders)}"
            )
            raise ValueError(err)
        elif len(subfolders) < 1:
            raise FileNotFoundError(
                "'dataset' folder missing from provided zip file. "
                "Redownload or choose correct zip file."
            )
        elif len(subfolders) == 1:
            zip_path = ZipPath(
                zip_file,
                at=(
                    f"{subfolders[0]}/dataset/"
                    if subfolders[0] != "dataset" else "dataset/"
                )
            )
    if not list(zip_path.glob(prefix)):
        err_msg = f"No csv files found for {prefix[:-6]}"
        _logger.warning(err_msg)
        return None
    for csv_file in zip_path.glob(prefix):
        with csv_file.open("r") as csv:
            _logger.debug("Returning %s", csv.name)
            yield (csv_file.stem, pd.read_csv(csv, low_memory=False))
