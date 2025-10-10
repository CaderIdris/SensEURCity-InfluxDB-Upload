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

logger = logging.getLogger(f'__main__.{__name__}')


class Cities(Flag):
    """"""
    Antwerp = auto()
    Oslo = auto()
    Zagreb = auto()


def download_data(
    download_url: str,
    download_path: Path
) -> Path | None:
    """Download SensEURCity data from URL and save to specified path.

    Parameters
    ----------
    download_url : str
        Link to the SensEURCity data.
    download_path : Path
        Where to save the data to.

    Returns
    -------
    - Path object pointing to downloaded file if successful
    - None if HTTPError encountered
    """
    response = requests.get(download_url)
    
    try:
        response.raise_for_status()
    except rqexc.HTTPError as exc:
        err_msg = f"HTTP Error: {exc}"
        logger.error(err_msg)
        return None

    with download_path.open("wb") as file:
        file.write(response.content)

    return download_path


class SensEURCityZipFile(ZipFile):
    """"""
    def get_csvs(self, city: Cities) -> Generator[tuple[str, pd.DataFrame]]:
        """"""
        if len(list(city)) > 1:
            raise ValueError("Multiple cities given. Please choose one.")
        elif city == Cities.Antwerp:
            prefix = "Antwerp_*.csv"
        elif city == Cities.Oslo:
            prefix = "Oslo_*.csv"
        elif city == Cities.Zagreb:
            prefix = "Zagreb_*.csv"
        else:
            raise ValueError(
                "Unexpected city given. "
                "Please choose from Antwerp, Oslo or Zagreb"
            )
        zip_path = ZipPath(self, at="dataset/")
        if not zip_path.exists():
            raise FileNotFoundError(
                "'dataset' folder missing from provided zip file. "
                "Redownload or choose correct zip file."
            )
        if not list(zip_path.glob(prefix)):
            err_msg = f"No csv files found for {prefix[:-6]}"
            logger.warning(err_msg)
            return None
        for csv_file in zip_path.glob(prefix):
            with csv_file.open("r") as csv:
                yield (csv_file.name[:-4], pd.read_csv(csv))
