"""
Used to parse measurement data from the SensEURCity project [^article].
The dataset can be downloaded from Zenodo [^zenodo]

[^article]: https://www.nature.com/articles/s41597-023-02135-w
[^zenodo]: https://zenodo.org/doi/10.5281/zenodo.7256405
"""

import logging
from pathlib import Path
import re
from typing import Dict, Iterable, Union

import pandas as pd

logger = logging.getLogger(f'__main__.{__name__}')


class SensEURCity:
    """
    Parent class of both the `LowCostSensor` and `ReferenceMonitor` class.
    """

    def __init__(self, path: Union[Path, Iterable[Path]]):
        """
        Initialises the class. Should not be called directly, only called
        as part of `LowCostSensor` or `ReferenceMonitor` __init__ methods.

        Parameters
        ----------
        path : Path, iterable of Path
            Paths to the csv files
        """
        self.dfs: Dict[str, pd.DataFrame] = dict()
        """
        Processed csvs stored as pandas DataFrames
        """
        self.paths: Iterable[Path] = list()
        """
        Paths to the csv files that are to be processed
        """
        if isinstance(path, Path):
            self.paths: Iterable[Path] = [path]
        else:
            self.paths: Iterable[Path] = path

    def return_dfs(self) -> Dict[str, pd.DataFrame]:
        """
        Returns the dataframes in a dictionary, keys are the device names

        Returns
        -------
        Dict[str, pd.DataFrames]
            Parsed measurement data
        """
        return self.dfs


class LowCostSensor(SensEURCity):
    """
    Used to extract the low-cost sensor data from the SensEURCity project
    
    Examples
    --------
    >>> import os
    >>> from senseurcity import LowCostSensor
    >>> csv_path = f'{os.getenv("HOME")}/Data/senseurcity_data_v02/dataset/'
    >>> csv_files = list(
            filter(
                lambda x: re.match(
                    r'Antwerp_.*\.csv|Oslo_.*\.csv|Zagreb_.*\.csv',
                    x.parts[-1]
                ),
                Path(csv_path).glob('*.csv')
            )
        )
    >>> for csv in csv_files:
            sensor = LowCostSensor(csv)
            sensor.parse_files()
            df = sensor.return_dfs()
            # Further processing
            # It is recommended to process one csv at a time due to memory issues
    >>> print(df)
    |       date        |latitude|longitude|altitude|Location.ID|BMP280|...|SHT31TE_flag|\
SHT31TI_flag|
    |-------------------|--------|---------|--------|-----------|------|---|------------|\
------------|
    |2020-09-09 13:14:00|  59.9  |  10.7   |  59.5  |OSL_REF_KVN|995.9 |   |      W     |\
     W      |
    """

    def __init__(self, path: Union[Path, Iterable[Path]]):
        """
        Initialises the class

        Parameters
        ----------
        path : Path, iterable of Path
            Paths to the csv files
        """
        super().__init__(path)

    def parse_files(self):
        """
        Parses individual csv files, removing all measurements from collocated
        reference monitors
        """
        for csv in self.paths:
            logger.debug(f'Analysing {csv}')
            name = csv.parts[-1][:-4]
            csv_raw = pd.read_csv(csv, low_memory=False)
            sensor = csv_raw.loc[:, filter(lambda x: 'Ref.' not in x, csv_raw.columns)]
            sensor['date'] = pd.to_datetime(sensor['date'])
            sensor = sensor.set_index('date')
            sensor['Location.ID'] = sensor['Location.ID'].fillna('Field')
            object_cols = [col[0] for col in sensor.items() if col[1].dtype == 'object']
            sensor[object_cols] = sensor[object_cols].fillna('None')
            self.dfs[name] = sensor


class ReferenceMonitor(SensEURCity):
    """
    Used to extract reference monitor data from the SensEURCity project

    Examples
    --------
    >>> import os
    >>> from senseurcity import LowCostSensor
    >>> csv_path = f'{os.getenv("HOME")}/Data/senseurcity_data_v02/dataset/'
    >>> csv_files = list(
            filter(
                lambda x: re.match(
                    r'Antwerp_.*\.csv|Oslo_.*\.csv|Zagreb_.*\.csv',
                    x.parts[-1]
                ),
                Path(csv_path).glob('*.csv')
            )
        )
    >>> ref_monitors = ReferenceMonitor(csv_files)
    >>> ref.parse_files()
    >>> dfs = ref.return_dfs()
    >>> print(dfs['OSL_REFKVN'])
    |       date        |Long|Lat |Press|...|NO2|PM10.TEOM|
    |-------------------|----|----|-----|---|---|---------|
    |2020-03-19 12:49:00|10.7|59.9| NaN |   |5.9|  36.28  |
    """

    def __init__(self, path: Union[Path, Iterable[Path]]):
        """
        Initialises the class

        Parameters
        ----------
        path : Path, iterable of Path
            Paths to the csv files
        """
        super().__init__(path)

    def parse_files(self):
        """
        Reads through all csv files to extract reference measurements and
        concatenates, removing duplicates
        """
        for csv in self.paths:
            logger.debug(f'Analysing {csv}')
            csv_raw = pd.read_csv(csv, low_memory=False)
            ref = csv_raw.loc[:, ['date', 'Location.ID', *filter(lambda x: 'Ref.' in x, csv_raw.columns)]]
            all_na = ref[filter(lambda x: 'Ref.' in x, ref.columns)].isna().all(axis=1)
            ref = ref[~all_na]
            ref['date'] = pd.to_datetime(ref['date'])
            ref = ref.set_index('date', drop=False)
            ref.columns = [re.sub(r'^Ref\.', '', i) for i in ref.columns]
            for name, data in ref.groupby('Location.ID'):
                data = data.drop(columns='Location.ID').dropna(axis=1, how='all')
                site = name
                if self.dfs.get(site) is None:
                    self.dfs[site] = data.drop(columns='date')
                else:
                    self.dfs[site] = pd.concat([self.dfs[site], data]).drop_duplicates().sort_index().drop(columns='date')
