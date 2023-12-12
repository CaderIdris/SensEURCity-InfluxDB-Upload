import argparse
import json
import logging
import os
from pathlib import Path
import re

from caderidflux import InfluxWriter
import numpy as np

from .data import LowCostSensor, ReferenceMonitor

level = logging.DEBUG if os.getenv('PYLOGDEBUG') else logging.INFO
logger = logging.getLogger()
logger.setLevel(level)

formatter = \
    '%(asctime)s - %(funcName)s - %(levelname)s - %(message)s' \
    if os.getenv('PYLOGDEBUG') else '%(message)s'

handler = logging.StreamHandler()
handler.setLevel(level)
formatter = logging.Formatter(
)
handler.setFormatter(formatter)
logger.addHandler(handler)


def get_json(path_to_json):
    """Finds json file and returns it as dict

    Creates blank file with required keys at path if json file is not
    present

        Keyword Arguments:
            path_to_json (str): Path to the json file, including
            filename and .json extension

        Returns:
            Dict representing contents of json file

        Raises:
            FileNotFoundError if file is not present, blank file created

            ValueError if file can not be parsed
    """

    try:
        with open(path_to_json, "r") as jsonFile:
            try:
                return json.load(jsonFile)
            except json.decoder.JSONDecodeError:
                raise ValueError(
                    f"{path_to_json} is not in the proper"
                    f"format. If you're having issues, consider"
                    f"using the template from the Github repo or "
                    f" use the format seen in README.md"
                )
    except FileNotFoundError:
        raise FileNotFoundError(
            f"{path_to_json} could not be found, use the "
            f"template from the Github repo or use the "
            f"format seen in README.md"
        )


def cli():
    arg_parser = argparse.ArgumentParser(
        prog="SensEURCity-To-InfluxDB",
        description="Reads data from SensEURCity, processes and uploads to "
        "InfluxDB 2.x database"
    )
    arg_parser.add_argument(
        "-i",
        "--influx-path",
        type=str,
        help=f"Alternate location for influx config json file (Defaults to "
        f"{os.getenv('PWD')}/influx.json)",
        default=os.getenv("INFLUX_TOKEN", "influx.json")
    )
    arg_parser.add_argument(
        "-d",
        "--data-path",
        type=str,
        help="Where data is stored",
        default=os.getenv("SENSEURCITY_DATA")
    )
    args = vars(arg_parser.parse_args())
    influx_config = get_json(args.get('influx_path'))

    if not args.get('data_path'):
        raise ValueError('No data path provided as argument or environment variable')

    csv_path = args.get('data_path')
    csv_files = list(
        filter(
            lambda x: re.match(
                r'Antwerp_.*\.csv|Oslo_.*\.csv|Zagreb_.*\.csv',
                x.parts[-1]
            ),
            Path(csv_path).glob('*.csv')
        )
    )
    split = 50000
    logger.info(f'{len(csv_files)} csv files found in {csv_path}')
#    for csv in csv_files:
#        sensor = LowCostSensor(csv)
#        sensor.parse_files()
#        for site, data in sensor.return_dfs().items():
#            inf = InfluxWriter(**influx_config, bucket='SensEURCity')
#            data['Name'] = site
#            measurement = csv.parts[-1].split('_')[0]
#            logging.info(f'Writing data for {site} ({data.shape})')
#            if data.shape[0] > split:
#                split_d = np.array_split(data, np.ceil(data.shape[0] / split))
#                for df in split_d:
#                    inf.write_dataframe(df, measurement)
#            else:
#                inf.write_dataframe(data, measurement)
    ref = ReferenceMonitor(csv_files)
    ref.parse_files()
    for site, data in ref.return_dfs().items():
        data['Name'] = site
        measurement = 'Reference'
        inf = InfluxWriter(**influx_config, bucket='SensEURCity')
        logging.info(f'Writing data for {site} ({data.shape})')
        if data.shape[0] > split:
            split_d = np.array_split(data, np.ceil(data.shape[0] / split))
            for df in split_d:
                inf.write_dataframe(df, measurement)
        else:
            inf.write_dataframe(data, measurement)
        


