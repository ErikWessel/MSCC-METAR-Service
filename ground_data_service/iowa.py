import csv
import logging
import time
from datetime import date
from io import StringIO
from typing import List

import pandas as pd
import requests
import yaml


class IowaMetarDownloader:

    def __init__(self) -> None:
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        config = yaml.safe_load(open('config.yml'))
        self.download_url: str = config['metar']['download-url']

    def download(self, stations:List[str], date_from:date, date_to:date) -> pd.DataFrame:
        # Prepare URL
        self.logger.info(f'Downloading data for stations {stations}\n from {date_from} until {date_to}')
        url = self.download_url + 'data=metar&tz=Etc%2FUTC&format=onlycomma'
        url += '&latlon=no&elev=no&missing=M&trace=T&direct=yes&report_type=3&report_type=4'
        url += date_from.strftime(  '&year1=%Y&month1=%m&day1=%d')
        url += date_to.strftime(    '&year2=%Y&month2=%m&day2=%d')
        url += '&station=' + '&station='.join(stations)
        # Download data from URL
        time_start = time.perf_counter()
        download = requests.get(url)
        time_end = time.perf_counter()
        self.logger.info(f'Download took {time_start - time_end} seconds')
        download.raise_for_status()
        # Process and store data
        data_io = StringIO(download.text)
        return pd.read_csv(data_io)
