import io
import logging
import os
from typing import List

import pandas as pd
import requests
import yaml


class StationControl:
    __data = None

    def __init__(self) -> None:
        station_data_config = yaml.safe_load(open('config.yml'))['station-data']
        self.filepath = station_data_config['filepath']
        self.download_url = station_data_config['download-url']
    
    def download_data(self):
        response = requests.get(f'{self.download_url}')
        response.raise_for_status()
        with open(self.filepath, 'w') as file:
            file.write(response.text)
        StationControl.__data = pd.read_csv(io.StringIO(response.text), index_col='id')
        logging.debug(StationControl.__data)

    def load_data(self):
        if os.path.exists(self.filepath):
            StationControl.__data = pd.read_csv(self.filepath, index_col='id')
        else:
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            # Data not in local storage - try to download
            self.download_data()

    def get_data(self):
        if StationControl.__data is None:
            self.load_data()
        return StationControl.__data

    def exists(self, station:str):
        data = self.get_data()
        return station in data['ident'].values
    
    def verify_stations(self, stations:List[str]):
        if not stations:
            raise ValueError(f'Stations must not be empty')
        incorrect_stations = []
        for station in stations:
            if not (self.exists(station)):
                incorrect_stations += [station]
        if incorrect_stations:
            raise ValueError(f'IDs {incorrect_stations} do not relate to stations')

    def format_stations(self, stations:List[str]) -> List[str]:
        return [self.format_station(station) for station in stations]
    
    def format_station(self, station:str) -> str:
        return station.upper()

    def prepare_stations_for_processing(self, stations:List[str]):
        stations = self.format_stations(stations)
        self.verify_stations(stations)
        return stations
