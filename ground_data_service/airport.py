import io
import logging
import os

import pandas as pd
import requests
import yaml


class AirportDataProvider:
    __data = None

    def __init__(self) -> None:
        airport_data_config = yaml.safe_load(open('config.yml'))['airport-data']
        self.filepath = airport_data_config['filepath']
        self.download_url = airport_data_config['download-url']
    
    def download_data(self):
        response = requests.get(f'{self.download_url}')
        response.raise_for_status()
        with open(self.filepath, 'w') as file:
            file.write(response.text)
        AirportDataProvider.__data = pd.read_csv(io.StringIO(response.text), index_col='id')
        logging.debug(AirportDataProvider.__data)

    def load_data(self):
        if os.path.exists(self.filepath):
            AirportDataProvider.__data = pd.read_csv(self.filepath, index_col='id')
        else:
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            # Data not in local storage - try to download
            self.download_data()

    def get_data(self):
        if AirportDataProvider.__data is None:
            self.load_data()
        return AirportDataProvider.__data

    def exists(self, airport_id:str):
        data = self.get_data()
        return airport_id in data['ident'].values
