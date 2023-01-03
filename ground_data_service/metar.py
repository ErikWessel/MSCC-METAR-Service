import os
from typing import Union

import requests
import yaml
from metar_taf_parser.parser.parser import Metar, MetarParser

from . import AirportDataProvider


class MetarDataProvider:
    data = {}

    def __init__(self) -> None:
        metar_data_config = yaml.safe_load(open('config.yml'))['metar-data']
        self.download_url = metar_data_config['download-url']
        self.dirpath = metar_data_config['dirpath']
        self.file_extension = metar_data_config['file-extension']
    
    def get_filepath(self, airport_id:str) -> str:
        return f'{self.dirpath}{airport_id}{self.file_extension}'

    def load_metar_string(self, airport_id:str, raw_metar_data:str):
        preprocessed_metar_data = raw_metar_data.split('\n')
        if len(preprocessed_metar_data) == 1 or preprocessed_metar_data[0].startswith(airport_id):
            preprocessed_metar_data = raw_metar_data
        else:
            preprocessed_metar_data = preprocessed_metar_data[1]
        metar_data = MetarParser().parse(preprocessed_metar_data)
        MetarDataProvider.data[airport_id] = metar_data

    def download_data(self, airport_id:str) -> None:
        response = requests.get(f'{self.download_url}{airport_id}{self.file_extension}')
        response.raise_for_status()
        self.load_metar_string(airport_id, response.text)
        filepath = self.get_filepath(airport_id)
        with open(filepath, 'w') as file:
            file.write(response.text)

    def load_from_file(self, airport_id:str) -> None:
        filepath = self.get_filepath(airport_id)
        if os.path.exists(filepath):
            with open(filepath) as file:
                raw_metar_data = file.read()
            self.load_metar_string(airport_id, raw_metar_data)
        else:
            os.makedirs(os.path.dirname(self.dirpath), exist_ok=True)
            # Data not in local storage - try to download
            self.download_data(airport_id)

    def query(self, airport_id:str) -> Union[Metar, None]:
        airport_id = airport_id.upper()
        if not AirportDataProvider().exists(airport_id):
            raise ValueError(f'ID {airport_id} does not relate to an airport')

        if airport_id in MetarDataProvider.data:
            return MetarDataProvider.data[airport_id]
        else:
            # Data of airport is not cached - try to load from file
            self.load_from_file(airport_id)
        
        if airport_id in MetarDataProvider.data:
            return MetarDataProvider.data[airport_id]
        else:
            raise RuntimeError(f'Data for airport {airport_id} is not available')