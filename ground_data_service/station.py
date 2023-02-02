import io
import logging
import os
from typing import List

import geopandas as gpd
import pandas as pd
import requests
from . import MetarMap
import yaml


class StationControl:

    def __init__(self) -> None:
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
    
    def verify_stations(self, stations:List[str]):
        if not stations:
            raise ValueError(f'Stations must not be empty')
        incorrect_stations = []
        metar_map = MetarMap()
        stations_exist, nonexistent_stations = metar_map.exists(stations)
        if not stations_exist:
            raise ValueError(f'IDs {nonexistent_stations} do not relate to stations')

    def format_stations(self, stations:List[str]) -> List[str]:
        return [self.format_station(station) for station in stations]
    
    def format_station(self, station:str) -> str:
        return station.upper()

    def prepare_stations_for_processing(self, stations:List[str]):
        stations = self.format_stations(stations)
        self.verify_stations(stations)
        return stations