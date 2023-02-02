import logging
import os
import shutil
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import pycountry
import yaml
from shapely import Polygon

from . import IowaMetarDownloader


class MetarMap:
    stations: Optional[gpd.GeoDataFrame] = None
    countries: Optional[gpd.GeoDataFrame] = None

    def __init__(self) -> None:
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        map_config = yaml.safe_load(open('config.yml'))['map']
        self.data_path = map_config['data-path']

    def __get_countries(self) -> gpd.GeoDataFrame:
        self.logger.info('Getting countries..')
        if MetarMap.countries is None:
            self.logger.info('Countries not in memory - loading..')
            MetarMap.countries = gpd.read_file('geo_data/countries.geojson')
        return MetarMap.countries[['ISO_A3_EH', 'NAME', 'CONTINENT', 'geometry']]
    
    def get_all_stations(self) -> gpd.GeoDataFrame:
        if MetarMap.stations is None:
            self.__load()
        return MetarMap.stations

    def force_rebuild(self):
        self.__build()

    def __build(self):
        self.logger.info('Building Stations-per-Country dict..')
        self.__delete_data()
        self.logger.info('Querying stations..')
        stations: gpd.GeoDataFrame = IowaMetarDownloader().get_stations_for_all_networks()
        stations.drop(['country'], axis=1, inplace=True) # Remove wrong country name
        self.logger.info('Querying countries..')
        countries = self.__get_countries()
        self.logger.debug(f'Countries-CRS = {countries.crs}, Stations-CRS = {stations.crs}')
        assert countries.crs == stations.crs, 'GeoDataFrames for countries and stations have different CRS, which is not allowed'
        countries = countries.rename(columns={'NAME':'country'})
        self.logger.info(f'Mapping {len(stations)} stations to {len(countries)} countries..')
        stations_in_countries: gpd.GeoDataFrame = stations.sjoin_nearest(countries, distance_col='distance_to_region')
        self.logger.info(f'Mapping resulted in {len(stations_in_countries)} stations')
        lost_stations = stations[~stations['id'].isin(stations_in_countries['id'])]
        num_lost_stations = len(lost_stations)
        if num_lost_stations > 0:
            lost_stations_subset = lost_stations[['id', 'name']]
            self.logger.warning(f'The lost stations are:\n{lost_stations_subset.to_string()}')
            self.logger.warning(f'We lost {num_lost_stations} stations in the spatial join!')
        MetarMap.stations = stations_in_countries
        self.logger.info(f'Mapping complete')
        self.__store()
    
    def __get_filename(self):
        return f'stations.geojson'

    def __delete_data(self):
        self.logger.info('Removing data..')
        os.makedirs(self.data_path, exist_ok=True)
        for filename in os.listdir(os.path.dirname(self.data_path)):
            file_path = os.path.join(self.data_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as exception:
                self.logger.error(f'Unable to delete file {file_path}\n--> Problem: {exception}')

    def __store(self):
        self.logger.info(f'Storing all station mappings..')
        os.makedirs(self.data_path, exist_ok=True)
        filepath = os.path.join(self.data_path, self.__get_filename())
        self.get_all_stations().to_file(filepath)
    
    def __load(self):
        self.logger.info(f'Loading station mappings..')
        filepath = os.path.join(self.data_path, self.__get_filename())
        if (not os.path.exists(self.data_path)) or (len(os.listdir(os.path.dirname(self.data_path))) == 0):
            # Directory does not exist or is empty - rebuild
            self.__build()
        elif os.path.exists(filepath):
            # File exists - load
            MetarMap.stations = gpd.read_file(filepath)
        else:
            # File does not exist but data is present
            raise ValueError(f'Station mappings have not been built')
    
    def get_stations(self, stations:List[str]) -> gpd.GeoDataFrame:
        data = self.get_all_stations()
        return data[data['id'].isin(stations)]
    
    def get_stations_in_polygon(self, polygon:Polygon) -> gpd.GeoDataFrame:
        data = self.get_all_stations()
        return data[data.within(polygon)]
    
    def get_stations_in_polygons(self, polygons:List[Polygon]) -> gpd.GeoDataFrame:
        data = [self.get_stations_in_polygon(x) for x in polygons]
        return gpd.GeoDataFrame(pd.concat(data))

    def exists(self, stations:List[str]) -> Tuple[bool, List[bool]]:
        data = self.get_all_stations()
        stations_exist = zip(stations, pd.Series(stations).isin(data['id']).to_list())
        nonexistent_stations = list(filter(lambda x: not x[1], stations_exist))
        return (all(stations_exist), nonexistent_stations)
