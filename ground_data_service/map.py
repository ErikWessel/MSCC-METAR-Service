import logging
import os
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd
import pycountry
import yaml

from . import IowaMetarDownloader


class MetarMap:
    stations_per_country: Dict[str, gpd.GeoDataFrame] = {}
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
        return MetarMap.countries[['GU_A3', 'NAME', 'CONTINENT', 'geometry']]
    
    def __get_stations(self, country_code:str):
        if country_code not in MetarMap.stations_per_country:
            self.__load(country_code)
        return MetarMap.stations_per_country[country_code]

    def get_all_stations(self):
        if not MetarMap.stations_per_country:
            if os.path.exists(self.data_path):
                filenames: List[str] = os.listdir(os.path.dirname(self.data_path))
                if len(filenames) != 0:
                    for filename in filenames:
                        self.logger.debug(f'Filename: {filename}')
                        country_code_alpha_3 = filename.split('_')[0]
                        self.__load(country_code_alpha_3)
                else:
                    self.__build()
            else:
                self.__build()
        data = [table for table in MetarMap.stations_per_country.values()]
        return gpd.GeoDataFrame(pd.concat(data))

    def force_build(self):
        self.__build()

    def __build(self):
        self.logger.info('Building Stations-per-Country dict..')
        stations: gpd.GeoDataFrame = IowaMetarDownloader().get_stations_for_all_networks()
        countries = self.__get_countries()
        self.logger.debug(f'Countries-CRS = {countries.crs}, Stations-CRS = {stations.crs}')
        assert countries.crs == stations.crs, 'GeoDataFrames for countries and stations have different CRS, which is not allowed'
        countries = countries.rename(columns={'NAME':'corrected_country'})
        self.logger.info(f'Mapping {len(stations)} stations to {len(countries)} countries..')
        stations_in_countries: gpd.GeoDataFrame = stations.sjoin_nearest(countries, distance_col='distance_to_region')
        self.logger.info(f'Mapping resulted in {len(stations_in_countries)} stations')
        lost_stations = stations[~stations['id'].isin(stations_in_countries['id'])]
        num_lost_stations = len(lost_stations)
        if num_lost_stations > 0:
            lost_stations_subset = lost_stations[['id', 'name']]
            self.logger.warning(f'The lost stations are:\n{lost_stations_subset.to_string()}')
            self.logger.warning(f'We lost {num_lost_stations} stations in the spatial join!')
        # TODO - Make sure not-intersecting points are not dropped
        MetarMap.stations_per_country = {key: table for key,table in stations_in_countries.groupby(['GU_A3'])}
        self.logger.info(f'Mapping complete')
        self.__store()
    
    def __get_filename(self, country_code:str):
        return f'{country_code}_stations.geojson'

    def __store(self):
        self.logger.info(f'Storing all mappings..')
        os.makedirs(self.data_path, exist_ok=True)
        for country_code, table in MetarMap.stations_per_country.items():
            filepath = os.path.join(self.data_path, self.__get_filename(country_code))
            table.to_file(filepath)
    
    def __load(self, country_code:str):
        self.logger.info(f'Loading mappings for country {pycountry.countries.get(alpha_3=country_code).name}..')
        filepath = os.path.join(self.data_path, self.__get_filename(country_code))
        if (not os.path.exists(self.data_path)) or (len(os.listdir(os.path.dirname(self.data_path))) == 0):
            # Directory does not exist or is empty - rebuild
            self.__build()
        elif os.path.exists(filepath):
            # File exists - load
            MetarMap.stations_per_country[country_code] = gpd.read_file(filepath)
        else:
            # File does not exist but data is present
            raise ValueError(f'Country-Code {country_code} does not relate to an existing country')

    def get_stations_in_country(self, country_code:str) -> gpd.GeoDataFrame:
        return self.__get_stations(country_code)
    
    def get_stations_in_countries(self, country_codes:List[str]) -> gpd.GeoDataFrame:
        data = [self.__get_stations(code) for code in country_codes]
        return gpd.GeoDataFrame(pd.concat(data))

    def find_country_code(self, country_query:str) -> str:
        try:
            found_countries = pycountry.countries.search_fuzzy(country_query)
        except LookupError:
            raise ValueError(f'Could not find country {country_query}')
        return found_countries[0].alpha_3
