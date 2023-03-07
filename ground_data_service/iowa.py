import csv
import logging
import os
import random
import time
from datetime import date
from io import StringIO
from typing import Dict, List, Optional

import geopandas as gpd
import pandas as pd
import requests
import yaml


class IowaMetarDownloader:
    networks: Optional[pd.DataFrame] = None
    networks_to_stations: Dict[str, gpd.GeoDataFrame] = {}

    def __init__(self) -> None:
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        metar_config = yaml.safe_load(open('config.yml'))['metar']
        self.download_url: str = metar_config['download-url']
        self.api_url: str = metar_config['api-url']
        self.data_path = os.path.join(metar_config['data-path'], 'iowa')

    def download(self, stations:List[str], date_from:date, date_to:date) -> pd.DataFrame:
        chunk_size = 100
        station_chunks = [stations[i:i + chunk_size] for i in range(0, len(stations), chunk_size)]
        data_chunks = []
        for partial_stations in station_chunks:
            # Prepare URL
            self.logger.info(f'Downloading data for stations {partial_stations}\n from {date_from} until {date_to}')
            url = self.download_url + 'data=metar&tz=Etc%2FUTC&format=onlycomma'
            url += '&latlon=no&elev=no&missing=M&trace=T&direct=yes&report_type=3&report_type=4'
            url += date_from.strftime(  '&year1=%Y&month1=%m&day1=%d')
            url += date_to.strftime(    '&year2=%Y&month2=%m&day2=%d')
            url += '&station=' + '&station='.join(partial_stations)
            # Download data from URL
            time_start = time.perf_counter()
            download = requests.get(url)
            time_end = time.perf_counter()
            self.logger.info(f'Download took {time_end - time_start:.6f} seconds')
            download.raise_for_status()
            # Process and store data
            data_io = StringIO(download.text)
            data_chunks += [pd.read_csv(data_io)]
            if len(partial_stations) == chunk_size:
                # Sleep between download calls
                time.sleep(0.5)
        return pd.concat(data_chunks, ignore_index=True).drop_duplicates(subset=['station', 'valid'])

    def get_networks(self) -> pd.DataFrame:
        '''
        Queries all networks that are available.
        
        Returns
        -------
        `DataFrame`
            The networks to find the stations for
        '''
        logging.info('Get all networks..')
        if IowaMetarDownloader.networks is None:
            # Data is not in memory - try to load
            filename = 'networks.json'
            path = os.path.join(self.data_path, filename)
            source = path
            if not os.path.exists(path):
                # Data is not available - download
                os.makedirs(os.path.dirname(path))
                logging.info('Information about networks is not avaiable, downloading..')
                response = requests.get(self.api_url + filename)
                response.raise_for_status()
                with open(path, 'wb') as file:
                    file.write(response.content)
                logging.info('Networks download complete!')
                source = StringIO(response.text)
            # Read data from file or network-response
            IowaMetarDownloader.networks = pd.read_json(source, orient='table')
        return IowaMetarDownloader.networks[['id', 'name']]

    def get_stations_from_networks(self, networks:List[str]) -> gpd.GeoDataFrame:
        '''
        Queries the stations that are located in each of the specified networks.
        
        Parameters
        ----------
        networks: `List[str]`
            The networks to find the stations for
        
        Returns
        -------
        `GeoDataFrame`
            The data for the stations in the given networks
        '''
        # Ensure that networks are valid
        networks = [network.upper() for network in networks]
        networks_data = self.get_networks()
        for network in networks:
            if not (networks_data['id'].eq(network)).any():
                raise ValueError(f'Network {network} is not a known network')
        # Make sure data is available
        networks_path = os.path.join(self.data_path, 'networks')
        network_url = self.api_url + 'network/'
        for network in networks:
            logging.info(f'Get all stations of network {network}..')
            if network not in IowaMetarDownloader.networks_to_stations:
                # Data is not in memory - try to load
                filename = f'{network}.geojson'
                path = os.path.join(networks_path, filename)
                source = path
                if not os.path.exists(path):
                    # Data is not available - download
                    os.makedirs(os.path.dirname(path), exist_ok=True)
                    logging.info(f'Information about stations in network {network} is not avaiable, downloading..')
                    sleep_seconds = random.randint(1, 3)
                    self.logger.debug(f'Sleeping for {sleep_seconds} seconds to reduce load on server..')
                    time.sleep(sleep_seconds)
                    response = requests.get(network_url + filename)
                    response.raise_for_status()
                    with open(path, 'wb') as file:
                        file.write(response.content)
                    logging.info(f'Stations in network {network} download complete!')
                    source = StringIO(response.text)
                # Read data from file or network-response
                data = gpd.read_file(source)
                IowaMetarDownloader.networks_to_stations[network] = data[[
                        'id', 'name', 'plot_name', 'network', 'country', 'latitude', 'longitude', 'elevation', 'geometry'
                    ]]
        # Append GeoDataFrames and return result
        stations = [IowaMetarDownloader.networks_to_stations[network] for network in networks]
        return gpd.GeoDataFrame(pd.concat(stations))

    def get_stations_for_all_networks(self) -> gpd.GeoDataFrame:
        '''
        Queries the stations of all networks.
        
        Returns
        -------
        `GeoDataFrame`
            The data for the stations of all networks
        '''
        networks_df = self.get_networks()
        networks: List[str] = networks_df['id'].to_list()
        self.logger.info(f'Get stations for {len(networks)} networks..')
        result = self.get_stations_from_networks(networks)
        self.logger.info(f'Found {len(result)} stations')
        return result
