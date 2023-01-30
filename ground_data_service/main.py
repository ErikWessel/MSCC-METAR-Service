import json
import logging
from datetime import date
from typing import List

from aimlsse_api.data.metar import MetarProperty
from aimlsse_api.interface import GroundDataAccess
from fastapi import APIRouter, FastAPI
from fastapi.responses import JSONResponse, Response, FileResponse
import pandas as pd
import pycountry

from . import IowaMetarDownloader, MetarDataProvider, StationControl, MetarMap


class GroundDataService(GroundDataAccess):
    def __init__(self) -> None:
        super().__init__()
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        # Setup a router for FastAPI
        self.router = APIRouter()
        self.router.add_api_route('/queryMetar', self.queryMetar, methods=['POST'])
        self.router.add_api_route('/queryPosition', self.queryPosition, methods=['POST'])
        self.router.add_api_route('/getStationsInCountries', self.getStationsInCountries, methods=['POST'])
    
    async def queryMetar(self, data:dict, date_from:date, date_to:date):
        self.validate_json_parameters(data, ['stations', 'properties'])
        stations: List[str] = data['stations']
        property_strings: List[str] = data['properties']
        properties = [MetarProperty.from_string(prop_str) for prop_str in property_strings]
        return JSONResponse(
            json.loads(MetarDataProvider().query(stations, date_from, date_to, properties)
                .to_json(date_format='iso'))
        )
    
    async def queryPosition(self, stations:List[str]):
        sc = StationControl()
        stations = sc.prepare_stations_for_processing(stations)
        return JSONResponse(json.loads(sc.get_positional_data(stations).to_json()))
    
    async def getStationsInCountries(self, countries:List[str]):
        self.logger.info(f'Querying stations for countries:\n{countries}')
        metar_map = MetarMap()
        country_codes = [metar_map.find_country_code(country) for country in countries]
        return JSONResponse(
            json.loads(metar_map.get_stations_in_countries(country_codes).to_json())
        )

    def validate_json_parameters(self, data:dict, parameters:List[str]) -> None:
        '''
        Ensures that the given JSON dict contains the specified parameters.

        Parameters
        ----------
        data: `JSON / dict`
            The JSON inside which the parameters are searched for
        parameters: `List[str]`
            The parameters to search for inside the JSON

        Raises
        ------
        `ValueError`
            If a parameter is not contained in the data
        '''
        for param in parameters:
            if param not in data:
                raise ValueError(f'Missing information about "{param}" from received JSON data.')

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('fiona').setLevel(logging.INFO)
app = FastAPI()
groundDataService = GroundDataService()
app.include_router(groundDataService.router)