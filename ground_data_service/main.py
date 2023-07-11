import json
import logging
from datetime import date, datetime
from typing import Annotated, List

import pandas as pd
import pycountry
import shapely.wkt
from aimlsse_api.data.metar import MetarProperty
from aimlsse_api.interface import GroundDataAccess
from fastapi import APIRouter, Body, FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, Response
from shapely import Polygon

from . import IowaMetarDownloader, MetarDataProvider, MetarMap, StationControl


class GroundDataService(GroundDataAccess):
    def __init__(self) -> None:
        super().__init__()
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        # Setup a router for FastAPI
        self.router = APIRouter()
        self.router.add_api_route('/queryMetar', self.queryMetar, methods=['POST'])
        self.router.add_api_route('/queryMetadata', self.queryMetadata, methods=['POST'])
        self.router.add_api_route('/getAllStations', self.getAllStations, methods=['GET'])
        self.router.add_api_route('/forceRebuildMap', self.forceRebuildMap, methods=['GET'])
    
    async def queryMetar(self, data:Annotated[dict, Body(
            examples=[
                {
                    'stations': ['EDDF', 'EDDV', 'ELLX', 'LOWL']
                },
                {
                    'polygons': ['POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))', 'POLYGON ((0 0, 0 10, 10 10, 10 0))']
                }
            ]
    )], datetime_from:datetime, datetime_to:datetime):
        parameters_present = self.validate_json_parameters(data, [['stations', 'polygons'], ['properties']])
        property_strings: List[str] = data['properties']
        properties = [MetarProperty.from_string(prop_str) for prop_str in property_strings]
        stations: List[str] = []
        if 'stations' in parameters_present[0]:
            stations += data['stations']
            self.logger.info(f'Querying METAR for stations:\n{stations}')
        elif 'polygons' in parameters_present[0]:
            polygon_strings: List[str] = data['polygons']
            polygons: List[Polygon] = [shapely.wkt.loads(x) for x in polygon_strings]
            self.logger.info(f'Querying METAR for stations in polygons:\n{polygons}')
            stations_gdf = MetarMap().get_stations_in_polygons(polygons)
            stations += stations_gdf['id'].to_list()
        else:
            raise HTTPException(status_code=400, detail='Neither stations nor polygons are defined')
        stations = sorted([*set(stations)]) # Remove duplicate stations
        self.logger.info(f'Querying METAR for stations:\n{stations}')
        return JSONResponse(
                json.loads(MetarDataProvider().query(stations, datetime_from, datetime_to, properties)
                    .to_json(date_format='iso', orient='table', index=False))
            )
    
    async def queryMetadata(self, data:Annotated[dict, Body(
            examples=[
                {
                    'stations': ['EDDF', 'EDDV', 'ELLX', 'LOWL']
                },
                {
                    'polygons': ['POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))', 'POLYGON ((0 0, 0 10, 10 10, 10 0))']
                }
            ]
    )]):
        parameters_present = self.validate_json_parameters(data, [['stations', 'polygons']])
        metar_map = MetarMap()
        stations: List[str] = []
        if 'stations' in parameters_present[0]:
            stations += data['stations']
            self.logger.info(f'Querying metadata for stations:\n{stations}')
        elif 'polygons' in parameters_present[0]:
            polygon_strings: List[str] = data['polygons']
            polygons: List[Polygon] = [shapely.wkt.loads(x) for x in polygon_strings]
            self.logger.info(f'Querying metadata for stations in polygons:\n{polygons}')
            stations_gdf = MetarMap().get_stations_in_polygons(polygons)
            stations += stations_gdf['id'].to_list()
        else:
            raise HTTPException(status_code=400, detail='Neither stations nor polygons are defined')
        stations = sorted([*set(stations)]) # Remove duplicate stations
        self.logger.info(f'Querying metadata for stations:\n{stations}')
        return JSONResponse(json.loads(metar_map.get_stations(stations).to_json()))

    async def getAllStations(self):
        self.logger.info('Querying metadata for all stations..')
        return JSONResponse(
            json.loads(MetarMap().get_all_stations().to_json())
        )

    async def forceRebuildMap(self):
        self.logger.info('Forcing rebuild of map data..')
        MetarMap().force_rebuild()
        return Response()

    def validate_json_parameters(self, data:dict, parameters:List[List[str]]) -> List[List[str]]:
        '''
        Ensures that the given JSON dict contains the specified parameters.
        Each entry in the parameters is a list of strings, where at least one must be present.

        Parameters
        ----------
        data: `JSON / dict`
            The JSON inside which the parameters are searched for
        parameters: `List[List[str]]`
            The parameters to search for inside the JSON

        Raises
        ------
        `ValueError`
            If a parameter is not contained in the data
        
        Returns
        -------
        `List[List[str]]`
            All parameters that are present in the data
        '''
        parameters_present = []
        for attributes in parameters:
            attributes_present = list(filter(lambda x: x in data, attributes))
            if len(attributes_present) == 0:
                raise HTTPException(status_code=400, detail=f'Missing information about "{attributes}" from received JSON data.')
            else:
                parameters_present += [attributes_present]
        return parameters_present

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('fiona').setLevel(logging.INFO)
app = FastAPI()
groundDataService = GroundDataService()
app.include_router(groundDataService.router)