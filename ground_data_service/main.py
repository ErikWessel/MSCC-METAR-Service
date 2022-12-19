import pandas as pd
import geopandas as gpd
import datetime
import logging
import json
from fastapi import FastAPI, APIRouter
from fastapi.responses import JSONResponse
from aimlsse_api.ground_data_access import GroundDataAccess

class GroundDataAccessor(GroundDataAccess):
    def __init__(self) -> None:
        super().__init__()
        # Setup a router for FastAPI
        self.router = APIRouter()
        self.router.add_api_route('/queryMeasurements', self.queryMeasurements, methods=['GET'])
    
    async def queryMeasurements(self, datetime_from:datetime.datetime, datetime_to:datetime.datetime) -> JSONResponse:
        # Placeholder implementation until actual data-access becomes available
        logging.info('Querying for measurements..')
        measurements = pd.read_csv('data/ground_measurements.csv')
        logging.info('Preprocessing measurements..')
        measurements = measurements[measurements.date.between(str(datetime_from), str(datetime_to))]
        measurements['geometry'] = gpd.points_from_xy(measurements.longitude, measurements.latitude)
        sensor_locations = gpd.GeoDataFrame(measurements)
        sensor_locations.set_crs(epsg=4326, inplace=True)
        logging.info('Query for grid measurements complete!')
        # GeoDataFrame.to_json() only converts to a json-readable string.
        # An additional wrapper to json-type has to be added to ensure a working API!
        # Make sure to check if the return type of an HTTP-Request is of type dict.
        return JSONResponse(json.loads(sensor_locations.to_json(drop_id=True)))

logging.basicConfig(level=logging.DEBUG)
app = FastAPI()
groundDataAccessor = GroundDataAccessor()
app.include_router(groundDataAccessor.router)