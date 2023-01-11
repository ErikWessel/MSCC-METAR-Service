import json
import logging
from datetime import date
from typing import List

from aimlsse_api.interface import GroundDataAccess
from fastapi import APIRouter, FastAPI
from fastapi.responses import JSONResponse

from . import MetarDataProvider


class GroundDataService(GroundDataAccess):
    def __init__(self) -> None:
        super().__init__()
        # Setup a router for FastAPI
        self.router = APIRouter()
        self.router.add_api_route('/queryMetar', self.queryMetar, methods=['POST'])
    
    async def queryMetar(self, stations:List[str], date_from:date, date_to:date):
        return JSONResponse(json.loads(MetarDataProvider().query(stations, date_from, date_to).to_json(date_format='iso')))

logging.basicConfig(level=logging.DEBUG)
app = FastAPI()
groundDataService = GroundDataService()
app.include_router(groundDataService.router)