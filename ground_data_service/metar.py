import csv
import logging
import time
from datetime import date, datetime
from typing import Dict, List

import numpy as np
import pandas as pd
import sqlalchemy as db
import sqlalchemy.orm as orm
import yaml

from . import DateChunker, IowaMetarDownloader, StationControl


class DatabaseConfig:

    def __init__(self, config:dict) -> None:
        self.technology = config['technology']
        self.name       = config['name']
        self.username   = config['username']
        self.password   = config['password']
        self.host       = config['host']
        self.port       = config['port']
    
    def createDatabase(self) -> db.engine.Engine:
        return db.create_engine(f'{self.technology}://{self.username}:{self.password}@{self.host}:{self.port}/{self.name}', echo=True)

class Base(orm.DeclarativeBase):
    pass

class MetarData(Base):
    __tablename__ = 'metar_data'

    station = orm.mapped_column(db.String, primary_key=True)
    datetime = orm.mapped_column(db.DateTime, primary_key=True)
    metar = orm.mapped_column(db.String)

    def __repr__(self) -> str:
        return f'MetarData(station={self.station!r}, datetime={self.datetime!r}, metar={self.metar!r})'    

class MetarDataProvider:

    def __init__(self) -> None:
        self.logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        config = yaml.safe_load(open('config.yml'))
        self.db_config = DatabaseConfig(config['database'])
        self.db_engine = self.db_config.createDatabase()
        self.download_url: str = config['metar']['download-url']
        Base.metadata.create_all(self.db_engine)

    def store_data(self, data:pd.DataFrame):
        with orm.Session(self.db_engine) as session:
            metar_data = []
            for index, row in data.iterrows():
                metar_data += [MetarData(station=row['station'], datetime=row['datetime'], metar=row['metar'])]
            self.logger.debug(f'Storing data: {metar_data}')
            session.add_all(metar_data)
            session.commit()
            self.logger.info(f'Stored data in database')

    def download_data(self, stations:List[str], date_from:date, date_to:date) -> pd.DataFrame:
        stations = StationControl().prepare_stations_for_processing(stations)
        data = IowaMetarDownloader().download(stations, date_from, date_to)
        data.columns = ['station', 'datetime', 'metar']
        data['datetime'] = pd.to_datetime(data['datetime'])
        return data
    
    def query_data(self, stations:List[str], date_from:date, date_to:date) -> pd.DataFrame:
        self.logger.info(f'Querying data for stations {stations}\n from {date_from} until {date_to}')
        metar_data = None
        with orm.Session(self.db_engine) as session:
            stmt = (
                db.select(MetarData.station, MetarData.datetime, MetarData.metar)
                .where(MetarData.station.in_(stations))
                .where(MetarData.datetime >= str(date_from))
                .where(MetarData.datetime < str(date_to))
                .order_by(db.asc(MetarData.station), db.asc(MetarData.datetime))
            )
            metar_data: db.engine.result.ChunkedIteratorResult = session.execute(stmt)
        # Format output
        self.logger.debug(f'Queried METAR data type: {type(metar_data)}')
        result = pd.DataFrame(metar_data.all())
        result.columns = ['station', 'datetime', 'metar']
        self.logger.debug(f'Result of query:\n{result}')
        self.logger.info('Query for data of stations complete')
        return result
    
    def query_dates(self, stations:List[str], date_from:date, date_to:date) -> Dict[str, np.ndarray[np.datetime64]]:
        self.logger.info(f'Querying datetimes for stations {stations}\n from {date_from} until {date_to}')
        datetime_data = None
        with orm.Session(self.db_engine) as session:
            stmt = (
                db.select(MetarData.station, MetarData.datetime)
                .where(MetarData.station.in_(stations))
                .where(MetarData.datetime >= str(date_from))
                .where(MetarData.datetime < str(date_to))
                .order_by(db.asc(MetarData.station), db.asc(MetarData.datetime))
            )
            datetime_data: db.engine.result.ChunkedIteratorResult = session.execute(stmt)
        # Format output
        self.logger.debug(f'Queried data type: {type(datetime_data)}')
        result: Dict[str, np.ndarray[np.datetime64]] = {}
        for station in stations:
            result[station] = np.array([], dtype='datetime64')
        for station, datetime_observation in datetime_data.tuples():
            assert isinstance(datetime_observation, datetime)
            result[station] = np.append(result[station], np.datetime64(datetime_observation, 'D'))
        for station, datetime_observations in result.items():
            result[station] = np.unique(datetime_observations)
        self.logger.debug(f'Result of query:\n{result}')
        self.logger.info('Query for datetimes of stations complete')
        return result

    def query(self, stations:List[str], date_from:date, date_to:date) -> pd.DataFrame:
        time_start = time.perf_counter()
        stations = StationControl().prepare_stations_for_processing(stations)
        # Create index of what date-range is requested, to be able to use the difference() function
        if date_from == date_to - pd.DateOffset(days=1):
            # Only one day - can not be created with pandas.date_range - create manually
            query_date_range = np.array([date_from], dtype='datetime64[D]')
        else:
            # Ensure consistency by making the date-range an interval with open end
            query_date_range = np.arange(date_from, date_to, dtype='datetime64[D]')
        self.logger.debug(f'Query date range: {query_date_range}')

        # Query what data is already available
        self.logger.info(f'Checking what parts of the data are availble..')
        station_date_sets = self.query_dates(stations, date_from, date_to) # does not work when date_from == date_to
        self.logger.debug(f'Station date sets: {station_date_sets}')

        # Find which date-ranges are missing and should be downloaded
        date_diffs: Dict[str, np.ndarray[np.datetime64]] = {}
        for station, dates in station_date_sets.items():
            date_diffs[station] = np.setdiff1d(query_date_range, dates)
        self.logger.debug(f'Date diffs: {date_diffs}')

        # Decision: Only download data for all stations to reduce number of remote-calls
        date_diffs_values = np.array([], dtype=np.datetime64)
        for station, dates in date_diffs.items():
            date_diffs_values = np.append(date_diffs_values, dates)
        unified_date_diffs = np.unique(date_diffs_values)
        self.logger.debug(f'Dates to query for all selected stations:\n{unified_date_diffs}')

        # Structurize missing date-ranges and download them
        if len(unified_date_diffs) != 0:
            # Missing date-ranges have to be downloaded
            self.logger.info(f'Downloading missing data..')
            # Start with preprocessing
            chunks = DateChunker.build_contiguous_chunks_from_dates(unified_date_diffs)
            self.logger.debug(f'Chunks:\n{chunks}')
            # Make sure chunks can be downloaded - intervals are [x, y) - to include y make interval [x, y + 1)
            chunks = DateChunker.extend_chunks(chunks)
            self.logger.debug(f'Extended chunks:\n{chunks}')
            # Continue with download per chunk
            for chunk in chunks:
                sleep_seconds = 5
                self.logger.debug(f'Sleeping for {sleep_seconds} seconds to reduce load on server..')
                time.sleep(sleep_seconds)
                data = self.download_data(stations, chunk.start, chunk.end)
                printable_subset = data[['station', 'datetime']]
                self.logger.debug(f'Data:\n{printable_subset}')
                for station, dates in station_date_sets.items():
                    data = data.loc[~((data['station'] == station) & (data['datetime'].dt.date.isin(dates)))]
                printable_subset = data[['station', 'datetime']]
                self.logger.debug(f'Remaining data to store:\n{printable_subset}')
                self.store_data(data)
            self.logger.info(f'Data download complete!')
        else:
            self.logger.info(f'All data available!')

        # Finally, query the actual data
        self.logger.info(f'Querying data from database..')
        data = self.query_data(stations, date_from, date_to)
        printable_subset = data[['station', 'datetime']]
        self.logger.debug(f'Data queried (only station and datetime):\n{printable_subset}')
        time_end = time.perf_counter()
        self.logger.info(f'Query took {time_end - time_start} seconds')
        return data