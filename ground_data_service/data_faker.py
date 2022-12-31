import datetime
import itertools
import logging
import os
import random
from typing import List, Tuple

import numpy as np
import pandas as pd


class GroundDataFaker:
    def generateMeasurements(self, datetime_from:datetime.datetime, datetime_to:datetime.datetime, out_path:str):
        def generate_coordinates() -> Tuple[float, float]:
            z = random.uniform(-1.0, 1.0) # offset from the center (0) of the sphere to one of the poles
            phi = random.uniform(-np.pi, np.pi)
            latitude = np.rad2deg(np.arcsin(z))
            longitude = np.rad2deg(phi)
            return (latitude, longitude)

        def generate_sensors(num_sensors:int) -> List[Tuple[float, float]]:
            return [ generate_coordinates() for i in range(num_sensors) ]

        def generate_temperature() -> float:
            return random.uniform(-20.0, 40.0) # degrees celsius

        logging.info("Creating fake measurements..")
        dates = pd.date_range(str(datetime_from), str(datetime_to), freq='D')
        num_dates = len(dates)
        num_sensors = 12
        sensors = generate_sensors(num_sensors)
        data = {}
        data['date'] = list(itertools.chain.from_iterable([ [date.date().isoformat()] * num_sensors for date in dates ]))
        data['latitude'] = [ sensor[0] for sensor in sensors ] * num_dates
        data['longitude'] = [ sensor[1] for sensor in sensors ] * num_dates
        num_measurements = num_dates * num_sensors
        data['temperature'] = [ generate_temperature() for i in range(num_measurements) ]
        df = pd.DataFrame(data)
        logging.info("Creation of fake measurements complete!")
        logging.debug(df)

        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        df.to_csv(out_path, index=False)