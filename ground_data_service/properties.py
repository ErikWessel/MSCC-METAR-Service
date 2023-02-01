from typing import List, Optional

import numpy as np
from aimlsse_api.data.metar import *
from metar import Datatypes, Metar

metar_library_mapping = {
    MetarPropertyType.METAR_CODE                  : 'code',
    MetarPropertyType.REPORT_TYPE                 : 'type',
    MetarPropertyType.REPORT_CORRECTION           : 'correction',
    MetarPropertyType.REPORT_MODE                 : 'mod',
    MetarPropertyType.STATION_ID                  : 'station_id',
    MetarPropertyType.TIME                        : 'time',
    MetarPropertyType.OBSERVATION_CYCLE           : 'cycle',
    MetarPropertyType.WIND_DIRECTION              : 'wind_dir',
    MetarPropertyType.WIND_SPEED                  : 'wind_speed',
    MetarPropertyType.WIND_GUST_SPEED             : 'wind_gust',
    MetarPropertyType.WIND_DIRECTION_FROM         : 'wind_dir_from',
    MetarPropertyType.WIND_DIRECTION_TO           : 'wind_dir_to',
    MetarPropertyType.VISIBILITY                  : 'vis',
    MetarPropertyType.VISIBILITY_DIRECTION        : 'vis_dir',
    MetarPropertyType.MAX_VISIBILITY              : 'max_vis',
    MetarPropertyType.MAX_VISIBILITY_DIRECTION    : 'max_vis_dir',
    MetarPropertyType.TEMPERATURE                 : 'temp',
    MetarPropertyType.DEW_POINT                   : 'dewpt',
    MetarPropertyType.PRESSURE                    : 'press',
    MetarPropertyType.RUNWAY_VISIBILITY           : 'runway',
    MetarPropertyType.CURRENT_WEATHER             : 'weather',
    MetarPropertyType.RECENT_WEATHER              : 'recent',
    MetarPropertyType.SKY_CONDITIONS              : 'sky',
    MetarPropertyType.RUNWAY_WINDSHEAR            : 'windshear',
    MetarPropertyType.WIND_SPEED_PEAK             : 'wind_speed_peak',
    MetarPropertyType.WIND_DIRECTION_PEAK         : 'wind_dir_peak',
    MetarPropertyType.PEAK_WIND_TIME              : 'peak_wind_time',
    MetarPropertyType.WIND_SHIFT_TIME             : 'wind_shift_time',
    MetarPropertyType.MAX_TEMPERATURE_6H          : 'max_temp_6hr',
    MetarPropertyType.MIN_TEMPERATURE_6H          : 'min_temp_6hr',
    MetarPropertyType.MAX_TEMPERATURE_24H         : 'max_temp_24hr',
    MetarPropertyType.MIN_TEMPERATURE_24H         : 'min_temp_24hr',
    MetarPropertyType.PRESSURE_AT_SEA_LEVEL       : 'press_sea_level',
    MetarPropertyType.PRECIPITATION_1H            : 'precip_1hr',
    MetarPropertyType.PRECIPITATION_3H            : 'precip_3hr',
    MetarPropertyType.PRECIPITATION_6H            : 'precip_6hr',
    MetarPropertyType.PRECIPITATION_24H           : 'precip_24hr',
    MetarPropertyType.SNOW_DEPTH                  : 'snowdepth',
    MetarPropertyType.ICE_ACCRETION_1H            : 'ice_accretion_1hr',
    MetarPropertyType.ICE_ACCRETION_3H            : 'ice_accretion_3hr',
    MetarPropertyType.ICE_ACCRETION_6H            : 'ice_accretion_6hr'
}
'''
Maps the METAR properties defined in AIMLSSE-API to the attribute-names of the Metar class
'''

class MetarWrapper:
    '''
    Wraps the library implementation of the METAR data to provide easier access to data using MetarProperty objects.
    '''
    def __init__(self, metar:Metar.Metar) -> None:
        self.metar = metar
    
    def get(self, properties:List[MetarProperty]) -> List[Optional[float]]:
        '''
        Returns the values of the requested properties.

        The output is given in the units that are specified inside the properties.

        Parameters
        ----------
        properties: `List[MetarProperty]`
            The properties to extract from the METAR object
        
        Returns
        -------
        `List[Optional[float]]`
            The values of the requested properties
        '''
        return [self.__get_metar_attr(prop) for prop in properties]

    def __get_metar_attr(self, property:MetarProperty):
        attribute = getattr(self.metar, metar_library_mapping[property.type])
        # Special handling for complex types first
        if property.type == MetarPropertyType.RUNWAY_VISIBILITY:
            # attribute = [entry0[value0, value1], entry1[value0, value1]]
            return [DataRunwayVisibility(
                str(entry[0]),
                self.__parse_value(entry[1], property),
                self.__parse_value(entry[2], property)
            ) for entry in attribute]
        if property.type == MetarPropertyType.CURRENT_WEATHER or property.type == MetarPropertyType.RECENT_WEATHER:
            # attribute = [entry0[value0, value1], entry1[value0, value1]]
            return [DataWeather(
                entry[0],
                entry[1],
                entry[2],
                entry[3],
                entry[4]
            ) for entry in attribute]
        if property.type == MetarPropertyType.SKY_CONDITIONS:
            # attribute = [entry0[value0, value1], entry1[value0, value1]]
            return [DataSkyConditions(
                entry[0],
                self.__parse_value(entry[1], property),
                entry[2]
            ) for entry in attribute]
        # Common handling for all other cases
        if property.type.has_multiple_entries():
            if property.type.uses_multiple_values():
                # attribute = [entry0[value0, value1], entry1[value0, value1]]
                return [[self.__parse_value(value, property) for value in entry] for entry in attribute]
            else:
                # attribute = [entry0, entry1]
                return [self.__parse_value(entry, property) for entry in attribute]
        elif property.type.uses_multiple_values():
            # attribute = [value0, value1]
            return [self.__parse_value(value, property) for value in attribute]
        else:
            # attribute = value
            return self.__parse_value(attribute, property)
    
    def __parse_value(self, value, property:MetarProperty):
        if value is not None:
            if isinstance(value, (Datatypes.distance, Datatypes.precipitation,
                    Datatypes.pressure, Datatypes.speed, Datatypes.temperature)):
                return value.value(property.unit)
            elif isinstance(value, Datatypes.direction):
                return value.value()
            else:
                return value
        return None