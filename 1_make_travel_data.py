'''Compiles data about visited metro areas (MSA) into a single msa table'''

########## IMPORTS AND SETTINGS
from pyspark.sql import SparkSession
import pyspark.pandas as ps
spark = SparkSession.builder.appName('make_travel_data').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

########## DEFINE CORE CLASS

class TravelData:
    '''Core class holding travel data tables'''

    def __init__(self, file_addr: str):

        # Define data slots
        self.file_addr = file_addr
        self.region, self.state = None, None
        self.trip, self.segment = None, None
        self.msa = None
        self.noon_temp, self.dusk_temp = None, None

        # Define status log
        self.status = {
            'import_raw_data': False, 'refine_area_data': False,
            'refine_msa_data': False, 'refine_weather_data': False,
            }

    def __str__(self) -> str:
        return 'TODO: WRITE THIS FUNCTION'
    
    def import_raw_data(self):
        '''Import raw-ish data from the source spreadsheet'''

        # temporary
        x = ps.read_excel(
            io=self.file_addr, sheet_name=None, dtype=str, engine='openpyxl')
        print('[TABS]=========: ', x.keys())

        # Import geographic area data tables
        self.region = ps.read_excel(
            io=self.file_addr, sheet_name='region', dtype=str, engine='openpyxl')
        self.state = ps.read_excel(
            io=self.file_addr, sheet_name='state', dtype=str, engine='openpyxl')

        # Import trip data tables
        self.trip = ps.read_excel(
            io = self.file_addr, sheet_name='trips', dtype=str, engine='openpyxl')
        self.segment = ps.read_excel(
            io = self.file_addr, sheet_name='segmentsTODO', dtype=str, engine='openpyxl')
        
        # Import msa data tables - travels, goals, coordinates
        # TODO

        # Import weather data tables
        self.noon_temp = ps.read_excel(
            io=self.file_addr, sheet_name='msa_temp_noon', dtype=str)
        self.dusk_temp = ps.read_excel(
            io=self.file_addr, sheet_name='msa_temp_dusk', dtype=str)
        
        print(self.state)
        return None

########## DEFINE MAIN EXECUTION FUNCTION

########## TEST EXECUTION
if __name__ == '__main__':
    spark.sparkContext.setLogLevel('ERROR')
    travel_data = TravelData('a_in/us_travels.xlsx')
    travel_data.import_raw_data()
    spark.stop()

##########==========##########==========##########==========##########==========
##########==========##########==========##########==========##########==========
