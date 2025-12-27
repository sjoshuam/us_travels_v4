'''Compiles data about visited metro areas (MSA) into a single msa table'''

########## IMPORTS AND SETTINGS
from pyspark.sql import SparkSession
from pyspark.sql import functions as ps_func
from pyspark.sql import types as ps_types
import pandas as pd
spark = SparkSession.builder.appName('make_travel_data').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

########## DEFINE CORE CLASS

class TravelData:
    '''Core class holding travel data tables'''

    def __init__(self, file_addr: str):

        # Define file source and status log
        self.file_addr = file_addr
        self.status = {'import_data': False, 'refine_place_data': False}
        self.months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

    def __str__(self) -> str:
        return 'TODO: WRITE THIS FUNCTION'
    
    def import_data(self) -> None:
        '''Import raw-ish data from the source spreadsheet'''

        def ReadExcel(dtype, sheet_name:str, file_addr:str=self.file_addr):
            '''make reusable excel-reader function'''
            data_file = pd.read_excel(
                io=file_addr, sheet_name=sheet_name, engine='openpyxl', usecols=dtype)
            return spark.createDataFrame(data_file)

        # Import geographic area data tables
        dtype = {'region_id':str, 'region_name':str, 'is_contiguous':int}
        self.region = ReadExcel(dtype=dtype, sheet_name='area_region')

        dtype = {'state_id':str, 'region_id':str, 'state_name':str, 'statehood':int,
                 'is_state':int, 'capital_msa':str}
        self.state = ReadExcel(dtype=dtype, sheet_name='area_state') 

        # Import msa data tables
        dtype = {'msa_id':str, 'state_id':str, 'msa_name':str, 'is_goal':int,
                 'mega_city':str,'state_capital':str, 'state_largest':int, 'special_interest':str,
                 'population':int} 
        self.msa = ReadExcel(dtype=dtype, sheet_name='msa_roster')

        dtype = {'msa_id':str, 'state_id':str, 'miles_walked':float,
                 'photo_count':int, 'photo_latest':str}
        self.msa_visit = ReadExcel(dtype=dtype, sheet_name='msa_visit')

        dtype = {'msa_id':str, 'state_id':str, 'longitude':float, 'latitude':float}
        self.msa_lonlat = ReadExcel(dtype=dtype, sheet_name='msa_lonlat')

        # Import trip data tables
        dtype = {'trip_id':str, 'trip_name':str, 'trip_start':str, 'trip_end':str}
        self.trip = ReadExcel(dtype=dtype, sheet_name='trip_roster')

        dtype = {'trip_id':str, 'segment_id':str, 'msa_start':str, 'msa_end':str, 'alt_lon':float,
                 'alt_lat':float, 'alt_name':str}
        self.trip_route = ReadExcel(dtype=dtype, sheet_name='trip_route')

        dtype = {'trip_id':str, 'msa_id':str}
        self.trip_visit = ReadExcel(dtype=dtype, sheet_name='trip_visit')

        # Import weather data tables
        months = {i:float for i in self.months}
        dtype = {'msa_id':str, 'state_id':str}
        dtype.update(months)
        self.weather_noon = ReadExcel(dtype=dtype, sheet_name='weather_noon')
        self.weather_dusk = ReadExcel(dtype=dtype, sheet_name='weather_dusk')

        # Update status log
        self.status['import_data'] = True

        return None
    
    def refine_place_data(self) -> None:
        '''Refine all imported data tables'''

        # confirm previous step executed
        assert self.status['import_data'], 'ERROR: Execute .import_data() first'

        ## REFINE GEOGRAPHIC AREA DATA
        # self.region - no refinement needed

        # self.state - nullify -1 statehood values
        self.state = self.state.withColumn('statehood',
            ps_func.when(self.state['statehood'] == -1, None).otherwise(self.state['statehood']))
        
        ## REFINE MSA DATA
        # self.msa - convert nulls to empty strings for goal inclusion criteria
        for i in ['mega_city', 'state_capital', 'state_largest', 'special_interest']:
            self.msa = self.msa.withColumn(
                i, ps_func.when(self.msa[i]=='nan', '').otherwise(self.msa[i]))
            
        # merge datasets into msa with a reusuable function
        for i in [('msa_visit', 'photo_count'), ('msa_lonlat', 'longitude')]:
            self.msa = self.msa.join(getattr(self, i[0]), on=['msa_id','state_id'], how='left')
            self.msa = self.msa.withColumn(
                i[0], ps_func.when(self.msa[i[1]].isNull(), 0).otherwise(1))
            
        # warn if any goal msa are are missing coordinate data
        self.msa = self.msa.withColumn(
            'missing_lonlat', (1 - ps_func.col('msa_lonlat')) * ps_func.col('is_goal')
        )
        missing_lonlat = self.msa.agg(ps_func.sum('missing_lonlat')).collect()[0][0]
        if missing_lonlat > 0:
            print(f'WARNING: {missing_lonlat} goal MSAs are missing coordinate data')
            self.msa.filter(self.msa['missing_lonlat'] == 1).select('msa_id').show(truncate=2**4)
        self.msa = self.msa.drop('missing_lonlat').drop('msa_lonlat')

        # Update status log
        self.status['refine_place_data'] = True
        return None

    def refine_weather_data(self) -> None:
        '''Refine weather data'''

        # merge weather data tables
        self.weather_dusk = self.weather_dusk.melt(
            ids=['msa_id', 'state_id'], values=self.months, variableColumnName='month', valueColumnName='weather_dusk')
        self.weather_noon = self.weather_noon.melt(
            ids=['msa_id', 'state_id'], values=self.months, variableColumnName='month', valueColumnName='weather_noon')
        self.weather = self.weather_dusk.join(self.weather_noon, on=['msa_id','state_id','month'], how='outer')
        self.weather_noon, self.weather_dusk = None, None

        # make weather data relative to ideal condition
        self.weather = self.weather.withColumn(
            'weather_dusk', ps_func.least(ps_func.round(ps_func.col('weather_dusk') - 50.0, 1), ps_func.lit(0.0)))
        self.weather = self.weather.withColumn(
            'weather_noon', ps_func.least(ps_func.round(75.0 - ps_func.col('weather_noon'), 1), ps_func.lit(0.0)))
        self.weather = self.weather.withColumn('weather', ps_func.col('weather_dusk') + ps_func.col('weather_noon'))

        return None
    
    def refine_trip_data(self) -> None:
        '''Refine trip data'''
        print('TODO: WRITE THIS FUNCTION')
        return None
    

    
    def show_data(self) -> None:
        # print values as needed
        self.weather.sort('weather', ascending=False).show(20, truncate=16)
        print(self.weather.count())
        self.weather.printSchema()
        self.weather.filter(self.weather['msa_id']=='Washington DC').sort('weather').show()
        return None

    def make_travel_data(self):
        '''Import and refine all travel data'''
        self.import_data()
        self.refine_place_data()
        self.refine_trip_data()
        self.refine_weather_data()
        self.show_data()


########## TEST EXECUTION
if __name__ == '__main__':
    spark.sparkContext.setLogLevel('ERROR')
    travel_data = TravelData('a_in/us_travels.xlsx').make_travel_data()
    spark.stop()


##########==========##########==========##########==========##########==========##########==========##########==========
##########==========##########==========##########==========##########==========##########==========##########==========
