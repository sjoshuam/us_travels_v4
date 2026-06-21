##########==========##########==========##########==========##########==========##########==========
## prepare environment

import pandas as pd
pd.set_option('display.max_colwidth', 16)
pd.set_option('display.expand_frame_repr', True)

##########==========##########==========##########==========##########==========##########==========
## define functions

class MainDataset:

    def __init__(self, file_path='a_in/us_travels.xlsx'):
        '''Initialize primary dataset object'''
        self.file_path = file_path
        self.data = None

    ## READ IN DATA
        
    def read_area_region(self):
        '''Read the Area Region tab'''
        self.area_region = pd.read_excel(self.file_path, sheet_name='area_region')
        self.area_region['is_contiguous'] = self.area_region['is_contiguous'].astype(bool)
        return self

    def read_area_state(self):
        '''Read the Area State tab'''
        self.area_state = pd.read_excel(self.file_path, sheet_name='area_state')
        self.area_state['is_state'] = self.area_state['is_state'].astype(bool)
        return self
    
    def read_msa_roster(self):
        '''Read and clean MSA Roster tab'''
        msa_roster = pd.read_excel(self.file_path, sheet_name='msa_roster')
        for i in ['mega_city', 'state_capital', 'state_largest']:
                  msa_roster[i] = msa_roster[i].isna()
        msa_roster['is_goal'] = msa_roster['is_goal'].astype(bool)
        self.msa_roster = msa_roster
        return self
    
    def read_msa_visit(self):
        '''Read and clean MSA Visit tab'''
        msa_visit = pd.read_excel(self.file_path, sheet_name='msa_visit', dtype={'photo_latest': str})
        self.msa_visit = msa_visit
        return self
    
    def read_msa_lonlat(self):
        '''Read and clean MSA LonLat tab'''
        self.msa_lonlat = pd.read_excel(self.file_path, sheet_name='msa_lonlat')
        return self
    
    def read_weather_noon(self):
        '''Read and clean Weather Noon tab'''
        self.weather_noon = pd.read_excel(self.file_path, sheet_name='weather_noon')
        self.weather_noon.set_index(['state_id', 'msa_id'], inplace=True)
        return self
    
    def read_weather_dusk(self):
        '''Read and clean Weather Dusk tab'''
        self.weather_dusk = pd.read_excel(self.file_path, sheet_name='weather_dusk')
        self.weather_dusk.set_index(['state_id', 'msa_id'], inplace=True)
        return self
    
    def read_data(self):
        '''Read all tabs'''
        self.read_area_region()
        self.read_area_state()
        self.read_msa_roster()
        self.read_msa_visit()
        self.read_msa_lonlat()
        self.read_weather_noon()
        self.read_weather_dusk()
        return self
    
    ## SCORE WEATHER CONDITIONS
    def score_weather(self):
        '''Score weather conditions for each MSA in each month'''
        ones = self.weather_noon.apply(lambda x: x - x + 1)
        keep_negatives = lambda x: x * (x < 0)
        low  = keep_negatives(self.weather_dusk - (ones * 45))
        high = keep_negatives((ones * 79) - self.weather_noon)
        negatives = low + high
        del low, high, keep_negatives, ones
        score = self.weather_noon - self.weather_dusk
        score = (score + negatives) / score
        score = (score * (score > 0)).round(2)
        self.weather_score = score
        return self
    
    ## COMPILE AND AGGREGATE DATA

    def compile_msa_data(self):
        '''Compile all MSA data into a single dataframe'''

        ## Create QA tracking object
        qa_statistics = {'MSA': self.msa_roster.shape[0]} # 387 MSA + 7 rural caps + San Juan = 395

        ## Retain only MSAs that are roadtrip goals (and do QA checks)
        msa_data = self.msa_roster.loc[self.msa_roster['is_goal']].copy()
        msa_data = msa_data.drop(columns='is_goal').sort_values(['state_id', 'msa_id'])
        assert msa_data.drop_duplicates().shape[0] == msa_data.shape[0], 'Duplicates detected'
        qa_statistics.update({'Goal':msa_data.shape[0]})
        del self.msa_roster

        ## Merge in coordinate and elevation data
        msa_data = msa_data.merge(self.msa_lonlat, on=['state_id', 'msa_id'], how='left')
        qa_statistics.update({'Coord': sum(msa_data['longitude'].notna())})
        del self.msa_lonlat

        ## Merge in data for visited MSAs
        msa_data = msa_data.merge(self.msa_visit, on=['state_id', 'msa_id'], how='left')
        msa_data = msa_data.assign(is_visited=msa_data['is_legacy'].notna())
        msa_data = msa_data.assign(is_legacy=msa_data['is_legacy'].fillna(0).astype(bool))
        qa_statistics.update({ 'Visited':sum(msa_data['is_legacy']) })
        qa_statistics.update({ 'Pictured':sum(msa_data['photo_latest'].notna()) })
        qa_statistics.update({ 'Visited':sum([qa_statistics[i] for i in ['Visited', 'Pictured']]) })
        del self.msa_visit

        ## Add in regional id foreign key, sort columns
        msa_data = msa_data.merge(self.area_state[['state_id', 'region_id']], how='left', on='state_id')
        msa_data = msa_data.sort_values(['state_id', 'msa_id'])
        qa_statistics.update({ 'Final': msa_data.shape[0] })
        self.msa_data = msa_data
        
        ## display qa statistics
        print('\n---- MSA QA Statistics ----')
        print(qa_statistics)
        del msa_data
        return self.msa_data



    ## TODO: state, region summaries



    def display_stuff(self):
        '''Temporarily display useful info'''
        print(self.msa_data.T.head(10))
        #print(self.area_region)
        #print(self.area_state)
        #print(self.msa_roster)
        #print(self.msa_visit)
        #print(self.msa_lonlat)
        #print(self.weather_noon)
        #print(self.weather_dusk)
        #print(self.weather_score)
        return None
        

##########==========##########==========##########==========##########==========##########==========
## execute functions

## area_region, area_state, msa_roster, msa_visit, msa_lonlat, weather_noon, weather_dusk

if __name__ == "__main__":
    main_dataset = MainDataset()
    main_dataset.read_data()

    main_dataset.score_weather()

    main_dataset.compile_msa_data()

    main_dataset.display_stuff()


##########==========##########==========##########==========##########==========##########==========
