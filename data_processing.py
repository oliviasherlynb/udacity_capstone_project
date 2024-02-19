import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('capstone_project.cfg')

def labels_description_data(file_input, file_output):
    ### Reading data
    labels_data_url = file_input + "I94_SAS_Labels_Descriptions.SAS"
    with open(labels_data_url, 'r') as file:
        lines = file.readlines()
    
    ### Defining which lines belong to which coded column
    country_code_lines = lines[9:298]
    port_code_lines = lines[302:962]
    state_code_lines = lines[981:1036]

    def process_lines_abbreviation(line):
        parts = line.strip().split('=')
        code = parts[0].strip().replace("'", "")
        value = parts[1].strip().replace("'", "")
        return code, value

    def process_lines_value(line):
        parts = line.strip().split('=')
        return parts[0].strip(), parts[1].strip().replace("'", "")

    ### Splitting SAS label file into 3 different dataframes    
    df_countries = pd.DataFrame([process_lines_value(line) for line in country_code_lines], columns=['code', 'country'])
    df_port = pd.DataFrame([process_lines_abbreviation(line) for line in port_code_lines], columns=['code', 'port'])
    df_state = pd.DataFrame([process_lines_abbreviation(line) for line in state_code_lines], columns=['code', 'state'])

    ### Load to processed folder
    df_countries.reset_index().to_csv(file_output + 'code_and_country.csv', index = False)
    df_port.reset_index().to_csv(file_output + 'code_and_port.csv', index = False)
    df_state.reset_index().to_csv(file_output + 'code_and_state.csv', index = False)

def immigration_data(file_input, file_output):
    ### Reading data
    immigration_data_url = file_input + 'immigration_data/18-83510-I94-Data-2016/*'

    dfs = []
    for file in immigration_data_fnames:
        immigration_chunk = pd.read_sas(file, 'sas7bdat', encoding='ISO-8859-1', chunksize=1000)
        dfs.append(immigration_chunk)
    
    immigration_df = pd.concat(dfs, ignore_index=True)
    
    ### Renaming columns
    new_col_names = ['Unnamed: 0', 'cic_id', 'year', 'month', 'citizen_country_code', 'residence_country_code', 'port_arrival_code', 'arrival_date', 'travel_mode', 'state_address_code', 'departure_date', 'passenger_age', 'visa', 'count', 'dtadfile', 'visapost', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'passenger_birth_year', 'dtaddto', 'gender', 'ins_num', 'airline_carrier', 'admission_num', 'flight_num', 'visa_type']
    immigration_df.columns = new_col_names

    ### Dropping columns
    cols_to_drop = ['Unnamed: 0', 'count', 'dtadfile', 'visapost', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'dtaddto']
    immigration_df = immigration_df.drop(columns=cols_to_drop)
    
    ### Ensuring all data types are aligned
    immigration_df.replace([np.inf, -np.inf, 'inf', '-inf', 'NaN'], np.nan, inplace=True)
    immigration_df.fillna(0, inplace=True)

    cols_to_int = ['cic_id', 'year', 'month', 'citizen_country_code', 'residence_country_code', 'travel_mode', 'passenger_age', 'visa', 'passenger_birth_year', 'ins_num', 'admission_num']
    immigration_df[cols_to_int] = immigration_df[cols_to_int].astype(int)

    cols_to_str = ['port_arrival_code', 'state_address_code', 'gender', 'airline_carrier', 'flight_num', 'visa_type']
    immigration_df[cols_to_str] = immigration_df[cols_to_str].astype(str)

    ### Dropping duplicates
    immigration_clean = immigration_df.drop_duplicates()

    ### Changing SAS data format    
    immigration_clean['arrival_date'] = pd.to_datetime('1960-01-01') + pd.to_timedelta(immigration_clean['arrival_date'], unit='D')
    immigration_clean['departure_date'] = pd.to_datetime('1960-01-01') + pd.to_timedelta(immigration_clean['departure_date'], unit='D')

    ### Load to processed folder
    immigration_clean.reset_index().to_csv(file_output + 'immigration_data.csv', index = False)

def arrivals_data(file_input, file_output):
    ### Reading data
    arrivals_data_url = file_input + "arrivals_data.csv"
    arrivals_df = pd.read_csv(arrivals_data_url)

    ### Dropping rows
    arrivals_useful_rows = arrivals_df.iloc[19:254]

    ### Dropping columns
    arrivals_useful_cols = arrivals_useful_rows.drop(arrivals_useful_rows.columns[0], axis=1)
    drop_these_cols = arrivals_useful_cols.columns.get_loc('2023-10')
    arrivals_clean = arrivals_useful_cols.drop(arrivals_useful_cols.columns[drop_these_cols+1:], axis=1)

    ### Renaming columns
    arrivals_final = arrivals_clean.rename(columns={'International Visitors--\n   1) Country of Residence\n   2) 1+ nights in the USA\n   3)  Among qualified visa types': 'country', 'World \nRegion': 'world_region'})
    
    ### Pivoting dataframe
    arrivals_pivoted_df = arrivals_final.melt(id_vars=['country', 'world_region'], var_name='arrival_date', value_name='arrival_total')
    
    ### Ensuring date formats are correct
    arrivals_pivoted_df['arrival_date'] = arrivals_pivoted_df.arrival_date + '-31'
    
    arrivals_pivoted_df['arrival_date'] = arrivals_pivoted_df['arrival_date'].apply(lambda x: '-'.join([part if len(part) > 1 else '0' + part for part in x.split('-')]))
    
    ### Ensuring data types are aligned
    arrivals_pivoted_df['arrival_total'] = arrivals_pivoted_df['arrival_total'].str.replace(',', '')
    arrivals_pivoted_df['arrival_total'] = arrivals_pivoted_df['arrival_total'].replace(' -   ', 0)
    
    arrivals_pivoted_final = arrivals_pivoted_df.fillna(0)
    
    arrivals_pivoted_final['arrival_total'] = arrivals_pivoted_final['arrival_total'].astype(int)

    ### Load to processed folder
    arrivals_pivoted_final.reset_index().to_csv(file_output + 'arrivals_data.csv', index = False)

def temperature_data(file_input, file_output):
    ### Reading data
    temperature_data_url = file_input + "temperature_data.csv"
    temperature_df = pd.read_csv(temperature_data_url)
    
    ### Filtering to United States
    temperature_clean = temperature_df[temperature_df['Country'] == 'United States']

    ### Renaming columns
    renamed_cols = ['date', 'avg_temp', 'avg_temp_uncertainty', 'city', 'country', 'latitude', 'longitude']
    temperature_clean.columns = renamed_cols

    ### Load to processed folder
    temperature_clean.reset_index().to_csv(file_output + 'temperature_data.csv', index = False)

def emissions_data(file_input, file_output):
    ### Reading data
    emissions_data_url = file_input + "co2_emissions_kt_by_country.csv"
    emissions_df = pd.read_csv(emissions_data_url)

    ### Filtering to United States
    emissions_clean = emissions_df[emissions_df['country_name'] == 'United States']

    ### Renaming columns
    emissions_clean = emissions_clean.rename(columns={'value': 'emissions_kt'})

    ### Load to processed folder
    emissions_clean.reset_index().to_csv(file_output + 'emissions_data.csv', index = False)

def data_processing(file_input, file_output):
    labels_description_data(file_input, file_output)
    immigration_data(file_input, file_output)
    arrivals_data(file_input, file_output)
    temperature_data(file_input, file_output)
    emissions_data(file_input, file_output)
