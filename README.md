# Capstone Project
## Udacity Data Engineering Nanodegree
### Project description and goals
The goal of this project is to build a data warehouse as a source-of-truth database, allowing a group of climate data scientists to have easier access to the data they need for their climate model and for further insights exploration. They're looking to better undrestand the relationship between world travel, world temperature, and world emissions in the United States.

#### Project steps:
1. Scope the project and gather data
2. Explore, assess, and clean the data
3. Define data model
4. Run ETL to model the data
5. Project write up

## 1. Project Scope and Gathering Data
### Scope of project
This project aims to build an ELT pipeline that extracts raw data from S3, stages the data in Redshift, and then transforms the data into a set of fact and dimension tables accessible to climate data scientists. The two main tools used will be AWS S3 for data storage purposes and AWS Redshift as a warehouse. Data exploration and the development of data processing steps before running the ETL will be run on Python Pandas within a notebook. SQL in Python will be used to create and load tables.

### Data used
Data used are:
1. I94 Immigration Data (SAS): data from the United States National Tourism and Trade Office. It contains information on arrivals in the United States, such as date of arrival, passenger residence country, visa type, and mode of transport.

2. I94 Immigration Data Labels (SAS): data from the United States National Tourism and Trade Office. It is a data dictionary for the I94 Immigration Data to help decode some abbreviations used within the Immigration Data.

3. Monthly Arrivals Data (CSV): data from the United States National Tourism and Trade Office. It contains information on how many arrivals the United States receives per month from countries of the world, from the year 2000 until 2023.

4. World Temperature Data (CSV): data from Kaggle. It contains information on average global land temperature for each city and country in the world starting from the year 1750 until 2013.

5. World Emissions Data (CSV): data from Kaggle. It contains information on average CO2 emissions by country each year from the year 1960 until 2016.

## 2. Explore, Assess, Clean Data
### Data issues
1.I94 Immigration Data
    a. Not all column names are necessarily clear 
    b. Contains unnecessary columns that are empty or are not required in the final tables
    c. Some columns do not have the right data types
    d. Some columns (i.e., 'arrival_date' and 'departure_date' are in SAS date formats
    
2. I94 Immigration Data Labels
    a. Has mulitple data in one file as a data dictionary and must be split
        
3. Arrivals Data
    a. Contains unnecessary rows that are empty or are not required in the final tables
    b. Contains unnecessary columns that are empty or are not required in the final tables
    c. Not all column names are necessarily clear
    d. Columns and rows are difficult to process and therefore must be pivoted to be more consistent
    e. Some columns do not have the right data types
    f. A date column (i.e., 'arrival_date') did not have the correct date format

4. World Temperature Data
    a. Contains temperature data for all cities in the world - only US data is needed
    b. Not all column names are necessarily clear
    
5. World Emissions Data
    a. Contains emissions data for all countries in the world - only US data is needed
    b. Not all column names are necessarily clear
    

### Cleaning steps
1. I94 Immigration Data: all columns will be renamed and those that are not required in the final tables will be dropped. SAS date formats are changed to pandas date format and other columns' data types will be addressed accordingly.
    
2. I94 Immigration Data Labels: this data dictionary will be split into 3 different files - code and country, code and port, and code and state. Code and country contains information on the country equivalent for each code that appears in I94 Immigration Data's 'i94cit' and 'i94res' columns. Code and port contains port equivalent for each code that appears in I94 Immigration Data's 'i94port' column. Code and state contains state equivalent for each code that appears in I94 Immigration Data's 'i94addr' column.
    
3. Arrivals Data: rows and columns that are not required in the final tables will be dropped. Arrivals data's table format is difficult to digest (i.e., columns were made up of 'countries', 'world_region', and all the dates from 2000 until 2023. This data will be pivoted so that the data's columns will only consist of 'countries', 'world_region', 'arrival_date' (with all the former date columns as rows), and 'arrival_total' (with all the total arrivals for each arrival date and for each country). All columns are also renamed to make data more consistent, and all data types will be addressed accordingly.
    
4. World Temperature Data: data will be sectioned to only contain temperature data on United States and its cities. All columns are also renamed to make data more consistent.
    
5. World Emissions Data: data will be sectioned to only contain emissions data on United States and its cities. All columns are also renamed to make data more consistent.

## 3. Define the Data Model
### 3.1 Conceptual data model
Data model can be found in file 'data_model.pdf'

### 3.2 Mapping out data pipelines
1. Data processing is run locally and clean files are uploaded into S3 bucket (s3://bucket-name-here/processed_data/)
2. Staging tables are dropped and then created in Redshift
3. Data from S3 is copied into staging tables
4. Immigration fact table is created
5. Passenger, arrivals, temperature, and emissions dimension tables are created
6. Final tables are uploaded back into S3 bucket (s3://bucket-name-here/final_tables/)
7. Data quality checks are run

## 4. Run Pipelines to Model the Data
### 4.1 Create the data model
create_tables.py and etl.py are the two scripts to be run to create the data model. Below is the code to execute the two scripts. %%time is included to track the time taken for each script to run.

    %%time
    !python create_tables.py

    %%time
    !python etl.py

### 4.2 Data quality checks
Data quality checks will ensure:
1. Fact and dimension tables are not empty after triggering ETL pipeline
2. Records in all fact and dimension tables are unique (there are no duplicates)

data_quality_checks.py is the script to run to perform data quality checks. Below is the code to execute the script. Logging is included to make sure data quality check outcomes are clear.

    from data_quality_checks import data_quality_checks

    data_quality_checks()

### 4.3 Data dictionary
Data dictionary can be found in file 'data_dictionary.pdf'.

## 5. Project Write Up
### 5.1 Rationale for choice of tools and technologies
1. AWS S3: S3 was used for data storage as it is highly scalable and can store large volumes of data. S3 also has a connection to Redshift, which makes querying data simpler and more efficient.
2. AWS Redshift: Redshift was used to stage data and store final tables as it is optimised for handling, querying, and analysing large volumes of data, which this project has. Redshift also has a connection to other AWS services, if the user requires further data handling.
3. Pandas: Pandas was used for the data processing step as it is simple to use and has multiple functionalities that can support data manipulation.
### 5.2 How often data should be updated
All data should be updated on a monthly basis if possible, to ensure data is always up to date for the climate model. Emissions data should be updated yearly. 
### 5.3 Problem scenarios
##### Data was increased by 100x
Redshift is able to handle larger datasets, however an increase in resources might be required. Additionally, using Spark to process larger datasets is recommended.
##### Data populates a dashboard that must be updated on a daily basis by 7am every day
Apache Airflow can be used to support this requirement. Apache Airflow allows the user to determine how frequent they'd like the ETL pipeline to run, and at whatever time they determine.
##### The database needs to be accessed by 100+ people
Redshift is a sufficient enough large-scale database and can be accessed by 100+ people simultaneously. It can support multiple users querying data concurrently
