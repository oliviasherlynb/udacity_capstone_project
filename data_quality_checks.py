import configparser
import logging
import psycopg2


# LOGGER
log = logging.getLogger()
log.setLevel(logging.INFO)

# CONFIG
config = configparser.ConfigParser()
config.read('capstone_project.cfg')


def fact_immigration_checks(cur, conn):
    logging.info("Performing data quality checks on fact_immigration...")
    
    logging.info("Checking if fact_immigration is not an empty table...")
    
    cur.execute("SELECT CASE WHEN EXISTS (SELECT 1 FROM fact_immigration) THEN 'Not Empty' ELSE 'Empty' END AS status")
    conn.commit()
    result = cur.fetchone()[0]
    if result == 'Not Empty':
        logging.info("Data quality check success, fact_immigration not empty.")
    else:
        logging.info("Data quality check failed, fact_immigration is empty.")
        
    logging.info("Checking if fact_immigration has duplicates...")
    
    duplicate_query = ("""
        SELECT cic_id, year, month, port_arrival_code, port_of_arrival, arrival_date, travel_mode, state_address_code, state_name, departure_date, visa, airline_carrier, flight_num, visa_type, COUNT(*)
        FROM fact_immigration
        GROUP BY cic_id, year, month, port_arrival_code, port_of_arrival, arrival_date, travel_mode, state_address_code, state_name, departure_date, visa, airline_carrier, flight_num, visa_type
        HAVING COUNT(*) > 1
        """)
    
    cur.execute(duplicate_query)
    conn.commit()
    duplicate_rows = cur.fetchall()
    if duplicate_rows:
        logging.info("Data quality check failed, fact_immigration has duplicated data.")
    else:
        logging.info("Data quality check success, fact_immigration has no duplicated data.")
    

def dim_passenger_checks(cur, conn):
    logging.info("Performing data quality checks on dim_passenger...")
    
    logging.info("Checking if dim_passenger is not an empty table...")
    
    cur.execute("SELECT CASE WHEN EXISTS (SELECT 1 FROM dim_passenger) THEN 'Not Empty' ELSE 'Empty' END AS status")
    conn.commit()
    result = cur.fetchone()[0]
    if result == 'Not Empty':
        logging.info("Data quality check success, dim_passenger not empty.")
    else:
        logging.info("Data quality check failed, dim_passenger is empty.")
        
    logging.info("Checking if dim_passenger has duplicates...")
    
    duplicate_query = (""" 
        SELECT cic_id, citizen_country_code, citizen_country, residence_country_code, residence_country, passenger_age, passenger_birth_year, gender, ins_num, admission_num, COUNT(*)
        FROM dim_passenger
        GROUP BY cic_id, citizen_country_code, citizen_country, residence_country_code, residence_country, passenger_age, passenger_birth_year, gender, ins_num, admission_num
        HAVING COUNT(*) > 1
        """)
    
    cur.execute(duplicate_query)
    conn.commit()
    duplicate_rows = cur.fetchall()
    if duplicate_rows:
        logging.info("Data quality check failed, dim_passenger has duplicated data.")
    else:
        logging.info("Data quality check success, dim_passenger has no duplicated data.")
    
    

def dim_arrivals_checks(cur, conn):
    logging.info("Performing data quality checks on dim_arrivals...")
    
    logging.info("Checking if dim_arrivals is not an empty table...")
    
    cur.execute("SELECT CASE WHEN EXISTS (SELECT 1 FROM dim_arrivals) THEN 'Not Empty' ELSE 'Empty' END AS status")
    conn.commit()
    result = cur.fetchone()[0]
    if result == 'Not Empty':
        logging.info("Data quality check success, dim_arrivals not empty.")
    else:
        logging.info("Data quality check failed, dim_arrivals is empty.")
        
    logging.info("Checking if dim_arrivals has duplicates...")
    
    duplicate_query = (""" 
        SELECT country, world_region, arrival_date, arrival_total, COUNT(*)
        FROM dim_arrivals
        GROUP BY country, world_region, arrival_date, arrival_total
        HAVING COUNT(*) > 1
        """)
    
    cur.execute(duplicate_query)
    conn.commit()
    duplicate_rows = cur.fetchall()
    if duplicate_rows:
        logging.info("Data quality check failed, dim_arrivals has duplicated data.")
    else:
        logging.info("Data quality check success, dim_arrivals has no duplicated data.")
    
    
    
def dim_temperature_checks(cur, conn):
    logging.info("Performing data quality checks on dim_temperature...")
    
    logging.info("Checking if dim_temperature is not an empty table...")
    
    cur.execute("SELECT CASE WHEN EXISTS (SELECT 1 FROM dim_temperature) THEN 'Not Empty' ELSE 'Empty' END AS status")
    conn.commit()
    result = cur.fetchone()[0]
    if result == 'Not Empty':
        logging.info("Data quality check success, dim_temperature not empty.")
    else:
        logging.info("Data quality check failed, dim_temperature is empty.")
        
    logging.info("Checking if dim_temperature has duplicates...")
    
    duplicate_query = (""" 
        SELECT dt, avg_temp, avg_temp_uncertainty, city, country, latitude, longitude, COUNT(*)
        FROM dim_temperature
        GROUP BY country, dt, avg_temp, avg_temp_uncertainty, city, country, latitude, longitude
        HAVING COUNT(*) > 1
        """)
    
    cur.execute(duplicate_query)
    conn.commit()
    duplicate_rows = cur.fetchall()
    if duplicate_rows:
        logging.info("Data quality check failed, dim_temperature has duplicated data.")
    else:
        logging.info("Data quality check success, dim_temperature has no duplicated data.")
    
    
    
def dim_emissions_checks(cur, conn):
    logging.info("Performing data quality checks on dim_emissions...")
    
    logging.info("Checking if dim_emissions is not an empty table...")
    
    cur.execute("SELECT CASE WHEN EXISTS (SELECT 1 FROM dim_emissions) THEN 'Not Empty' ELSE 'Empty' END AS status")
    conn.commit()
    result = cur.fetchone()[0]
    if result == 'Not Empty':
        logging.info("Data quality check success, dim_emissions not empty.")
    else:
        logging.info("Data quality check failed, dim_emissions is empty.")
        
    logging.info("Checking if dim_emissions has duplicates...")
    
    duplicate_query = (""" 
        SELECT country_code, country, year, emissions_kt, COUNT(*)
        FROM dim_emissions
        GROUP BY country_code, country, year, emissions_kt
        HAVING COUNT(*) > 1
        """)
    
    cur.execute(duplicate_query)
    conn.commit()
    duplicate_rows = cur.fetchall()
    if duplicate_rows:
        logging.info("Data quality check failed, dim_emissions has duplicated data.")
    else:
        logging.info("Data quality check success, dim_emissions has no duplicated data.")
    
    
    
def data_quality_checks():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    fact_immigration_checks(cur, conn)
    dim_passenger_checks(cur, conn)
    dim_arrivals_checks(cur, conn)
    dim_temperature_checks(cur, conn)
    dim_emissions_checks(cur, conn)
    
    logging.info("Data quality checks complete.")
    
    conn.close()                        
