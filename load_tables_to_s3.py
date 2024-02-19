import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('capstone_project.cfg')

ROLE = config.get('IAM_ROLE', 'ARN')
FINAL_TABLES = config.get('S3', 'FINAL_TABLES')

# TABLES
load_fact_immigration_to_s3 = ("""
    UNLOAD ('SELECT * FROM fact_immigration') 
    TO {}
    CREDENTIALS 'aws_iam_role={}'
    DELIMITER ',' 
    ALLOWOVERWRITE 
    ESCAPE;
    """).format(FINAL_TABLES, ROLE)

load_code_country_to_s3 = ("""
  UNLOAD (SELECT * FROM code_country_table)
  TO {}
  CREDENTIALS 'aws_iam_role={}'
  DELIMITER ','
  ALLOWOVERWRITE
  ESCAPE;
  """).format(FINAL_TABLES, ROLE)

load_code_port_to_s3 = ("""
  UNLOAD (SELECT * FROM code_port_table)
  TO {}
  CREDENTIALS 'aws_iam_role={}'
  DELIMITER ','
  ALLOWOVERWRITE
  ESCAPE;
  """).format(FINAL_TABLES, ROLE)

load_code_state_to_s3 = ("""
  UNLOAD (SELECT * FROM code_state_table)
  TO {}
  CREDENTIALS 'aws_iam_role={}'
  DELIMITER ','
  ALLOWOVERWRITE
  ESCAPE;
  """).format(FINAL_TABLES, ROLE)

load_dim_passenger_to_s3 = ("""
    UNLOAD (SELECT * FROM dim_passenger) 
    TO {}
    CREDENTIALS 'aws_iam_role={}'
    DELIMITER ',' 
    ALLOWOVERWRITE 
    ESCAPE;
    """).format(FINAL_TABLES, ROLE)

load_dim_arrivals_to_s3 = ("""
    UNLOAD (SELECT * FROM dim_arrivals) 
    TO {}
    CREDENTIALS 'aws_iam_role={}'
    DELIMITER ',' 
    ALLOWOVERWRITE 
    ESCAPE;
    """).format(FINAL_TABLES, ROLE)

load_dim_temperature_to_s3 = ("""
    UNLOAD (SELECT * FROM dim_temperature) 
    TO {}
    CREDENTIALS 'aws_iam_role={}'
    DELIMITER ',' 
    ALLOWOVERWRITE 
    ESCAPE;
    """).format(FINAL_TABLES, ROLE)

load_dim_emissions_to_s3 = ("""
    UNLOAD (SELECT * FROM dim_emissions) 
    TO {}
    CREDENTIALS 'aws_iam_role={}'
    DELIMITER ',' 
    ALLOWOVERWRITE 
    ESCAPE;
    """).format(FINAL_TABLES, ROLE)


# QUERY LIST
load_tables_to_s3_queries = [load_fact_immigration_to_s3, load_code_country_to_s3, load_code_port_to_s3, load_code_state_to_s3, load_dim_passenger_to_s3, load_dim_arrivals_to_s3, load_dim_temperature_to_s3, load_dim_emissions_to_s3]
