import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('capstone_project.cfg')

ROLE = config.get('IAM_ROLE', 'ARN')
IMMIGRATION_DATA = config.get('S3', 'IMMIGRATION_DATA')
COUNTRY_CODE_DATA = config.get('S3', 'COUNTRY_CODE_DATA')
PORT_CODE_DATA = config.get('S3', 'PORT_CODE_DATA')
STATE_CODE_DATA = config.get('S3', 'STATE_CODE_DATA')
ARRIVALS_DATA = config.get('S3', 'ARRIVALS_DATA')
TEMPERATURE_DATA = config.get('S3', 'TEMPERATURE_DATA')
EMISSIONS_DATA = config.get('S3', 'EMISSIONS_DATA')

# DROP TABLES
staging_immigration_table_drop = "DROP TABLE IF EXISTS staging_immigration"
staging_country_code_table_drop = "DROP TABLE IF EXISTS staging_country_code"
staging_port_code_table_drop = "DROP TABLE IF EXISTS staging_port_code"
staging_state_code_table_drop = "DROP TABLE IF EXISTS staging_state_code"
staging_arrivals_table_drop = "DROP TABLE IF EXISTS staging_arrivals"
staging_temperature_table_drop = "DROP TABLE IF EXISTS staging_temperature"
staging_emissions_table_drop = "DROP TABLE IF EXISTS staging_emissions"
fact_immigration_table_drop = "DROP TABLE IF EXISTS fact_immigration"
dim_passenger_table_drop = "DROP TABLE IF EXISTS dim_passenger"
dim_arrivals_table_drop = "DROP TABLE IF EXISTS dim_arrivals"
dim_temperature_table_drop = "DROP TABLE IF EXISTS dim_temperature"
dim_emissions_table_drop = "DROP TABLE IF EXISTS dim_emissions"

# CREATE TABLES - STAGING TABLES
staging_immigration_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_immigration (
        index INT,
        cic_id INT,
        year INT,
        month INT,
        citizen_country_code INT,
        residence_country_code INT,
        port_arrival_code VARCHAR(255),
        arrival_date DATE,
        travel_mode INT,
        state_address_code VARCHAR(255),
        departure_date DATE,
        passenger_age INT,
        visa INT,
        passenger_birth_year INT,
        gender VARCHAR(255),
        ins_num INT,
        airline_carrier VARCHAR(255),
        admission_num BIGINT,
        flight_num VARCHAR(255),
        visa_type VARCHAR(255)
        );
""")

staging_country_code_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_country_code (
        index INT,
        code INT,
        country VARCHAR(255)
        );
""")

staging_port_code_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_port_code (
        index INT,
        code VARCHAR(255),
        port VARCHAR(255)
        );
""")

staging_state_code_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_state_code (
        index INT,
        code VARCHAR(255),
        state VARCHAR(255)
        );
""")

staging_arrivals_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_arrivals (
        index INT,
        country VARCHAR(255),
        world_region VARCHAR(255),
        arrival_date DATE,
        arrival_total INT
        );
""")

staging_temperature_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_temperature (
        index INT,
        dt DATE,
        avg_temp DECIMAL,
        avg_temp_uncertainty DECIMAL,
        city VARCHAR(255),
        country VARCHAR(255),
        latitude VARCHAR(255),
        longitude VARCHAR(255)
        );
""")

staging_emissions_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_emissions (
        index INT,
        country_code VARCHAR(255),
        country VARCHAR(255),
        year INT,
        emissions_kt DECIMAL(10,3)
        );
""")

# CREATE TABLES - FACT TABLE
fact_immigration_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_immigration (
        immigration_id INT IDENTITY(1,1) PRIMARY KEY,
        cic_id INT,
        year INT,
        month INT,
        port_arrival_code VARCHAR(255),
        port_of_arrival VARCHAR(255),
        arrival_date DATE,
        travel_mode INT,
        state_address_code VARCHAR(255),
        state_name VARCHAR(255),
        departure_date DATE,
        visa INT,
        airline_carrier VARCHAR(255),
        flight_num VARCHAR(255),
        visa_type VARCHAR
        );
""")

# CREATE TABLES - DIMENSTION TABLE
code_country_table_create = ("""
    CREATE TABLE IF NOT EXISTS code_country_table (
        index INT IDENTITY(1,1) PRIMARY KEY,
        code INT,
        country VARCHAR(255)
        );
""")

code_port_table_create = ("""
    CREATE TABLE IF NOT EXISTS code_port_table (
        index INT IDENTITY(1,1) PRIMARY KEY,
        code VARCHAR(255),
        port VARCHAR(255)
        );
""")

code_state_table_create = ("""
    CREATE TABLE IF NOT EXISTS code_state_table (
        index INT IDENTITY(1,1) PRIMARY KEY,
        code VARCHAR(255),
        state VARCHAR(255)
        );
""")

dim_passenger_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_passenger (
        passenger_id INT IDENTITY(1,1) PRIMARY KEY,
        cic_id INT,
        citizen_country_code INT,
        citizen_country VARCHAR(255),
        residence_country_code INT,
        residence_country VARCHAR(255),
        passenger_age INT,
        passenger_birth_year INT,
        gender VARCHAR(255),
        ins_num INT,
        admission_num BIGINT
        );
""")

dim_arrivals_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_arrivals (
        arrival_id INT IDENTITY(1,1) PRIMARY KEY,
        country VARCHAR(255),
        world_region VARCHAR(255),
        arrival_date DATE,
        arrival_total INT
        );
""")

dim_temperature_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_temperature (
        temperature_id INT IDENTITY(1,1) PRIMARY KEY,
        dt DATE,
        avg_temp DECIMAL,
        avg_temp_uncertainty DECIMAL,
        city VARCHAR(255),
        country VARCHAR(255),
        latitude VARCHAR(255),
        longitude VARCHAR(255)
        );
""")

dim_emissions_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_emissions (
        emissions_id INT IDENTITY(1,1) PRIMARY KEY,
        country_code VARCHAR(255),
        country VARCHAR(255),
        year INT,
        emissions_kt DECIMAL(10,3)
        );
""")

# STAGING TABLE
staging_country_code_copy = ("""
    COPY staging_country_code 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    CSV
    REGION 'us-east-1'
    IGNOREHEADER 1;
    """).format(COUNTRY_CODE_DATA, ROLE)

staging_port_code_copy = ("""
    COPY staging_port_code 
    FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    CSV
    REGION 'us-east-1'
    IGNOREHEADER 1;
    """).format(PORT_CODE_DATA, ROLE)

staging_state_code_copy = ("""
    COPY staging_state_code 
    FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    CSV
    REGION 'us-east-1'
    IGNOREHEADER 1;
    """).format(STATE_CODE_DATA, ROLE)

staging_arrivals_copy = ("""
    COPY staging_arrivals 
    FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    CSV
    REGION 'us-east-1'
    IGNOREHEADER 1;
    """).format(ARRIVALS_DATA, ROLE)

staging_temperature_copy = ("""
    COPY staging_temperature 
    FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    CSV
    REGION 'us-east-1'
    IGNOREHEADER 1;
    """).format(TEMPERATURE_DATA, ROLE)

staging_emissions_copy = ("""
    COPY staging_emissions
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    CSV
    REGION 'us-east-1'
    IGNOREHEADER 1;
    """).format(EMISSIONS_DATA, ROLE)

staging_immigration_copy = ("""
    COPY staging_immigration 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    CSV
    REGION 'us-east-1'
    IGNOREHEADER 1;
    """).format(IMMIGRATION_DATA, ROLE)

# FACT AND DIMENSION TABLES
fact_immigration_table_insert = ("""
    INSERT INTO fact_immigration (
        cic_id,
        year,
        month,
        port_arrival_code,
        port_of_arrival,
        arrival_date,
        travel_mode,
        state_address_code,
        state_name,
        departure_date,
        visa,
        airline_carrier,
        flight_num,
        visa_type)
    
    SELECT DISTINCT
        i.cic_id,
        i.year,
        i.month,
        i.port_arrival_code,
        p.port as port_of_arrival,
        i.arrival_date,
        i.travel_mode,
        i.state_address_code,
        s.state as state_name,
        i.departure_date,
        i.visa,
        i.airline_carrier,
        i.flight_num,
        i.visa_type
     
     FROM staging_immigration i
     JOIN staging_port_code p ON i.port_arrival_code = p.code
     JOIN staging_state_code s ON i.state_address_code = s.code;
     """)

dim_passenger_table_insert = ("""
    INSERT INTO dim_passenger (
        cic_id,
        citizen_country_code,
        citizen_country,
        residence_country_code,
        residence_country,
        passenger_age,
        passenger_birth_year,
        gender,
        ins_num,
        admission_num)
    
    SELECT DISTINCT
        i.cic_id,
        i.citizen_country_code,
        c.country AS citizen_country,
        i.residence_country_code,
        c.country AS residence_country,
        i.passenger_age,
        i.passenger_birth_year,
        i.gender,
        i.ins_num,
        i.admission_num
    
    FROM staging_immigration i
    JOIN staging_country_code c ON ((i.citizen_country_code = c.code) AND (i.residence_country_code = c.code));
    """)

code_country_table_insert = ("""
    INSERT INTO code_country_table (
        code,
        country)
    
    SELECT DISTINCT
        code,
        country
    
    FROM staging_country_code;
    """)

code_port_table_insert = ("""
    INSERT INTO code_port_table (
        code,
        port)
    
    SELECT DISTINCT
        code,
        port
    
    FROM staging_port_code;
    """)

code_state_table_insert = ("""
    INSERT INTO code_state_table (
        code,
        state)
    
    SELECT DISTINCT
        code,
        state
    
    FROM staging_state_code;
    """)

dim_arrivals_table_insert = ("""
    INSERT INTO dim_arrivals (
        country,
        world_region,
        arrival_date,
        arrival_total)
        
    SELECT DISTINCT
        country,
        world_region,
        arrival_date,
        arrival_total
     
    FROM staging_arrivals;
    """)

dim_temperature_table_insert = ("""
    INSERT INTO dim_temperature (
        dt,
        avg_temp,
        avg_temp_uncertainty,
        city,
        country,
        latitude,
        longitude)
    
    SELECT DISTINCT
        dt AS date,
        avg_temp,
        avg_temp_uncertainty,
        city,
        country,
        latitude,
        longitude
    
    FROM staging_temperature;
    """)

dim_emissions_table_insert = ("""
    INSERT INTO dim_emissions (
        country_code,
        country,
        year,
        emissions_kt)

    SELECT DISTINCT
        country_code,
        country,
        year,
        emissions_kt
    
    FROM staging_emissions;
    """)

# QUERY LISTS
create_table_queries = [staging_immigration_table_create, staging_country_code_table_create, staging_port_code_table_create, staging_state_code_table_create, staging_arrivals_table_create, staging_temperature_table_create, staging_state_code_table_create, staging_arrivals_table_create, staging_temperature_table_create, staging_emissions_table_create, fact_immigration_table_create, code_country_table_create, code_port_table_create, code_state_table_create, dim_passenger_table_create, dim_arrivals_table_create, dim_temperature_table_create, dim_emissions_table_create]

drop_table_queries = [staging_immigration_table_drop, staging_country_code_table_drop, staging_port_code_table_drop, staging_state_code_table_drop, staging_arrivals_table_drop, staging_temperature_table_drop, staging_emissions_table_drop, fact_immigration_table_drop, dim_passenger_table_drop, dim_arrivals_table_drop, dim_temperature_table_drop, dim_emissions_table_drop]

copy_table_queries = [staging_immigration_copy, staging_country_code_copy, staging_port_code_copy, staging_state_code_copy, staging_arrivals_copy, staging_temperature_copy, staging_emissions_copy]

insert_table_queries = [fact_immigration_table_insert, code_country_table_insert, code_port_table_insert, code_state_table_insert, dim_passenger_table_insert, dim_arrivals_table_insert, dim_temperature_table_insert, dim_emissions_table_insert]
