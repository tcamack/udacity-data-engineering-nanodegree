CREATE TABLE IF NOT EXISTS staging_schools(
    unit_id FLOAT,
    opeid6 INT,
    inst_name VARCHAR,
    "control" VARCHAR,
    cip_code INT,
    cip_desc VARCHAR,
    cred_level INT,
    cred_desc VARCHAR
);

CREATE TABLE IF NOT EXISTS staging_income(
    fips INT,
    state_fips INT,
    county_fips INT,
    "location" VARCHAR,
    "year" INT,
    naics_code VARCHAR,
    naics_desc VARCHAR,
    units FLOAT,
    employment FLOAT,
    wages FLOAT,
    average_wages FLOAT,
    average_weekly_wages FLOAT
);

CREATE TABLE IF NOT EXISTS staging_fips_zip(
    zip INT,
    fips INT
);

CREATE TABLE IF NOT EXISTS staging_school_information(
    longitude FLOAT,
    latitude FLOAT,
    unit_id INT,
    inst_name VARCHAR,
    street VARCHAR,
    city VARCHAR,
    "state" VARCHAR,
    zip INT
);

CREATE TABLE IF NOT EXISTS staging_education(
    fips INT,
    "state" VARCHAR,
    county VARCHAR,
    less_than_hs FLOAT,
    hs_diploma FLOAT,
    some_college FLOAT,
    bachelors_or_higher FLOAT
);

CREATE TABLE IF NOT EXISTS staging_fips_county_state(
    fips INT,
    county VARCHAR,
    "state" VARCHAR
);

CREATE TABLE IF NOT EXISTS staging_rent(
    fips INT,
    studio INT,
    one_br INT,
    two_br INT,
    three_br INT,
    four_br INT
);

CREATE TABLE IF NOT EXISTS staging_population(
    fips INT,
    "population" INT
)