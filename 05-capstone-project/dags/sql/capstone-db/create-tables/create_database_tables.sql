CREATE TABLE IF NOT EXISTS fips(
    fips INT PRIMARY KEY NOT NULL,
    "state" CHAR(2) NOT NULL,
    county VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS zip(
    zip INT PRIMARY KEY NOT NULL,
    fips INT REFERENCES fips NOT NULL
);

CREATE TABLE IF NOT EXISTS schools(
    opeid6 INT PRIMARY KEY NOT NULL,
    inst_name VARCHAR NOT NULL,
    "control" VARCHAR NOT NULL,
    longitude FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    street VARCHAR,
    city VARCHAR NOT NULL,
    zip INT REFERENCES zip NOT NULL
);

CREATE TABLE IF NOT EXISTS courses(
    cip_code INT PRIMARY KEY NOT NULL,
    cip_desc VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS courses_offered(
    opeid6 INT REFERENCES schools,
    cip_code INT REFERENCES courses,
    undergrad_cert BOOLEAN DEFAULT FALSE,
    associates BOOLEAN DEFAULT FALSE,
    bachelors BOOLEAN DEFAULT FALSE,
    post_bacc BOOLEAN DEFAULT FALSE,
    masters BOOLEAN DEFAULT FALSE,
    doctoral BOOLEAN DEFAULT FALSE,
    first_pro_deg BOOLEAN DEFAULT FALSE,
    grad_cert BOOLEAN DEFAULT FALSE,
    PRIMARY KEY(opeid6, cip_code)
);

CREATE TABLE IF NOT EXISTS local_education(
    fips INT PRIMARY KEY REFERENCES fips NOT NULL,
    less_than_hs_pct FLOAT,
    hs_diploma_pct FLOAT,
    some_college_pct FLOAT,
    bachelors_or_higher_pct FLOAT
);

CREATE TABLE IF NOT EXISTS industries(
    naics_code VARCHAR PRIMARY KEY NOT NULL,
    naics_desc VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS local_income(
    fips INT REFERENCES fips,
    naics_code VARCHAR REFERENCES industries,
    units INT NOT NULL,
    employment FLOAT NOT NULL,
    wages FLOAT,
    average_wages FLOAT,
    average_weekly_wages FLOAT,
    PRIMARY KEY(fips, naics_code)
);

CREATE TABLE IF NOT EXISTS local_rent(
    fips INT PRIMARY KEY REFERENCES fips NOT NULL,
    studio INT,
    one_br INT,
    two_br INT,
    three_br INT,
    four_br INT
);

CREATE TABLE IF NOT EXISTS local_population(
    fips INT PRIMARY KEY REFERENCES fips NOT NULL,
    "population" INT
)