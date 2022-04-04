INSERT INTO local_education
SELECT DISTINCT se.fips,
                se.less_than_hs,
                se.hs_diploma,
                se.some_college,
                se.bachelors_or_higher
  FROM staging_education se
 INNER JOIN staging_fips_county_state fcs
    ON se.fips = fcs.fips
 WHERE fcs.state != 'NA'
 ORDER BY se.fips
    ON CONFLICT DO NOTHING;