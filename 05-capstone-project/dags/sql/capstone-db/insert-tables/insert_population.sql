INSERT INTO local_population
SELECT DISTINCT sp.fips, sp.population
  FROM staging_population sp
 INNER JOIN staging_fips_county_state fcs
    ON fcs.fips = sp.fips
 WHERE fcs.state != 'NA'
 ORDER BY sp.fips
    ON CONFLICT DO NOTHING;