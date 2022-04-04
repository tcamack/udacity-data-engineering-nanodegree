INSERT INTO local_rent
SELECT DISTINCT sr.*
  FROM staging_rent sr
 INNER JOIN staging_fips_county_state fcs
    ON sr.fips = fcs.fips
 ORDER BY fips
    ON CONFLICT DO NOTHING;