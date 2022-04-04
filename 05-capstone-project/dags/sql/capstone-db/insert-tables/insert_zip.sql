INSERT INTO zip
SELECT DISTINCT fz.*
  FROM staging_fips_zip fz
 INNER JOIN staging_fips_county_state fcs
    ON fz.fips = fcs.fips
 ORDER BY fz.zip
    ON CONFLICT DO NOTHING;