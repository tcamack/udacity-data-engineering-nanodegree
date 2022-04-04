INSERT INTO fips
SELECT DISTINCT fips, "state", county
  FROM staging_fips_county_state
 WHERE "state" != 'NA'
 ORDER BY fips
    ON CONFLICT DO NOTHING;