INSERT INTO courses_offered
SELECT DISTINCT ss.opeid6,
                ss.cip_code
  FROM staging_schools ss
 INNER JOIN staging_school_information ssi
    ON ss.unit_id = ssi.unit_id
 INNER JOIN staging_fips_zip fz
    ON fz.zip = ssi.zip
 INNER JOIN staging_fips_county_state fcs
    ON fz.fips = fcs.fips
 ORDER BY ss.opeid6, ss.cip_code
    ON CONFLICT DO NOTHING;