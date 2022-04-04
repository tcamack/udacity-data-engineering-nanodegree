INSERT INTO schools
SELECT DISTINCT ss.opeid6,
                ss.inst_name,
                ss.control,
                ssi.longitude,
                ssi.latitude,
                ssi.street,
                ssi.city,
                ssi.zip
  FROM staging_schools ss
 INNER JOIN staging_school_information ssi
    ON ss.unit_id = ssi.unit_id
 INNER JOIN staging_fips_zip fz
    ON fz.zip = ssi.zip
 INNER JOIN staging_fips_county_state fcs
    ON fz.fips = fcs.fips
 ORDER BY ss.inst_name
    ON CONFLICT DO NOTHING;