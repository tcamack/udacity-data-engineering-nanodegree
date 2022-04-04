INSERT INTO courses
SELECT DISTINCT cip_code, cip_desc
  FROM staging_schools
 ORDER BY cip_code
    ON CONFLICT DO NOTHING;