INSERT INTO industries
SELECT DISTINCT naics_code, naics_desc
  FROM staging_income
    ON CONFLICT DO NOTHING;