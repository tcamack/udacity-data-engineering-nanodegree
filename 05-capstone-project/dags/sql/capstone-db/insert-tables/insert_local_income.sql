INSERT INTO local_income
SELECT DISTINCT si.fips,
                si.naics_code,
                si.units,
                si.employment,
                si.wages,
                si.average_wages,
                si.average_weekly_wages
  FROM staging_income si
 INNER JOIN staging_fips_county_state fcs
    ON si.fips = fcs.fips
 ORDER BY si.fips, si.naics_code
    ON CONFLICT DO NOTHING;