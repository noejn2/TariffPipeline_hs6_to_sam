-- Example: Filter saudi reporter by year and partner
SELECT *
FROM ksa_hs_trade.saudi_reporter
WHERE year IN (2020, 2021)
AND partner_name = 'China';
