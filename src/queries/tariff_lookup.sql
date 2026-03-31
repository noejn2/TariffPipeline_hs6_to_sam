-- Example: Look up bound tariff rates for specific products
SELECT *
FROM ksa_bound_tariffs.ksa_final_bound_tariffs
WHERE product_code IN ('010121', '010129', '010130');
