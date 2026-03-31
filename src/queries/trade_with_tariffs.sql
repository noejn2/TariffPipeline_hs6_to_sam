-- Example: Join trade data with tariff rates
SELECT
    sr.year,
    sr.product_code,
    sr.partner_name,
    sr.value AS trade_value,
    bt.value AS bound_tariff_pct
FROM ksa_hs_trade.saudi_reporter sr
LEFT JOIN ksa_bound_tariffs.ksa_final_bound_tariffs bt
    ON sr.product_code = bt.product_code
WHERE sr.year = 2021;
