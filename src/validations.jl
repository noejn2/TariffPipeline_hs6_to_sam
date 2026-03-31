"""
    validate_trade_inputs(trade_df, countries, year, is_world)

Check that the requested `countries` and `year` exist in the trade DataFrame.
Skips country validation when `is_world` is true.
Throws an error with available values if validation fails.
"""
function validate_trade_inputs(trade_df::DataFrame, countries::Vector{<:AbstractString}, year::Int, is_world::Bool)
    valid_countries = unique(trade_df.partner_name)
    valid_years = unique(trade_df.year)

    if !is_world
        for c in countries
            if !(c in valid_countries)
                error("Country '$c' not found in trade data. Available: $(join(sort(collect(valid_countries)), ", "))")
            end
        end
    end

    if !(year in valid_years)
        error("Year $year not found in trade data. Available: $(sort(collect(valid_years)))")
    end
end

"""
    validate_tariff_data(tariff_df, countries)

Validate the tariff DataFrame schema and contents:
- Columns must be exactly: `indicator`, `partner_name`, `product_code`, `value`.
- `indicator` values must be `"Imports"` and/or `"Exports"`.
- `partner_name` values must be `"WORLD"` or one of the requested `countries`.
- `product_code` values must be 6-character HS6 codes.
"""
function validate_tariff_data(tariff_df::DataFrame, countries::Vector{<:AbstractString})
    required_cols = Set(["indicator", "partner_name", "product_code", "value"])
    actual_cols = Set(names(tariff_df))
    if actual_cols != required_cols
        extra = setdiff(actual_cols, required_cols)
        missing_c = setdiff(required_cols, actual_cols)
        msg = "Tariff data must have exactly columns: indicator, partner_name, product_code, value."
        !isempty(missing_c) && (msg *= " Missing: $(join(missing_c, ", ")).")
        !isempty(extra) && (msg *= " Unexpected: $(join(extra, ", ")).")
        error(msg)
    end

    invalid_ind = setdiff(Set(unique(tariff_df.indicator)), Set(["Imports", "Exports"]))
    if !isempty(invalid_ind)
        error("Tariff 'indicator' must be \"Imports\" and/or \"Exports\". Found invalid: $(join(invalid_ind, ", "))")
    end

    invalid_partners = setdiff(Set(unique(tariff_df.partner_name)), Set(["WORLD"; countries]))
    if !isempty(invalid_partners)
        error("Tariff 'partner_name' must be \"WORLD\" or one of ($(join(countries, ", "))). Found invalid: $(join(invalid_partners, ", "))")
    end

    bad_codes = filter(r -> length(string(r.product_code)) != 6, eachrow(tariff_df))
    if length(bad_codes) > 0
        samples = [r.product_code for r in first(bad_codes, 5)]
        error("Tariff 'product_code' must be 6 characters. Found $(length(bad_codes)) invalid, e.g.: $(join(samples, ", "))")
    end
end

"""
    validate_cpc_sam_path(cpc_sam_path)

When `cpc_sam_path` is a String, verify that the file exists and has a `.json` extension.
No-op when a pre-loaded Dict is passed.
"""
function validate_cpc_sam_path(cpc_sam_path)
    if cpc_sam_path isa String
        if !isfile(cpc_sam_path)
            error("CPC→SAM mapping file not found: $cpc_sam_path")
        end
        if !endswith(cpc_sam_path, ".json")
            error("CPC→SAM mapping must be a JSON file, got: $cpc_sam_path")
        end
    end
end
