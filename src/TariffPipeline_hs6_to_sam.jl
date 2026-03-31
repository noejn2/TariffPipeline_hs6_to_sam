module TariffPipeline_hs6_to_sam

using DataFrames, Parquet2, JSON3, Statistics

export hs6_to_sam_pipeline, load_trade, check_partners

include("loaders.jl")
include("transforms.jl")
include("validations.jl")
include("run_pipeline.jl")

# ─── Default paths ────────────────────────────────────────────────────────────

const TRADE_PATH = joinpath(@__DIR__, "..", "data", "saudi_reporter.parquet")
const HS6_CPC_PATH = joinpath(@__DIR__, "..", "assets", "mappings", "hs6_to_cpc", "hs6_cpc_concordance.parquet")
const DEFAULT_CPC_SAM_MAP = joinpath(@__DIR__, "..", "assets", "mappings", "cpc_to_sam", "SAMv3-A21-C22-L6-cmap.json")
const SNA_PATH = joinpath(@__DIR__, "..", "assets", "mappings", "hs_sitc_bec", "hs6_to_sna.json")
const DEFAULT_OUTPUT_DIR = joinpath(@__DIR__, "..", "output")

# ─── High-level entry point ───────────────────────────────────────────────────

"""
    hs6_to_sam_pipeline(countries_raw, year; tariff_data, cpc_sam_map, write_json, consider_intermediates) -> Dict

High-level wrapper around `run_pipeline`. Validates inputs, runs the full
HS6 → CPC → SAM pipeline, and returns a Dict with keys `"data"`, `"imports"`,
`"exports"`, and `"intermediate_hs6"`.

# Arguments
- `countries_raw`: countries as a comma-separated string (`"CHINA,JAPAN"`), a String (`"CHINA"`), a Vector (`["CHINA", "JAPAN"]`), or `"WORLD"`
- `year`: trade year, e.g. `2023`
- `tariff_data`: path to a tariff parquet file, or a pre-loaded DataFrame. Default (`nothing`) builds a uniform 100% tariff for all HS6 codes.
- `cpc_sam_map`: path to the CPC→SAM JSON mapping (default: SAMv3-A21-C22-L6)
- `write_json`: if `true`, write the output Dict as a JSON file to `output/`. Default `false`.
- `consider_intermediates`: if `true` (default), zero out tariffs for intermediate goods (BEC/SNA). Set to `false` to skip this step.
"""
function hs6_to_sam_pipeline(
    countries_raw::Union{String,Vector{String}},
    year::Int,
    cpc_sam_map::Union{Dict,String,Nothing}=DEFAULT_CPC_SAM_MAP;
    tariff_data::Union{DataFrame,String,Nothing}=nothing,
    write_json::Bool=false,
    consider_intermediates::Bool=true,
)::Dict
    cpc_sam_map = isnothing(cpc_sam_map) ? DEFAULT_CPC_SAM_MAP : cpc_sam_map

    countries = countries_raw isa Vector ? strip.(countries_raw) : [strip(c) for c in split(countries_raw, ",")]
    is_world = length(countries) == 1 && countries[1] == "WORLD"

    # --- Load and validate trade inputs ---
    trade_df = load_trade(TRADE_PATH)
    validate_trade_inputs(trade_df, countries, year, is_world)

    # --- Build or load tariff data ---
    if isnothing(tariff_data)
        tariff_df = build_uniform_tariffs(trade_df, year)
        println("Using default uniform 100% tariffs ($(nrow(tariff_df)) rows)")
    else
        tariff_df = tariff_data isa DataFrame ? tariff_data : load_tariffs(tariff_data)
    end
    validate_tariff_data(tariff_df, countries)

    # --- Validate CPC→SAM mapping ---
    validate_cpc_sam_map(cpc_sam_map)

    println("Running pipeline: countries=$(join(countries, ",")), year=$year, tariff=$(isnothing(tariff_data) ? "<uniform 100%>" : tariff_data isa DataFrame ? "<DataFrame>" : tariff_data), cpc_sam=$cpc_sam_map")

    # --- Run pipeline ---
    result, intermediate_hs6 = run_pipeline(;
        trade_path=TRADE_PATH,
        tariff_data=tariff_df,
        hs6_cpc_path=HS6_CPC_PATH,
        cpc_sam_map=cpc_sam_map,
        sna_path=SNA_PATH,
        consider_intermediates=consider_intermediates,
        years=[year],
        partners=is_world ? nothing : countries,
    )

    # --- Aggregate across countries ---
    if is_world || length(countries) > 1
        result = aggregate_countries(result)
        result.partner_name .= is_world ? "WORLD" : "AGG_COUNTRY"
    end

    # --- Coalesce missing → 0 ---
    for col in names(result)
        if eltype(result[!, col]) >: Missing
            result[!, col] = coalesce.(result[!, col], 0.0)
        end
    end

    println("Result: $(size(result, 1)) rows × $(size(result, 2)) columns")
    println(result)

    # --- Build output dict ---
    imports_df = filter(r -> r.indicator == "Imports", result)
    exports_df = filter(r -> r.indicator == "Exports", result)

    output = Dict(
        "data" => [Dict(String(k) => v for (k, v) in pairs(r)) for r in eachrow(result)],
        "imports" => Dict(r.sam => Dict("trade_value" => r.trade_value, "tariff" => r.tariff) for r in eachrow(imports_df)),
        "exports" => Dict(r.sam => Dict("trade_value" => r.trade_value, "tariff" => r.tariff) for r in eachrow(exports_df)),
        "intermediate_hs6" => sort(collect(intermediate_hs6)),
    )

    # --- Write JSON (optional) ---
    if write_json
        mkpath(DEFAULT_OUTPUT_DIR)
        slug = replace(lowercase(join(countries, "_")), r"[^a-z0-9_]+" => "_")
        output_path = joinpath(DEFAULT_OUTPUT_DIR, "ksa_sam_trade_tariffs_$(slug)_$(year).json")
        open(output_path, "w") do io
            JSON3.pretty(io, output)
        end
        println("Output written to $output_path")
    end

    return output
end

end # module TariffPipeline_hs6_to_sam

