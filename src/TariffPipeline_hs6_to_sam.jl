module TariffPipeline_hs6_to_sam

using DataFrames, Parquet2, JSON3, Statistics

export hs6_to_sam_pipeline, load_trade

include("loaders.jl")
include("transforms.jl")
include("validations.jl")
include("run_pipeline.jl")

# ─── Default paths ────────────────────────────────────────────────────────────

const TRADE_PATH = joinpath(@__DIR__, "..", "data", "saudi_reporter.parquet")
const HS6_CPC_PATH = joinpath(@__DIR__, "..", "assets", "mappings", "hs6_to_cpc", "hs6_cpc_concordance.parquet")
const DEFAULT_TARIFF_PATH = joinpath(@__DIR__, "..", "data", "ksa_final_bound_tariffs.parquet")
const DEFAULT_CPC_SAM_PATH = joinpath(@__DIR__, "..", "assets", "mappings", "cpc_to_sam", "SAMv3-A21-C22-L6-cmap.json")
const DEFAULT_SNA_PATH = joinpath(@__DIR__, "..", "assets", "mappings", "hs_sitc_bec", "hs6_to_sna.json")

# ─── High-level entry point ───────────────────────────────────────────────────

"""
    hs6_to_sam_pipeline(countries_raw, year; tariff_path, cpc_sam_path, sna_path, output_dir) -> Dict

High-level wrapper around `run_pipeline`. Accepts the same arguments as the CLI,
validates all inputs, runs the full pipeline, writes a JSON file to `output_dir`,
and returns the output Dict with keys `"imports"`, `"exports"`, and `"intermediate_hs6"`.

# Arguments
- `countries_raw`: countries as a comma-separated string (`"CHINA,JAPAN"`), a String (`"CHINA"`), a Vector (`["CHINA", "JAPAN"]`), or `"WORLD"`
- `year`: trade year, e.g. `2023`
- `tariff_path`: path to the tariff parquet file, or a pre-loaded DataFrame (default: KSA bound tariffs)
- `cpc_sam_path`: path to the CPC→SAM JSON mapping (default: SAMv3-A21-C22-L6)
- `sna_path`: path to the HS6→SNA JSON for intermediate tariff nullification. Pass `nothing` to disable. (default: bundled hs6_to_sna.json)
- `output_dir`: directory to write output JSON file. Pass `nothing` (default) to skip writing.
"""
function hs6_to_sam_pipeline(
    countries_raw::Union{String,Vector{String}},
    year::Int,
    cpc_sam_path::Union{Dict,String,Nothing}=DEFAULT_CPC_SAM_PATH;
    tariff_path::Union{DataFrame,String,Nothing}=DEFAULT_TARIFF_PATH,
    sna_path::Union{String,Nothing}=isfile(DEFAULT_SNA_PATH) ? DEFAULT_SNA_PATH : nothing,
    output_dir::Union{String,Nothing}=nothing,
)::Dict
    tariff_path = isnothing(tariff_path) ? DEFAULT_TARIFF_PATH : tariff_path
    cpc_sam_path = isnothing(cpc_sam_path) ? DEFAULT_CPC_SAM_PATH : cpc_sam_path

    countries = countries_raw isa Vector ? strip.(countries_raw) : [strip(c) for c in split(countries_raw, ",")]
    is_world = length(countries) == 1 && countries[1] == "WORLD"

    # --- Validate trade inputs ---
    trade_df = load_trade(TRADE_PATH)
    validate_trade_inputs(trade_df, countries, year, is_world)

    # --- Validate tariff data ---
    tariff_df = tariff_path isa DataFrame ? tariff_path : load_tariffs(tariff_path)
    validate_tariff_data(tariff_df, countries)

    # --- Validate CPC→SAM mapping ---
    validate_cpc_sam_path(cpc_sam_path)

    println("Running pipeline: countries=$(join(countries, ",")), year=$year, tariff=$(tariff_path isa DataFrame ? "<DataFrame>" : tariff_path), cpc_sam=$cpc_sam_path, sna=$(isnothing(sna_path) ? "disabled" : sna_path)")

    # --- Run pipeline ---
    result, intermediate_hs6 = run_pipeline(;
        trade_path=TRADE_PATH,
        tariff_path=tariff_path,
        hs6_cpc_path=HS6_CPC_PATH,
        cpc_sam_path=cpc_sam_path,
        sna_path=sna_path,
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
    if !isnothing(output_dir)
        mkpath(output_dir)
        slug = replace(lowercase(join(countries, "_")), r"[^a-z0-9_]+" => "_")
        output_path = joinpath(output_dir, "ksa_sam_trade_tariffs_$(slug)_$(year).json")
        open(output_path, "w") do io
            JSON3.pretty(io, output)
        end
        println("Output written to $output_path")
    end

    return output
end

end # module TariffPipeline_hs6_to_sam

