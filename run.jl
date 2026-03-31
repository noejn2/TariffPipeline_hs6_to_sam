#=
run.jl — CLI entry point for the HS6 → SAM pipeline.

Usage (from project root):
    julia --project run.jl <countries> <year> [tariff_data] [cpc_sam_map]
    julia --project run.jl CHINA 2023
    julia --project run.jl CHINA 2023 data/ksa_final_bound_tariffs.parquet
    julia --project run.jl "CHINA,JAPAN,INDIA" 2023
    julia --project run.jl WORLD 2023
    julia --project run.jl CHINA 2023 data/ksa_final_bound_tariffs.parquet assets/mappings/cpc_to_sam/SAMv3-A81-C83-L6-cmap.json

If arguments are not provided, the user is prompted interactively.
Tariff data defaults to KSA bound tariffs if not provided.
=#

using TariffPipeline_hs6_to_sam

countries_raw = if length(ARGS) >= 1
    ARGS[1]
else
    print("Enter country/countries (comma-separated, e.g. CHINA or CHINA,JAPAN): ")
    strip(readline())
end

year_raw = if length(ARGS) >= 2
    ARGS[2]
else
    print("Enter year (e.g. 2023): ")
    strip(readline())
end
year = try
    parse(Int, year_raw)
catch
    error("Year must be a single integer, got: '$year_raw'")
end

kwargs = (;
    (length(ARGS) >= 3 ? (tariff_path=ARGS[3],) : (;))...,
    (length(ARGS) >= 4 ? (cpc_sam_path=ARGS[4],) : (;))...,
)

hs6_to_sam_pipeline(countries_raw, year; output_dir="output", kwargs...)

