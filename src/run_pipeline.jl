# ─── Full pipeline ────────────────────────────────────────────────────────────

"""
    run_pipeline(;
        trade_path, tariff_path, hs6_cpc_path, cpc_sam_path,
        sna_path=nothing, years=nothing, partners=nothing
    ) -> DataFrame

Run the full HS6 → CPC → SAM aggregation pipeline and return the final
SAM-level trade/tariff DataFrame.
"""
function run_pipeline(;
    trade_path::String,
    tariff_path::Union{String,DataFrame},
    hs6_cpc_path::String,
    cpc_sam_path::Union{Dict,String},
    sna_path::Union{String,Nothing}=nothing,
    years=nothing,
    partners=nothing
)
    # 1. Load inputs
    trade = load_trade(trade_path)
    tariffs = tariff_path isa DataFrame ? tariff_path : load_tariffs(tariff_path)
    hs6_cpc = load_hs6_cpc(hs6_cpc_path)
    cpc_sam = cpc_sam_path isa Dict ? load_cpc_sam(cpc_sam_path) : load_cpc_sam(cpc_sam_path)
    intermediate_hs6 = isnothing(sna_path) ? Set{String}() : load_hs6_sna(sna_path)

    # 2. Filter trade
    trade = filter_trade(trade; years, partners)

    # 3. Merge trade + tariffs on HS6
    merged = merge_trade_tariffs(trade, tariffs)

    # 3.5. Nullify tariffs for intermediate goods (Imports only — KSA policy)
    imports_mask = merged.indicator .== "Imports"
    merged[imports_mask, :tariff] = nullify_intermediate_tariffs(merged[imports_mask, :], intermediate_hs6).tariff

    # 4. Aggregate by HS6
    hs6_agg = aggregate_by_hs6(merged)

    # 5. Map HS6 → CPC
    cpc_mapped = map_hs6_to_cpc(hs6_agg, hs6_cpc)

    # 6. Aggregate by CPC
    cpc_agg = aggregate_by_cpc(cpc_mapped)

    # 7. Map CPC → SAM
    sam_mapped = map_cpc_to_sam(cpc_agg, cpc_sam)

    # 8. Aggregate by SAM
    sam_agg = aggregate_by_sam(sam_mapped)

    return sam_agg, intermediate_hs6
end
