"""
    filter_trade(df; years=nothing, partners=nothing) -> DataFrame

Filter the trade DataFrame by `years` and/or `partners`. Returns a copy;
the original is not modified. Passing `nothing` skips that filter.
"""
function filter_trade(df::DataFrame; years=nothing, partners=nothing)::DataFrame
    out = copy(df)
    if !isnothing(years)
        out = filter(r -> r.year in years, out)
    end
    if !isnothing(partners)
        out = filter(r -> r.partner_name in partners, out)
    end
    out
end

"""
    nullify_intermediate_tariffs(df, intermediate_hs6) -> DataFrame

For any row whose product_code is in the set of intermediate HS6 codes,
set the tariff to 0.0. Called after the trade+tariff merge, before
HS6 aggregation, so intermediate goods do not distort trade-weighted tariffs.
"""
function nullify_intermediate_tariffs(df::DataFrame, intermediate_hs6::Set{String})::DataFrame
    out = copy(df)
    out.tariff = ifelse.(in.(out.product_code, Ref(intermediate_hs6)), 0.0, coalesce.(out.tariff, 0.0))
    out
end

"""
    merge_trade_tariffs(trade, tariffs) -> DataFrame

Join trade and tariff data on (indicator, partner_name, product_code).
If tariff data contains partner_name="WORLD", those rows are expanded
to match every partner found in the trade data.
"""
function merge_trade_tariffs(trade::DataFrame, tariffs::DataFrame)::DataFrame
    t = rename(tariffs, :value => :tariff)

    # Expand WORLD rows to match each trade partner
    if "WORLD" in t.partner_name
        trade_partners = unique(trade.partner_name)
        world_rows = filter(r -> r.partner_name == "WORLD", t)
        expanded = [
            let chunk = copy(world_rows)
                chunk.partner_name .= p
                chunk
            end
            for p in trade_partners
        ]
        non_world = filter(r -> r.partner_name != "WORLD", t)
        t = vcat(expanded..., non_world)
    end

    leftjoin(trade, select(t, :indicator, :partner_name, :product_code, :tariff);
        on=[:indicator, :partner_name, :product_code])
end

"""
    weighted_avg(tariffs, weights) -> Float64

Trade-weighted average tariff. Uses import trade values as weights.
"""
function weighted_avg(tariffs, weights)
    t = coalesce.(tariffs, 0.0)
    w = coalesce.(weights, 0.0)
    valid = w .> 0
    wv = w[valid]
    tv = t[valid]
    sum(wv) == 0 ? 0.0 : sum(tv .* wv) / sum(wv)
end

"""
Collapse to unique (indicator, year, partner_name, product_code) rows.
  - trade_value = sum(value)
  - tariff = first non-missing tariff in the group (constant per product/indicator)
"""
function aggregate_by_hs6(df::DataFrame)::DataFrame
    gdf = groupby(df, [:indicator, :year, :partner_name, :product_code])
    combine(gdf,
        :value => sum => :trade_value,
        :tariff => (x -> let s = collect(skipmissing(x))
            isempty(s) ? 0.0 : first(s)
        end) => :tariff
    )
end

"""
    map_hs6_to_cpc(df, concordance) -> DataFrame

Join HS6-level data to CPC codes. When one HS6 maps to N CPCs (partial codes),
the trade value is split equally across the N rows (trade_value / N).
Tariff is kept as-is (same rate applies to each split).
"""
function map_hs6_to_cpc(df::DataFrame, concordance::DataFrame)::DataFrame
    n_map = combine(groupby(concordance, :hs6), nrow => :n_cpcs)
    conc = leftjoin(concordance, n_map; on=:hs6)

    out = leftjoin(df, conc; on=:product_code => :hs6)
    out.trade_value = out.trade_value ./ coalesce.(out.n_cpcs, 1)
    select!(out, Not(:n_cpcs))
    out
end

"""
Aggregate trade and tariff to CPC level.
  - trade_value = sum per (indicator, year, partner_name, cpc)
  - tariff = weighted average using the corresponding indicator's trade as weights
"""
function aggregate_by_cpc(df::DataFrame)::DataFrame
    df_valid = dropmissing(df, :cpc)
    combine(
        groupby(df_valid, [:indicator, :year, :partner_name, :cpc]),
        :trade_value => sum => :trade_value,
        [:tariff, :trade_value] => ((t, w) -> weighted_avg(t, w)) => :tariff
    )
end

"""
    map_cpc_to_sam(df, cpc_sam) -> DataFrame

Match each 5-digit CPC code to a SAM sector via the longest matching prefix.
"""
function map_cpc_to_sam(df::DataFrame, cpc_sam::DataFrame)::DataFrame
    prefix_to_sam = Dict(r.cpc_prefix => r.sam for r in eachrow(cpc_sam))
    prefixes_sorted = sort(collect(keys(prefix_to_sam)); by=length, rev=true)

    function find_sam(cpc::AbstractString)
        for p in prefixes_sorted
            if startswith(cpc, p)
                return prefix_to_sam[p]
            end
        end
        return missing
    end

    out = copy(df)
    out.sam = [ismissing(c) ? missing : find_sam(c) for c in out.cpc]
    out
end

"""
Aggregate trade and tariff to SAM level.
  - trade_value = sum per (indicator, year, partner_name, sam)
  - tariff = weighted average using the corresponding indicator's trade as weights
"""
function aggregate_by_sam(df::DataFrame)::DataFrame
    df_valid = dropmissing(df, :sam)
    combine(
        groupby(df_valid, [:indicator, :year, :partner_name, :sam]),
        :trade_value => sum => :trade_value,
        [:tariff, :trade_value] => ((t, w) -> weighted_avg(t, w)) => :tariff
    )
end

"""
    aggregate_countries(df) -> DataFrame

Aggregate multiple countries into a single "AGG_COUNTRY" row per (indicator, year, sam).
  - trade_value = sum across countries
  - tariff = weighted average using imports trade as weights
"""
function aggregate_countries(df::DataFrame)::DataFrame
    result = combine(
        groupby(df, [:indicator, :year, :sam]),
        :trade_value => sum => :trade_value,
        [:tariff, :trade_value] => ((t, w) -> weighted_avg(t, w)) => :tariff
    )
    result.partner_name .= "AGG_COUNTRY"
    result
end

# ─── Helper: build uniform 100% tariff DataFrame ─────────────────────────────

"""
    build_uniform_tariffs(trade_df, year) -> DataFrame

Build a default tariff DataFrame with a uniform 100% rate for every unique
(indicator, product_code) combination observed in the trade data for `year`.
Partner is set to `"WORLD"` so it matches all partners via expansion.
"""
function build_uniform_tariffs(trade_df::DataFrame, year::Int)::DataFrame
    subset = filter(r -> r.year == year, trade_df)
    keys = unique(select(subset, :indicator, :product_code))
    DataFrame(
        indicator=keys.indicator,
        partner_name=fill("WORLD", nrow(keys)),
        product_code=keys.product_code,
        value=fill(100.0, nrow(keys)),
    )
end
