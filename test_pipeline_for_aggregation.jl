using TariffPipeline_hs6_to_sam, DataFrames, JSON3, Printf

# ─────────────────────────────────────────────────────────────────────────────
# Test: Country aggregation
#
# Verify that running the pipeline on individual countries and then
# manually aggregating produces the same result as passing multiple
# countries (or "WORLD") to the pipeline, which aggregates internally.
#
# We also build per-country tariff DataFrames so that each country has
# distinct tariff rates, ensuring the trade-weighted average is non-trivial.
# ─────────────────────────────────────────────────────────────────────────────

# ── 1. Pick two countries present in the trade data ─────────────────────────
trade_raw = load_trade("data/saudi_reporter.parquet")
all_partners = sort(unique(trade_raw.partner_name))
println("Available partners ($(length(all_partners))): $(join(first(all_partners, 10), ", "))…")

country_a = "CHINA"
country_b = "JAPAN"

# Confirm they exist
@assert check_partners([country_a, country_b]) "One or more countries not found in trade data"

# ── 2. Build per-country tariff DataFrames ──────────────────────────────────
#    country_a gets 10% on all imports, country_b gets 20%.
#    This makes the aggregated tariff a trade-weighted blend of 10% and 20%.

function make_tariff_df(trade_df, country, year, rate)
    hs6 = unique(filter(r -> r.indicator == "Imports" &&
                                 r.year == year &&
                                 r.partner_name == country, trade_df).product_code)
    DataFrame(
        indicator=fill("Imports", length(hs6)),
        partner_name=fill(country, length(hs6)),
        product_code=hs6,
        value=fill(rate, length(hs6)),
    )
end

yr = 2023
tariff_a = make_tariff_df(trade_raw, country_a, yr, 10.0)
tariff_b = make_tariff_df(trade_raw, country_b, yr, 20.0)
tariff_ab = vcat(tariff_a, tariff_b)

println("\nTariff rows: $(country_a)=$(nrow(tariff_a)), $(country_b)=$(nrow(tariff_b)), combined=$(nrow(tariff_ab))")

# ── 3. Run pipeline: individual countries ───────────────────────────────────
println("\n=== Run 1: $(country_a) alone (10% tariff) ===")
out_a = hs6_to_sam_pipeline(country_a, yr; tariff_data=tariff_a)
result_a = DataFrame(out_a["data"])
println("  Rows: $(nrow(result_a))")

println("\n=== Run 2: $(country_b) alone (20% tariff) ===")
out_b = hs6_to_sam_pipeline(country_b, yr; tariff_data=tariff_b)
result_b = DataFrame(out_b["data"])
println("  Rows: $(nrow(result_b))")

# ── 4. Run pipeline: both countries at once (internal aggregation) ──────────
println("\n=== Run 3: $(country_a),$(country_b) aggregated (pipeline handles it) ===")
out_ab = hs6_to_sam_pipeline("$(country_a),$(country_b)", yr; tariff_data=tariff_ab)
result_ab = DataFrame(out_ab["data"])
println("  Rows: $(nrow(result_ab))")

# ── 5. Manual aggregation of individual results ────────────────────────────
#    Reproduce what aggregate_countries does: sum trade values, trade-weighted avg tariff.
manual = let
    stacked = vcat(
        select(filter(r -> r.indicator == "Imports", result_a), :sam, :trade_value, :tariff),
        select(filter(r -> r.indicator == "Imports", result_b), :sam, :trade_value, :tariff),
    )
    combine(groupby(stacked, :sam),
        :trade_value => sum => :trade_value_manual,
        [:tariff, :trade_value] => ((t, w) -> begin
            valid = w .> 0
            sum(w[valid]) == 0 ? 0.0 : sum(t[valid] .* w[valid]) / sum(w[valid])
        end) => :tariff_manual,
    )
end

# ── 6. Compare pipeline aggregation vs manual aggregation ───────────────────
pipeline_imp = filter(r -> r.indicator == "Imports", result_ab)
cmp = innerjoin(
    select(pipeline_imp, :sam, :trade_value => :tv_pipeline, :tariff => :tariff_pipeline),
    manual;
    on=:sam,
)

println("\n=== Comparison: pipeline aggregation vs manual aggregation ===")
println("SAM                  | TV pipeline    | TV manual      | Tariff pipeline | Tariff manual | Match?")
println("─"^100)
all_match = true
for r in sort(eachrow(cmp), by=r -> r.sam)
    tv_ok = isapprox(r.tv_pipeline, r.trade_value_manual; rtol=1e-6)
    t_ok = isapprox(r.tariff_pipeline, r.tariff_manual; rtol=1e-6)
    ok = tv_ok && t_ok ? "✓" : "✗"
    if !tv_ok || !t_ok
        global all_match = false
    end
    @printf("%-20s | %14.2f | %14.2f | %15.4f | %13.4f | %s\n",
        r.sam, r.tv_pipeline, r.trade_value_manual, r.tariff_pipeline, r.tariff_manual, ok)
end

println("\n  All sectors match: $(all_match ? "YES ✓" : "NO ✗")")

# ── 7. Show individual-country breakdown ────────────────────────────────────
println("\n=== Per-country detail (Imports) ===")
println("SAM                  | $(country_a) TV     | $(country_a) tariff | $(country_b) TV     | $(country_b) tariff | Agg tariff")
println("─"^105)
imp_a = select(filter(r -> r.indicator == "Imports", result_a), :sam, :trade_value => :tv_a, :tariff => :t_a)
imp_b = select(filter(r -> r.indicator == "Imports", result_b), :sam, :trade_value => :tv_b, :tariff => :t_b)

detail = outerjoin(imp_a, imp_b; on=:sam)
detail = leftjoin(detail, select(pipeline_imp, :sam, :tariff => :t_agg); on=:sam)
for col in [:tv_a, :t_a, :tv_b, :t_b, :t_agg]
    detail[!, col] = coalesce.(detail[!, col], 0.0)
end
sort!(detail, :sam)

for r in eachrow(detail)
    @printf("%-20s | %14.2f | %11.4f%% | %14.2f | %11.4f%% | %10.4f%%\n",
        r.sam, r.tv_a, r.t_a, r.tv_b, r.t_b, r.t_agg)
end

println("\n=== All done ===")
