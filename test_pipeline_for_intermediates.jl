using TariffPipeline_hs6_to_sam, DataFrames, JSON3, CairoMakie, Printf

# ─────────────────────────────────────────────────────────────────────────────
# Test: Intermediate tariff nullification
#
# The pipeline always applies BEC/SNA intermediate filtering: HS6 codes
# classified as intermediate inputs get their tariff zeroed before
# trade-weighted aggregation. This test uses the default uniform 100%
# tariff to isolate how much each SAM sector is reduced by the filter.
#
# If all HS6 codes in a sector are intermediate → effective tariff = 0%.
# If none are intermediate → effective tariff stays at 100%.
# Mixed sectors land in between, proportional to intermediate trade share.
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Run pipeline with default uniform 100% tariff
#     (intermediate filter always on — reveals each sector's intermediate share)
# ─────────────────────────────────────────────────────────────────────────────
println("=== Run 1: Default uniform 100% tariff (intermediate filter always on) ===")
out_default = hs6_to_sam_pipeline("WORLD", 2023)
result_default = DataFrame(out_default["data"])
println("  Rows: $(nrow(result_default))")

# ─────────────────────────────────────────────────────────────────────────────
# 2.  Run pipeline with KSA bound tariffs
# ─────────────────────────────────────────────────────────────────────────────
println("\n=== Run 2: KSA bound tariffs (intermediate filter always on) ===")
out_ksa = hs6_to_sam_pipeline("WORLD", 2023;
    tariff_data="data/ksa_final_bound_tariffs.parquet")
result_ksa = DataFrame(out_ksa["data"])
println("  Rows: $(nrow(result_ksa))")

# ─────────────────────────────────────────────────────────────────────────────
# 3.  Build comparison table (imports only)
# ─────────────────────────────────────────────────────────────────────────────
imp_default = select(filter(r -> r.indicator == "Imports", result_default), :sam, :tariff => :tariff_uniform)
imp_ksa = select(filter(r -> r.indicator == "Imports", result_ksa), :sam, :tariff => :tariff_ksa)

cmp = outerjoin(imp_default, imp_ksa; on=:sam)
for col in [:tariff_uniform, :tariff_ksa]
    cmp[!, col] = coalesce.(cmp[!, col], 0.0)
end
cmp.intermediate_share = 100.0 .- cmp.tariff_uniform  # drop from 100% = intermediate %
sort!(cmp, :intermediate_share; rev=true)

# ─────────────────────────────────────────────────────────────────────────────
# 4.  Print summary
# ─────────────────────────────────────────────────────────────────────────────
println("\n=== Import tariff comparison (WORLD 2023) ===")
println("SAM                  | Uniform 100% → | KSA bound → | Intermediate share")
println("─"^75)
for r in eachrow(cmp)
    @printf("%-20s | %13.2f%% | %10.2f%% | %17.2f%%\n",
        r.sam, r.tariff_uniform, r.tariff_ksa, r.intermediate_share)
end

n_fully_intermediate = count(r -> r.tariff_uniform ≈ 0.0, eachrow(cmp))
n_no_intermediate = count(r -> r.tariff_uniform ≈ 100.0, eachrow(cmp))
println("\n  Sectors 100% intermediate (tariff → 0%): $n_fully_intermediate / $(nrow(cmp))")
println("  Sectors 0% intermediate (tariff stays 100%): $n_no_intermediate / $(nrow(cmp))")

# ─────────────────────────────────────────────────────────────────────────────
# 5.  Plots
# ─────────────────────────────────────────────────────────────────────────────
mkpath("assets")

function abbrev(s, n=10)
    length(s) <= n ? s : s[1:n] * "…"
end

# ── Plot 1: Intermediate share by sector (bar chart) ────────────────────────
let
    fig = Figure(size=(900, 600))
    ax = Axis(fig[1, 1];
        xlabel="SAM sector",
        ylabel="Effective tariff under uniform 100% (%)",
        title="Effect of intermediate nullification by SAM sector\n(WORLD imports 2023, uniform 100% baseline)",
        xticklabelrotation=π / 4,
        xticks=(1:nrow(cmp), abbrev.(cmp.sam, 12)),
    )

    xs = 1:nrow(cmp)
    barplot!(ax, xs, cmp.tariff_uniform; color=:steelblue, label="Effective tariff")

    # Reference line at 100%
    hlines!(ax, [100.0]; color=:gray70, linestyle=:dash, linewidth=1.5, label="No intermediates (100%)")

    axislegend(ax; position=:rt)

    try
        save("assets/tariff_intermediate_effect.png", fig; px_per_unit=2)
    catch e
        e isa SystemError || rethrow()
        @warn "Non-fatal close error" exception = e
    end
    println("\nSaved: assets/tariff_intermediate_effect.png")
end

# ── Plot 2: Uniform vs KSA bound tariffs (scatter) ─────────────────────────
let
    fig = Figure(size=(800, 700))
    ax = Axis(fig[1, 1];
        xlabel="Effective tariff — uniform 100% baseline (%)",
        ylabel="Effective tariff — KSA bound rates (%)",
        title="SAM sector tariffs: uniform 100% vs KSA bound\n(WORLD imports 2023, intermediate filter on)",
    )

    x = cmp.tariff_uniform
    y = cmp.tariff_ksa

    scatter!(ax, x, y; color=:firebrick, markersize=9)

    for (xi, yi, lab) in zip(x, y, cmp.sam)
        text!(ax, xi, yi; text=abbrev(lab, 12), fontsize=9, offset=(4, 4))
    end

    try
        save("assets/tariff_uniform_vs_ksa.png", fig; px_per_unit=2)
    catch e
        e isa SystemError || rethrow()
        @warn "Non-fatal close error" exception = e
    end
    println("Saved: assets/tariff_uniform_vs_ksa.png")
end

println("\n=== All done ===")
