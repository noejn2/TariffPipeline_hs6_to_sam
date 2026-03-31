using TariffPipeline_hs6_to_sam, DataFrames, JSON3, CairoMakie, Printf

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Run pipeline WITH intermediate tariff nullification (default)
# ─────────────────────────────────────────────────────────────────────────────
println("=== Run 1: WITH intermediate nullification ===")
out_with = hs6_to_sam_pipeline("WORLD", 2023)
result_with = DataFrame(out_with["data"])
println("  Rows: $(nrow(result_with))")

# ─────────────────────────────────────────────────────────────────────────────
# 2.  Run pipeline WITHOUT intermediate tariff nullification
# ─────────────────────────────────────────────────────────────────────────────
println("=== Run 2: WITHOUT intermediate nullification ===")
out_without = hs6_to_sam_pipeline("WORLD", 2023; sna_path=nothing)
result_without = DataFrame(out_without["data"])
println("  Rows: $(nrow(result_without))")

# ─────────────────────────────────────────────────────────────────────────────
# 3.  Build simulated IMPORT tariffs: one row per unique import HS6, value = 100.0
#     All raw tariffs are set to a uniform 100% so we can isolate the effect of
#     intermediate-input nullification on each SAM sector's trade-weighted tariff.
#     Sectors with more intermediate HS6 codes will see a larger drop.
# ─────────────────────────────────────────────────────────────────────────────
trade_raw = load_trade("data/saudi_reporter.parquet")
import_hs6 = unique(filter(r -> r.indicator == "Imports" && r.year == 2023, trade_raw).product_code)

tariff_ones = DataFrame(
    indicator=fill("Imports", length(import_hs6)),
    partner_name=fill("WORLD", length(import_hs6)),
    product_code=import_hs6,
    value=fill(100.0, length(import_hs6)),
)

println("=== Run 3: WITH intermediate nullification + uniform-100% import tariffs ===")
out_uniform_with = hs6_to_sam_pipeline("WORLD", 2023; tariff_path=tariff_ones)
result_uniform_with = DataFrame(out_uniform_with["data"])

println("=== Run 4: WITHOUT intermediate nullification + uniform-100% import tariffs ===")
out_uniform_without = hs6_to_sam_pipeline("WORLD", 2023; tariff_path=tariff_ones, sna_path=nothing)
result_uniform_without = DataFrame(out_uniform_without["data"])

# ─────────────────────────────────────────────────────────────────────────────
# 4.  Build data for scatterplots
# ─────────────────────────────────────────────────────────────────────────────

# --- Plot 1: Import tariffs (default KSA bound tariffs) ---
imp_with = filter(r -> r.indicator == "Imports", result_with)
imp_without = filter(r -> r.indicator == "Imports", result_without)

# Align on sam
imp = innerjoin(
    select(imp_with, :sam, :tariff => :tariff_with),
    select(imp_without, :sam, :tariff => :tariff_without);
    on=:sam,
)
sort!(imp, :tariff_without)

# --- Plot 2: Import tariffs (simulated uniform 100%) ---
uni_with = filter(r -> r.indicator == "Imports", result_uniform_with)
uni_without = filter(r -> r.indicator == "Imports", result_uniform_without)

uni = innerjoin(
    select(uni_with, :sam, :tariff => :tariff_with),
    select(uni_without, :sam, :tariff => :tariff_without);
    on=:sam,
)
sort!(uni, :tariff_without)

# ─────────────────────────────────────────────────────────────────────────────
# 5.  Print summary statistics
# ─────────────────────────────────────────────────────────────────────────────
println("\n=== Import tariff comparison (KSA bound rates, WORLD 2023) ===")
println("Sector              | Without filter | With filter | Diff")
println("─"^60)
for r in eachrow(imp)
    diff = r.tariff_with - r.tariff_without
    @printf("%-20s | %14.4f | %11.4f | %+.4f\n",
        r.sam, r.tariff_without, r.tariff_with, diff)
end

n_lower = count(r -> r.tariff_with < r.tariff_without, eachrow(imp))
n_same = count(r -> r.tariff_with ≈ r.tariff_without, eachrow(imp))
println("\n  Sectors where tariff is lower with filter: $n_lower / $(nrow(imp))")
println("  Sectors unchanged: $n_same / $(nrow(imp))")

# ─────────────────────────────────────────────────────────────────────────────
# 6.  Scatterplots
# ─────────────────────────────────────────────────────────────────────────────
mkpath("assets")

# Helper to determine font size from sector label length
function abbrev(s, n=10)
    length(s) <= n ? s : s[1:n] * "…"
end

# ── Plot 1: Import tariffs ──────────────────────────────────────────────────
let
    fig = Figure(size=(800, 700))
    ax = Axis(fig[1, 1];
        xlabel="Tariff rate WITHOUT intermediate filter (%)",
        ylabel="Tariff rate WITH intermediate filter (%)",
        title="Import tariffs by SAM sector\n(KSA bound rates, WORLD 2023)",
    )

    x = imp.tariff_without
    y = imp.tariff_with

    # 45-degree reference line
    lim = max(maximum(x), maximum(y)) * 1.05
    lines!(ax, [0, lim], [0, lim]; color=:gray70, linestyle=:dash, linewidth=1.5, label="y = x")

    # Points coloured by whether filter reduced the tariff
    colors = [yi < xi ? :firebrick : :steelblue for (xi, yi) in zip(x, y)]
    scatter!(ax, x, y; color=colors, markersize=9)

    # Labels for points that differ visibly
    for (xi, yi, lab) in zip(x, y, imp.sam)
        abs(yi - xi) > 0.3 && text!(ax, xi, yi; text=abbrev(lab, 12),
            fontsize=9, offset=(4, 4))
    end

    # Legend proxy
    scatter!(ax, Float64[], Float64[]; color=:firebrick, label="Lower with filter")
    scatter!(ax, Float64[], Float64[]; color=:steelblue, label="Unchanged")
    axislegend(ax; position=:lt)

    try
        save("assets/tariff_comparison_imports.png", fig; px_per_unit=2)
    catch e
        e isa SystemError || rethrow()
        @warn "Non-fatal close error writing imports plot" exception = e
    end
    println("Saved: assets/tariff_comparison_imports.png")
end

# ── Plot 2: Import tariffs (simulated uniform 100%) ─────────────────────────
let
    fig = Figure(size=(800, 700))
    ax = Axis(fig[1, 1];
        xlabel="Tariff rate WITHOUT intermediate filter (%)",
        ylabel="Tariff rate WITH intermediate filter (%)",
        title="Import tariffs by SAM sector\n(uniform 100% raw tariffs, WORLD 2023)",
    )

    x = uni.tariff_without
    y = uni.tariff_with

    lim = max(maximum(x), maximum(y)) * 1.05
    lines!(ax, [0, lim], [0, lim]; color=:gray70, linestyle=:dash, linewidth=1.5, label="y = x")

    colors = [yi < xi ? :darkorange : :steelblue for (xi, yi) in zip(x, y)]
    scatter!(ax, x, y; color=colors, markersize=9)

    for (xi, yi, lab) in zip(x, y, uni.sam)
        abs(yi - xi) > 0.02 && text!(ax, xi, yi; text=abbrev(lab, 12),
            fontsize=9, offset=(4, 4))
    end

    scatter!(ax, Float64[], Float64[]; color=:darkorange, label="Lower with filter")
    scatter!(ax, Float64[], Float64[]; color=:steelblue, label="Unchanged")
    axislegend(ax; position=:lt)

    try
        save("assets/tariff_comparison_imports_uniform.png", fig; px_per_unit=2)
    catch e
        e isa SystemError || rethrow()
        @warn "Non-fatal close error writing uniform imports plot" exception = e
    end
    println("Saved: assets/tariff_comparison_imports_uniform.png")
end

println("\n=== All done ===")
