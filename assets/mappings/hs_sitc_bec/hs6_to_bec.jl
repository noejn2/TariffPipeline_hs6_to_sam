using XLSX, DataFrames, Parquet2, JSON3

hs6_to_bec_path = "assets/mappings/hs_sitc_bec/"

function clean_hs6_bec_concordance()
    dt = XLSX.readtable(hs6_to_bec_path * "HS-SITC-BEC Correlations_2022.xlsx", "HS SITC BEC")
    df = DataFrame(dt)

    # Keep all columns as strings to avoid numeric formatting issues.
    for col in names(df)
        df[!, col] = string.(coalesce.(df[!, col], ""))
    end

    invalid = ["", "999999", "NULL", "missing"]
    result = select(df |> df -> filter(row -> !(row.HS17 in invalid), df), [:HS17, :BEC4])
    return filter(row -> !(row.HS17 in invalid) && !(row.BEC4 in invalid), result)
end

df = clean_hs6_bec_concordance()

# Build reverse map: BEC4 code => SNA category (first match wins for duplicates)
sna = JSON3.read(read(hs6_to_bec_path * "sna_use.json", String))
bec_to_sna = Dict{String,String}()
for (category, codes) in pairs(sna)
    for code in codes
        if !haskey(bec_to_sna, String(code))
            bec_to_sna[String(code)] = String(category)
        end
    end
end

# Map each HS6 code to its SNA category via BEC4
sna_to_hs6 = Dict{String,Vector{String}}(k => [] for k in keys(bec_to_sna) |> collect .|> identity |> unique)
sna_categories = unique(values(bec_to_sna))
sna_to_hs6 = Dict{String,Vector{String}}(cat => [] for cat in sna_categories)

for row in eachrow(df)
    category = get(bec_to_sna, row.BEC4, nothing)
    if !isnothing(category)
        push!(sna_to_hs6[category], row.HS17)
    end
end

# Deduplicate HS6 codes within each category
for cat in keys(sna_to_hs6)
    sna_to_hs6[cat] = unique(sna_to_hs6[cat])
end

# Write output JSON
output_path = hs6_to_bec_path * "hs6_to_sna.json"
open(output_path, "w") do io
    JSON3.pretty(io, sna_to_hs6)
end
println("Written to $output_path")
println("Category counts: ", Dict(k => length(v) for (k, v) in sna_to_hs6))

