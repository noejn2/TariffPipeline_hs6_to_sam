using CSV, DataFrames, Parquet2

path_assets = "assets/mappings/hs6_to_cpc/"
df = CSV.read(path_assets * "CPC21-HS2017.csv", DataFrame; types=String)

# Remove "." from HS codes and lpad to 6 characters
df[!, "HS 2017"] = [lpad(replace(x, "." => ""), 6, '0') for x in df[!, "HS 2017"]]
df[!, "CPC Ver. 2.1"] = [lpad(x, 5, '0') for x in df[!, "CPC Ver. 2.1"]]

# Rename and select
rename!(df, "HS 2017" => :hs6, "CPC Ver. 2.1" => :cpc)
select!(df, :hs6, :cpc)

Parquet2.writefile(path_assets * "hs6_cpc_concordance.parquet", df)
