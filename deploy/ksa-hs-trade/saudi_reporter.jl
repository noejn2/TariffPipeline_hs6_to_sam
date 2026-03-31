using CSV, DataFrames, Parquet2

# Load and process the KSA Trade data
saudi_reporter_csv = CSV.read("deploy/ksa-hs-trade/Trade data (2019-24).csv", DataFrame; silencewarnings=true) |>
                     df -> select(df, ["TRD_DESC_EN", "TRD_YR", "COUNTRY_DESC_EN", "COMMODTIY_CODE", "VALUE(S.R)"]) |>
                           df -> rename(df,
      "TRD_DESC_EN" => :indicator,
      "TRD_YR" => :year,
      "COUNTRY_DESC_EN" => :partner_name,
      "COMMODTIY_CODE" => :product_code,
      "VALUE(S.R)" => :value_raw) |>
                                 df -> dropmissing(df, [:product_code, :value_raw, :partner_name]) |>
                                       df -> filter(r -> r.indicator != "Rexports", df) |>
                                             df -> transform(df,
      :product_code => ByRow(x -> lpad(string(x), 8, '0')[1:6]) => :product_code,
      :value_raw => ByRow(x -> parse(Float64, replace(string(x), "," => ""))) => :value) |>
                                                   df -> select(df, [:indicator, :year, :partner_name, :product_code, :value]) |>
                                                         df -> combine(groupby(df, [:indicator, :year, :partner_name, :product_code]), :value => sum => :value)

# Save to Parquet
Parquet2.writefile("deploy/ksa-hs-trade/saudi_reporter.parquet", saudi_reporter_csv)