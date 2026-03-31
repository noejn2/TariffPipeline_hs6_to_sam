using CSV, DataFrames, Parquet2, Statistics

# Load and process the KSA bound tariffs data
ksa_final_bound_tariffs_csv = CSV.read("deploy/ksa-bound-tariffs/ksa_final_bound_tariffs.csv", DataFrame) |>
                              df -> filter(r -> r.product_code != 999999, df) |>
                                    df -> select(df, [:product_code, :value]) |>
                                          df -> transform(df,
      :product_code => ByRow(x -> lpad(string(x), 6, '0')) => :product_code,
      :value => ByRow(x -> coalesce(x, 0.0)) => :value) |>
                                                df -> combine(groupby(df, [:product_code]), :value => mean => :value) |>
                                                      df -> insertcols!(df, 1,
      :indicator => "Imports",
      :partner_name => "WORLD")

# Save to Parquet
Parquet2.writefile("data/ksa_final_bound_tariffs.parquet", ksa_final_bound_tariffs_csv)

