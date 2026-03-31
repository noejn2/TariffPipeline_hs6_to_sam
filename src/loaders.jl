"""
    load_trade(path) -> DataFrame

Read the trade parquet file at `path` and return it as a DataFrame.
"""
function load_trade(path::String)::DataFrame
    DataFrame(Parquet2.readfile(path))
end

"""
    load_tariffs(path) -> DataFrame

Read the tariff parquet file at `path` and return it as a DataFrame.
"""
function load_tariffs(path::String)::DataFrame
    DataFrame(Parquet2.readfile(path))
end

"""
    load_hs6_cpc(path) -> DataFrame

Read the HS6→CPC concordance parquet at `path` and return it as a DataFrame.
"""
function load_hs6_cpc(path::String)::DataFrame
    DataFrame(Parquet2.readfile(path))
end

"""
    load_cpc_sam(path) -> DataFrame

Read the SAM cmap JSON (SAM sector → [CPC prefixes]) and return a DataFrame
with columns `cpc_prefix` and `sam`. Accepts either a file path or a pre-loaded Dict.
"""
function _cpc_sam_dict_to_df(d::Dict)::DataFrame
    rows = [(cpc_prefix=String(p), sam=String(k)) for (k, v) in d for p in v]
    DataFrame(rows)
end

function load_cpc_sam(path::Union{Dict,String})::DataFrame
    path isa Dict && return _cpc_sam_dict_to_df(path)
    raw = JSON3.read(read(path, String))
    rows = [(cpc_prefix=String(p), sam=String(k)) for (k, v) in raw for p in v]
    DataFrame(rows)
end

"""
    load_hs6_sna(path) -> Set{String}

Read the hs6_to_sna JSON (SNA category => [HS6 codes]) and return the set of
HS6 codes classified as "Intermediate".
"""
function load_hs6_sna(path::String)::Set{String}
    raw = JSON3.read(read(path, String))
    codes = get(raw, :Intermediate, String[])
    Set(String.(codes))
end
