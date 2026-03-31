#=
query.jl — Run SQL files against Athena and get results as CSV.

Usage (from project root):
    julia src/query.jl queries/saudi_reporter_filter.sql
    julia src/query.jl queries/tariff_lookup.sql --output results.csv
    julia src/query.jl queries/trade_with_tariffs.sql --format json --output results.json
=#

using AWS, JSON3, CSV, DataFrames

@service Athena

const REGION = "ap-south-1"
const BUCKET = "lbpo-blob-storage"
const OUTPUT_LOCATION = "s3://$(BUCKET)/athena-results/"

aws_cfg = AWSConfig(region=REGION)

function run_query(sql::AbstractString)
    resp = Athena.start_query_execution(
        sql,
        Dict("ResultConfiguration" => Dict("OutputLocation" => OUTPUT_LOCATION));
        aws_config=aws_cfg
    )
    query_id = resp["QueryExecutionId"]
    println("Query started: $query_id")

    while true
        status = Athena.get_query_execution(query_id; aws_config=aws_cfg)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED")
            if state != "SUCCEEDED"
                reason = get(status["QueryExecution"]["Status"], "StateChangeReason", "")
                error("Query $state: $reason")
            end
            break
        end
        sleep(1)
    end

    # Collect results
    headers = String[]
    rows = Vector{Dict{String,String}}()
    next_token = nothing
    first_page = true

    while true
        result = if isnothing(next_token)
            Athena.get_query_results(query_id; aws_config=aws_cfg)
        else
            Athena.get_query_results(query_id, Dict("NextToken" => next_token); aws_config=aws_cfg)
        end
        result_rows = result["ResultSet"]["Rows"]

        for (i, row) in enumerate(isa(result_rows, Vector) ? result_rows : [result_rows])
            data = row["Data"]
            values = [get(col, "VarCharValue", "") for col in (isa(data, Vector) ? data : [data])]
            if first_page && i == 1
                headers = values
            else
                push!(rows, Dict(zip(headers, values)))
            end
        end
        first_page = false

        next_token = get(result, "NextToken", nothing)
        if isnothing(next_token)
            break
        end
    end

    return headers, rows
end

function main()
    if isempty(ARGS)
        println("Usage: julia src/query.jl <sql_file> [--output file] [--format csv|json]")
        exit(1)
    end

    sql_file = ARGS[1]
    output = nothing
    fmt = "csv"

    i = 2
    while i <= length(ARGS)
        if ARGS[i] in ("--output", "-o")
            output = ARGS[i+1]
            i += 2
        elseif ARGS[i] in ("--format", "-f")
            fmt = ARGS[i+1]
            i += 2
        else
            i += 1
        end
    end

    sql = strip(read(sql_file, String))
    display_sql = join(filter(l -> !startswith(strip(l), "--"), split(sql, "\n")), "\n")
    println("Running:\n  $(strip(display_sql))\n")

    headers, rows = run_query(sql)
    println("Returned $(length(rows)) rows.\n")

    if fmt == "json"
        content = JSON3.write(rows)
        if isnothing(output)
            println(content)
        else
            write(output, content)
            println("Saved to $output")
        end
    else
        df = DataFrame(rows)
        if isnothing(output)
            CSV.write(stdout, df)
        else
            CSV.write(output, df)
            println("Saved to $output")
        end
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
