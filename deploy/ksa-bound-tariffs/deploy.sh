#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Deploys KSA final bound tariffs as a serverless SQL database.
#
# Architecture:
#   S3 (Parquet) → Glue Data Catalog → Athena (serverless SQL)
#
# What it does:
#   1. Creates S3 bucket with encryption (shared with other datasets)
#   2. Runs Julia pipeline to produce Parquet
#   3. Uploads Parquet to S3
#   4. Creates Glue database + table (schema on the Parquet)
#   5. Creates Athena workgroup with results location
#
# After deploy, query the data in the Athena console:
#   SELECT * FROM ksa_bound_tariffs.ksa_final_bound_tariffs
#   WHERE product_code = '010121'
#
# Prerequisites:
#   - AWS CLI with s3, glue, athena permissions
#   - Julia 1.11+ with CSV, DataFrames, Parquet2, Statistics
#
# Usage:
#   ./deploy/ksa-bound-tariffs/deploy.sh
# =============================================================================
set -euo pipefail

BUCKET="lbpo-blob-storage"
DATASET="ksa-bound-tariffs"
REGION="ap-south-1"
GLUE_DB="ksa_bound_tariffs"
GLUE_TABLE="ksa_final_bound_tariffs"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ---- S3 Bucket ----
echo "=== S3 Bucket ==="
if aws s3api head-bucket --bucket "${BUCKET}" --region "${REGION}" 2>/dev/null; then
  echo "  Bucket already exists."
else
  aws s3 mb "s3://${BUCKET}" --region "${REGION}"
fi

aws s3api put-bucket-encryption \
  --bucket "${BUCKET}" \
  --server-side-encryption-configuration \
    '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}' \
  --region "${REGION}"

aws s3api put-public-access-block \
  --bucket "${BUCKET}" \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true \
  --region "${REGION}"

# ---- Julia Pipeline ----
echo "=== Julia Pipeline ==="
cd "${PROJECT_DIR}"
julia -e '
using CSV, DataFrames, Parquet2, Statistics

ksa_final_bound_tariffs = CSV.read("data/raw/ksa_final_bound_tariffs.csv", DataFrame) |>
    df -> filter(r -> r.product_code != 999999, df) |>
    df -> select(df, [:product_code, :value]) |>
    df -> transform(df, :product_code => ByRow(x -> lpad(string(x), 6, '\''0'\'')) => :product_code,
                        :value => ByRow(x -> coalesce(x, 0.0)) => :value) |>
    df -> combine(groupby(df, [:product_code]), :value => mean => :value)

Parquet2.writefile("data/ksa_final_bound_tariffs.parquet", ksa_final_bound_tariffs)
println("Parquet: ", filesize("data/ksa_final_bound_tariffs.parquet"), " bytes, ", nrow(ksa_final_bound_tariffs), " rows")
'

echo "  Uploading to S3..."
aws s3 cp data/ksa_final_bound_tariffs.parquet \
  "s3://${BUCKET}/${DATASET}/ksa_final_bound_tariffs.parquet" \
  --region "${REGION}"

# ---- Glue Data Catalog ----
echo "=== Glue Data Catalog ==="
aws glue create-database \
  --database-input '{"Name":"'"${GLUE_DB}"'","Description":"KSA final bound tariffs data"}' \
  --region "${REGION}" 2>/dev/null || echo "  Database already exists."

aws glue create-table \
  --database-name "${GLUE_DB}" \
  --table-input '{
    "Name": "'"${GLUE_TABLE}"'",
    "Description": "KSA final bound tariffs by HS6 product code (mean value)",
    "TableType": "EXTERNAL_TABLE",
    "Parameters": {"classification": "parquet", "has_encrypted_data": "false"},
    "StorageDescriptor": {
      "Location": "s3://'"${BUCKET}"'/'"${DATASET}"'/",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "Parameters": {"serialization.format": "1"}
      },
      "Columns": [
        {"Name": "product_code", "Type": "string", "Comment": "HS6 product code (6-char, zero-padded)"},
        {"Name": "value",        "Type": "double", "Comment": "Mean final bound tariff rate (%)"}
      ]
    }
  }' \
  --region "${REGION}" 2>/dev/null || echo "  Table already exists."

# ---- Athena Workgroup ----
echo "=== Athena Workgroup ==="
aws athena create-work-group \
  --name "${DATASET}" \
  --description "Serverless SQL for KSA final bound tariffs" \
  --configuration '{
    "ResultConfiguration": {"OutputLocation": "s3://'"${BUCKET}"'/athena-results/"},
    "EnforceWorkGroupConfiguration": true,
    "PublishCloudWatchMetricsEnabled": false
  }' \
  --region "${REGION}" 2>/dev/null || echo "  Workgroup already exists."

echo ""
echo "=== Done ==="
echo "Query in Athena console (region: ${REGION}, workgroup: ${DATASET}):"
echo ""
echo "  SELECT * FROM ${GLUE_DB}.${GLUE_TABLE}"
echo "  WHERE product_code = '010121';"
echo ""
