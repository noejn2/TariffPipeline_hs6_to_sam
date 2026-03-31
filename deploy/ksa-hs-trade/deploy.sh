#!/usr/bin/env bash
# =============================================================================
# deploy.sh — Deploys the KSA HS trade data as a serverless SQL database.
#
# Architecture:
#   S3 (Parquet) → Glue Data Catalog → Athena (serverless SQL)
#
# What it does:
#   1. Creates S3 bucket with encryption
#   2. Runs Julia pipeline to produce Parquet
#   3. Uploads Parquet to S3
#   4. Creates Glue database + table (schema on the Parquet)
#   5. Creates Athena workgroup with results location
#
# After deploy, query the data in the Athena console:
#   SELECT * FROM ksa_hs_trade.saudi_reporter
#   WHERE year IN (2020, 2021) AND partner_name = 'China'
#
# Prerequisites:
#   - AWS CLI with s3, glue, athena permissions
#   - Julia 1.11+ with CSV, DataFrames, Parquet2
#
# Usage:
#   ./deploy/ksa-hs-trade/deploy.sh
# =============================================================================
set -euo pipefail

BUCKET="lbpo-blob-storage"
DATASET="ksa-hs-trade"
REGION="ap-south-1"
GLUE_DB="ksa_hs_trade"
GLUE_TABLE="saudi_reporter"
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
julia deploy/ksa-hs-trade/saudi_reporter.jl
echo "  Parquet written to data/saudi_reporter.parquet"

echo "  Uploading to S3..."
aws s3 cp data/saudi_reporter.parquet \
  "s3://${BUCKET}/${DATASET}/saudi_reporter.parquet" \
  --region "${REGION}"

# ---- Glue Data Catalog ----
echo "=== Glue Data Catalog ==="
aws glue create-database \
  --database-input '{"Name":"'"${GLUE_DB}"'","Description":"KSA HS6 trade data"}' \
  --region "${REGION}" 2>/dev/null || echo "  Database already exists."

# Delete existing table if present (schema changed: added indicator column)
aws glue delete-table \
  --database-name "${GLUE_DB}" \
  --name "${GLUE_TABLE}" \
  --region "${REGION}" 2>/dev/null || echo "  No existing table to delete."

aws glue create-table \
  --database-name "${GLUE_DB}" \
  --table-input '{
    "Name": "'"${GLUE_TABLE}"'",
    "Description": "Saudi Arabia KSA trade data (indicator, year, partner_name, product_code, value)",
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
        {"Name": "indicator",    "Type": "string",  "Comment": "Trade direction (Imports, Exports, Rexports)"},
        {"Name": "year",         "Type": "bigint",  "Comment": "Trade year"},
        {"Name": "partner_name", "Type": "string",  "Comment": "Trade partner country"},
        {"Name": "product_code", "Type": "string",  "Comment": "HS6 product code (6-char, zero-padded)"},
        {"Name": "value",        "Type": "double",  "Comment": "Aggregated trade value (SAR)"}
      ]
    }
  }' \
  --region "${REGION}"

# ---- Athena Workgroup ----
echo "=== Athena Workgroup ==="
aws athena create-work-group \
  --name "${DATASET}" \
  --description "Serverless SQL for KSA HS trade data" \
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
echo "  WHERE year IN (2020, 2021)"
echo "  AND partner_name = 'China';"
echo ""
