#!/usr/bin/env bash
# Optional paid-cloud step: requires configured AWS CLI and permission to write to the bucket.
set -euo pipefail
: "${ECOMMERCE_S3_BUCKET:?Set ECOMMERCE_S3_BUCKET to an existing bucket name}"
aws sts get-caller-identity >/dev/null
aws s3 sync data/raw "s3://${ECOMMERCE_S3_BUCKET}/ecommerce/raw/" --exclude "*" --include "*.csv"
echo "Source CSVs uploaded. Configure a Unity Catalog external volume for s3://${ECOMMERCE_S3_BUCKET}/ecommerce/raw/ in Databricks."
