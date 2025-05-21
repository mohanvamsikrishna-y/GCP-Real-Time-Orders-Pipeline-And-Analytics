#!/bin/bash

PROJECT_ID="fleet-parity-458321-e4"
REGION="us-east1"
BUCKET_NAME="dataflow-staging-us-east1-543216624555"

TEMPLATE_PATH="gs://${BUCKET_NAME}/templates/ecommerce-orders-template.json"
TEMP_DIR="gs://${BUCKET_NAME}/temp"

gcloud dataflow flex-template build "$TEMPLATE_PATH" \
  --image-gcr-path "gcr.io/$PROJECT_ID/ecommerce-orders-image" \
  --sdk-language "PYTHON" \
  --flex-template-base-image "PYTHON3" \
  --metadata-file "metadata.json" \
  --py-path "dataflow_job_script.py" \
  --env FLEX_TEMPLATE_PYTHON_PY_FILE=dataflow_job_script.py \
  --env FLEX_TEMPLATE_BUILD_MODE=V2 \
  --project "$PROJECT_ID" \
  --temp-location "$TEMP_DIR"
