#!/usr/bin/env bash

# Define variables
ROLE="arn:aws:iam::246012762261:role/service-role/AmazonSageMakerServiceCatalogProductsCodeBuildRole"
S3_BUCKET="demo-poc-iata"
REGION="eu-central-1"
WORKFLOW_NAME="iata-workflow"

# Check for required tools
if ! command -v python &> /dev/null; then
    echo "Python is not installed. Exiting."
    exit 1
fi

# Run the Python pipeline script
echo "Running the pipeline..."
python pipeline.py --role "${ROLE}" --s3_bucket "${S3_BUCKET}" --region "${REGION}" --workflow_name "${WORKFLOW_NAME}"

# Check the exit status of the Python script
if [ $? -ne 0 ]; then
    echo "Pipeline script failed. Please check the logs."
    exit 1
else
    echo "Pipeline script executed successfully."
fi