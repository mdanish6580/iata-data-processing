import os
from pyspark.sql import SparkSession
import boto3  

def csv_to_parquet_partitioned(input_csv, output_dir, partition_column):
    """
    Convert a CSV file to Parquet format, partitioned by a specified column.
    
    Args:
        input_csv (str): Path to the input CSV file.
        output_dir (str): Path to the output directory for Parquet files.
        partition_column (str): Column name to partition the data by.
    """
    # Initialize SparkSession
    spark = SparkSession.builder.appName("CSVtoParquetPartition").getOrCreate()

    # Read the CSV file into a DataFrame
    print(f"Reading CSV file: {input_csv}")
    df = spark.read.csv(input_csv, header=True, inferSchema=True)
    print(df.columns)

    if partition_column not in df.columns:
        raise ValueError(f"Partition column '{partition_column}' does not exist in the DataFrame. Available columns: {df.columns}")

    # Write the DataFrame to Parquet with partitioning
    print(f"Writing Parquet files partitioned by '{partition_column}' to {output_dir}...")
    df.write.partitionBy(partition_column).parquet(output_dir, mode="overwrite")
    print(f"Parquet files saved to {output_dir}")

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CSVtoParquetPartition").getOrCreate()
    import argparse
    parser = argparse.ArgumentParser(description="Run Convert to Parquet.")
    parser.add_argument("--s3_bucket", required=True, help="S3 bucket name", default="demo-poc-iata")
    parser.add_argument("--uuid", required=True, help="UUID for the run")
    parser.add_argument("--workflow_name", required=True, help="Workflow name")

    args = parser.parse_args()
    s3_bucket = args.s3_bucket
    uuid = args.uuid
    workflow_name = args.workflow_name

    # Paths and parameters
    
    input_path = f"s3://{s3_bucket}/{workflow_name}/extracted_data/2m Sales Records.csv"
    output_dir = f"s3://{s3_bucket}/{workflow_name}/parquet/" + uuid + "/"

    partition_column = "Country"
    
    # Convert CSV to partitioned Parquet files
    csv_to_parquet_partitioned(input_path, output_dir, partition_column)
