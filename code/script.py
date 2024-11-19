import argparse
import os
from pyspark.sql import SparkSession

def main(input_csv_path):
    # Initialize the Spark session
    spark = SparkSession.builder \
        .appName("TestReadCSV") \
        .getOrCreate()

    # Full S3 path for the file
    # s3_full_path = f"s3a://{s3_bucket}/{s3_file_path}"

    # Read the CSV file into a PySpark DataFrame
    print(f"Reading CSV file from S3: {input_csv_path}...")
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(input_csv_path)

    # Show the first 5 rows of the DataFrame
    print("Displaying the first 5 rows of the DataFrame:")
    df.show(5)

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test reading a CSV file from an S3 bucket using PySpark.")
    parser.add_argument("--s3_bucket", required=True, help="The name of the S3 bucket.")
    
    args = parser.parse_args()

    print("s3 bucket: ", args.s3_bucket)

    # Paths and parameters
    input_csv = "2m Sales Records.csv"
    output_dir = "/opt/ml/processing/input"
    # output_dir = "s3://demo-poc-iata/spark_event_logs/"

    # input_csv_path = os.path.join(output_dir, input_csv)
    input_csv_path = output_dir

    print("input_csv_path: ", input_csv_path)

    main(input_csv_path)
