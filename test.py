# from sagemaker.spark.processing import PySparkProcessor
# from sagemaker.processing import ProcessingInput, ProcessingOutput
# import sagemaker
# from datetime import datetime

# uuid = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

# s3_bucket = "demo-poc-iata"

# spark_processor = PySparkProcessor(
#     base_job_name="spark-preprocessor",
#     framework_version="3.5",
#     role="arn:aws:iam::246012762261:role/service-role/AmazonSageMakerServiceCatalogProductsCodeBuildRole",
#     instance_count=1,
#     instance_type="ml.m4.xlarge",
#     max_runtime_in_seconds=1200,
#     sagemaker_session=sagemaker.Session(),
#     image_uri= "906073651304.dkr.ecr.eu-central-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py39-v1.0"
# )

# configuration = [{
#   "Classification": "spark-defaults",
#   "Properties": {"spark.executor.memory": "2g", "spark.executor.cores": "1"},
# }]

# spark_processor.run(
#     submit_app="./code/convert_to_parquet.py",
#     configuration=configuration,
#     arguments=['--s3_bucket', s3_bucket, "--uuid", uuid],

#     spark_event_logs_s3_uri=f"s3://{s3_bucket}/spark_event_logs",
#     logs=True,
#     wait=False
# )


import sagemaker
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.processing import FrameworkProcessor
from sagemaker.sklearn.processing import SKLearnProcessor

s3_bucket = "demo-poc-iata"
workflow_name = "iata-workflow"

step_processor = SKLearnProcessor(framework_version='0.23-1',
                                  role="arn:aws:iam::246012762261:role/service-role/AmazonSageMakerServiceCatalogProductsCodeBuildRole",
                                  instance_type='ml.m4.xlarge',
                                  instance_count=1,
                                  sagemaker_session=sagemaker.Session())


step_processor.run(
        code="./code/unzip.py",
        # source_dir="./code",
        inputs=[
            ProcessingInput(
                input_name="fetched_data",
                # source=fetch_job.properties.ProcessingOutputConfig.Outputs["fetched_data"].S3Output.S3Uri,
                source="s3://demo-poc-iata/iata-workflow/ef0gsgns0xzp/fetch_data/output/fetched_data/dataset.zip",
                destination="/opt/ml/processing/input"
            )
        ],
        outputs=[
            ProcessingOutput(
                output_name="unzipped_data",
                source="/opt/ml/processing/input",
                destination=f"s3://{s3_bucket}/{workflow_name}/original"
            )
        ]
    )

