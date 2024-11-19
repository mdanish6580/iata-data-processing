import os
from sagemaker.workflow.pipeline import Pipeline
from sagemaker.workflow.parameters import ParameterInteger, ParameterString
from sagemaker.workflow.pipeline_context import PipelineSession
from sagemaker.workflow.steps import ProcessingStep
from sagemaker.processing import ProcessingInput, ProcessingOutput
from processors.processor import processor, spark_processor
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.realpath(__file__))

def get_pipeline(role, s3_bucket, region, workflow_name):
    """Gets a SageMaker ML Pipeline instance working with on a toy data.

    Args:
        region: AWS region to create and run the pipeline.
        role: IAM role to create and run steps and pipeline.
        s3_bucket: the bucket to use for storing the artifacts

    Returns:
        an instance of a pipeline
    """

    pipeline_session = PipelineSession(default_bucket=s3_bucket)
    uuid = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    
    # Parameters for processing
    processing_instance_count = ParameterInteger(name="ProcessingInstanceCount", default_value=1)
    processing_instance_type = ParameterString(name="ProcessingInstanceType", default_value="ml.m4.xlarge")
    image_uri = "246012762261.dkr.ecr.eu-central-1.amazonaws.com/python3108"
    
    # Parameters for Spark transform job
    transform_job_instance_count = ParameterInteger(name="TransformJobInstanceCount", default_value=1)
    transform_job_instance_type = ParameterString(name="TransformJobInstanceType", default_value="ml.c4.xlarge")

    ###################### Step Definitions #########################
    step_processor = processor(role,
                                s3_bucket, 
                                processing_instance_count, 
                                processing_instance_type, 
                                image_uri, 
                                pipeline_session)

    transform_job_spark_processor = spark_processor(role,
                                                    s3_bucket, 
                                                    transform_job_instance_count, 
                                                    transform_job_instance_type,
                                                    pipeline_session)

    fetch_job = ProcessingStep(
        name="fetch_data",
        step_args=step_processor.run(
            code="fetch.py",
            source_dir="./code",
            outputs=[
                ProcessingOutput(
                    output_name="fetched_data",
                    source="/opt/ml/processing/output",
                    destination=f"s3://{s3_bucket}/{workflow_name}/fetched_data"
                )
            ]
        )
        )

    unzip_job = ProcessingStep(
        name="unzip_data",
        step_args=step_processor.run(
            code="unzip.py",
            source_dir="./code",
            inputs=[
                ProcessingInput(
                    input_name="fetched_data",
                    source=fetch_job.properties.ProcessingOutputConfig.Outputs["fetched_data"].S3Output.S3Uri,
                    destination="/opt/ml/processing/input"
                )
            ],
            outputs=[
                ProcessingOutput(
                    output_name="unzipped_data",
                    source="/opt/ml/processing/output",
                    destination=f"s3://{s3_bucket}/{workflow_name}/extracted_data"
                )
            ]
        )
    )

    configuration = [{
        "Classification": "spark-defaults",
        "Properties": {"spark.executor.memory": "2g", "spark.executor.cores": "1"},
        }]

    transform_job = ProcessingStep(
        name="convert_to_parquet",
        step_args=transform_job_spark_processor.run(
            submit_app="./code/convert_to_parquet.py",
            arguments=['--s3_bucket', s3_bucket, '--uuid', uuid, '--workflow_name', workflow_name],
            configuration=configuration,
            spark_event_logs_s3_uri=f"s3://{s3_bucket}/{workflow_name}/spark_event_logs",
            logs=True
        )
    )

    ###################### Dependance Conditions #########################
    unzip_job.add_depends_on([fetch_job])
    transform_job.add_depends_on([unzip_job])

    ###################### Pipeline Definition #########################
    pipeline = Pipeline(
        name=workflow_name,
        parameters=[
            processing_instance_type,
            processing_instance_count,
            transform_job_instance_type,
            transform_job_instance_count
        ],
        steps=[fetch_job, unzip_job, transform_job],
        sagemaker_session=pipeline_session
    )

    return pipeline


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run SageMaker pipeline.")
    parser.add_argument("--role", required=True, help="IAM role ARN")
    parser.add_argument("--s3_bucket", required=True, help="S3 bucket name")
    parser.add_argument("--region", required=True, help="AWS region")
    parser.add_argument("--workflow_name", required=True, help="Workflow name")

    args = parser.parse_args()

    role = args.role
    s3_bucket = args.s3_bucket
    region = args.region
    workflow_name = args.workflow_name

    # Fetch and upsert pipeline
    pipeline = get_pipeline(role, s3_bucket, region, workflow_name)
    upsert_response = pipeline.upsert(role_arn=role)
    print(f"Pipeline upsert response: {upsert_response}")

    # Execute the pipeline
    execution = pipeline.start()
    print(f"\n###### Execution started with PipelineExecutionArn: {execution.arn}")
