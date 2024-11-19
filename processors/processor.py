import sagemaker
from sagemaker.processing import FrameworkProcessor
from sagemaker.spark.processing import PySparkProcessor

def processor(role, bucket, processing_instance_count, processing_instance_type, image_uri, sagemaker_session):
    script_processor = FrameworkProcessor(
        role=role,
        estimator_cls=sagemaker.sklearn.estimator.SKLearn,
        framework_version='0.20.0',
        image_uri=image_uri,
        instance_count=processing_instance_count,
        instance_type=processing_instance_type,
        code_location=f"s3://{bucket}",
        py_version="py3",
        sagemaker_session=sagemaker_session
    )
    
    return script_processor


def spark_processor(role, bucket, transform_job_instance_count, transform_job_instance_type, sagemaker_session):
    spark_processor = PySparkProcessor(
        base_job_name="spark_transform_job",
        framework_version="3.5",
        image_uri = "906073651304.dkr.ecr.eu-central-1.amazonaws.com/sagemaker-spark-processing:3.5-cpu-py39-v1.0",
        role=role,
        instance_count=transform_job_instance_count,
        instance_type=transform_job_instance_type,
        max_runtime_in_seconds=1200,
        sagemaker_session=sagemaker_session
    )

    return spark_processor