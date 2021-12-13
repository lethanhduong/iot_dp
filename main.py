import time
from loguru import logger
from pipeline.utils.load_env import ExtractEnvironment, TransformEnvironment, LoadEnvironment
from pipeline.extract.extract_operator_factory import ExtractOperatorFactory
from pipeline.transform.transform_operactor_factory import TransformationOperatorFactory
from pipeline.load.load_operator_factory import LoadOperatorFactory

# ELT Factory
def batch_processing(extract_env, transform_env, load_env):
    """Extract, transform and load data pipeline"""
    logger.info("= "*15 + "Starting batch processing")
    extract_op = ExtractOperatorFactory(extract_env).get_extract_operator()
    transform_op = TransformationOperatorFactory(transform_env).get_transform_operator()
    load_op = LoadOperatorFactory(load_env).get_load_operator()
    
    while True:
        try:
            logger.info("Starting batch processing")
            data = extract_op.extract_data()
            data_transformed = transform_op.transform_data(data)
            load_op.load_data(data_transformed)
            logger.info("Batch processing completed")
        except Exception as e:
            logger.error(e)
            logger.error("Batch processing failed")
        time.sleep(5)


def loading_env():
    """Loading environment variables from .env file"""
    extract_env = ExtractEnvironment()
    transform_env = TransformEnvironment()
    load_env = LoadEnvironment()
    extract_env.load_env()
    transform_env.load_env()
    load_env.load_env()
    return extract_env, transform_env, load_env


def extract_transform_load(extract_env, transform_env, load_env):
    """Extract, transform and load data pipeline"""
    batch_processing(extract_env, transform_env, load_env)


if __name__ == "__main__":
    extract_env, transform_env, load_env = loading_env()
    extract_transform_load(extract_env, transform_env, load_env)
    
    









