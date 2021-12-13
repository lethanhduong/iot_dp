from loguru import logger
from ..utils.load_env import TransformEnvironment

class TransformDataFrame:
    """
    TransformDataFrame class
    """
    def __init__(self, transform_env: TransformEnvironment):
        """
        Initialize TransformDataFrame class
        """
        logger.info("Initialize TransformDataFrame class")

    def transform(self, dataframe):
        """
        Transform dataframe
        """
        logger.info("Transform dataframe")
        return dataframe