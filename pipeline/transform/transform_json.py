from loguru import logger
from ..utils.load_env import TransformEnvironment


class TransformJson:
    """
    TransformJson class
    """

    def __init__(self, transform_env: TransformEnvironment):
        """
        Constructor
        """
        logger.info("Initializing TransformJson")
        self.transform_env = transform_env

    def transform_data(self, data):
        """
        Transform data
        """
        logger.info("Transform data with length %d" % len(data))
        
        logger.debug(f"Data output: {data}")
        return data


