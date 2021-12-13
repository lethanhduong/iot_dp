from loguru import logger
from ..utils.load_env import ExtractEnvironment


class ExtractFileOperator:
    """
    ExtractFileOperator class
    """

    def __init__(self, extract_env: ExtractEnvironment):
        """
        Constructor for ExtractFileOperator class
        :param file_path: path to file
        """
        logger.info(f"Initializing ExtractFileOperator")
        self.extract_env = extract_env

    def extract_data(self):
        """
        Extracts file
        :return: extracted file
        """
        logger.info(f"Extracting file {self.extract_env.file_path}")
        return self.extract_env.file_path