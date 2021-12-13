from loguru import logger
from ..utils.load_env import LoadEnvironment


class LoadOperatorCsv:
    """This is class handle to load data to a CSV file
    """
    
    def __init__(self, load_env: LoadEnvironment):
        """Initialize the class
        """
        logger.info("Initializing LoadOperatorCsv")
        self.load_env = load_env

    def loading_data(self):
        """This function is used to load data to a CSV file
        """
        logger.info("Loading data to CSV file")
        return "Loaded data to CSV file"