from loguru import logger
from ..utils.load_env import LoadEnvironment

class LoadOperatorFactory:
    """
    LoadOperatorFactory class
    """
    def __init__(self, load_env: LoadEnvironment):
        """
        """
        self.load_env = load_env
    
    
    def get_load_operator(self):
        """
        Create a load operator object
        :param load_operator_type:
        :param kwargs:
        :return:
        """
        if self.load_env.target_type == 'CSV':
            from pipeline.load.load_csv import LoadOperatorCsv
            return LoadOperatorCsv(self.load_env)
        if self.load_env.target_type == 'POSTGRES':
            from pipeline.load.load_postgres import LoadOperatorPostgres
            return LoadOperatorPostgres(self.load_env)
    
