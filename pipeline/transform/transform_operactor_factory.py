from loguru import logger
from ..utils.load_env import TransformEnvironment
from .transform_json import TransformJson
from .transform_dataframe import TransformDataFrame


class TransformationOperatorFactory:
    def __init__(self, transform_env: TransformEnvironment):
        """ Initialize the transformation operator factory.
        """
        self.transform_env = transform_env
    
    def get_transform_operator(self):
        """ Get the transformation operator.
        """
        if self.transform_env.data_type == 'JSON':
            return TransformJson(self.transform_env)
        elif self.transform_env.data_type == 'DATAFRAME':
            return TransformDataFrame(self.transform_env)
        return None


