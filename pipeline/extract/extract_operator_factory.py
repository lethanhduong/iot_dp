from loguru import logger
from ..utils.load_env import ExtractEnvironment

class ExtractOperatorFactory:
    """
    This is class that provides a factory for extracting data from a given source.
    """

    def __init__(self, extract_env: ExtractEnvironment):
        self.extract_env = extract_env

    def get_extract_operator(self):
        """
        This method returns an extract operator based on the source type.
        :return:
        """
        if self.extract_env.source_type == "FILE":
            from .extract_file_operator import ExtractFileOperator
            return ExtractFileOperator(self.extract_env)
        # elif self.source_type == "database":
        #     from extract.extract_database_operator import ExtractDatabaseOperator
        #     return ExtractDatabaseOperator()
        elif self.extract_env.source_type == "MQTT":
            from .extract_mqtt_operator import ExtractMQTTOperator
            return ExtractMQTTOperator(self.extract_env)
        return None