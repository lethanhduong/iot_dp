from os import environ
from dataclasses import dataclass
from loguru import logger
from dotenv import load_dotenv


load_dotenv()

logger.info("= "*15 + "Loading environment variables...")

@dataclass
class ExtractEnvironment:
    """Environment variables for Extract operator
    """
    source_type: str = environ.get("SOURCE_TYPE", 'CSV')
    
    def load_env(self):
        """Load environment variables
        """
        logger.info("Loading environment variables for Extract operator...")
        if self.source_type.upper() == 'CSV':
            logger.info("Loading CSV environment variables...")
            
        elif self.source_type == "MQTT":
            logger.info("Loading MQTT environment variables...")
            self.mqtt_username = environ.get("MQTT_USERNAME", 'user')
            self.mqtt_password = environ.get("MQTT_PASSWORD", '123')
            self.mqtt_host = environ.get("MQTT_HOST", '127.0.0.1')
            self.mqtt_port = int(environ.get("MQTT_PORT", '1883'))
            self.mqtt_topic = environ.get("MQTT_TOPIC", 'test')


@dataclass
class TransformEnvironment:
    """Environment variables for Transform operator
    """
    data_type: str = environ.get("DATA_TYPE", 'JSON')
    
    def load_env(self):
        """Load environment variables
        """
        logger.info("Loading environment variables for Transform operator...")
        pass

            
@dataclass
class LoadEnvironment:
    """Environment variables for Load operator
    """
    target_type: str = environ.get("TARGET_TYPE", 'CSV')
    
    def load_env(self):
        """Load environment variables
        """
        logger.info("Loading environment variables for Load operator...")
        if self.target_type.upper() == 'POSTGRES':
            logger.info("Loading POSTGRES environment variables...")
            self.db_user = environ.get("DB_USER", 'user')
            self.db_password = environ.get("DB_PASSWORD", '123')
            self.db_host = environ.get("DB_HOST", '127.0.0.1')
            self.db_port = int(environ.get("DB_PORT", '5432'))
            self.db_name = environ.get("DB_NAME", 'db')
            # self.LOADING-CONFIG = load_loading_config()
            self.table_name = environ.get("TABLE_NAME", 'table')
            self.schema_name = environ.get("SCHEMA_NAME", 'schema')
            self.columns = environ.get("COLUMNS", 'col1,col2').split(',')
        pass   

    




