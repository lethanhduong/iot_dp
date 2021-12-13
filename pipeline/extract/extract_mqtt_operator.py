import time
import random
import paho.mqtt.client as paho
from paho import mqtt
from loguru import logger
from ..utils.load_env import ExtractEnvironment
from ..utils.mqtt_callback import on_connect, on_message, on_subscribe


class ExtractMQTTOperator:
    def __init__(self, extract_env: ExtractEnvironment):
        """Initialize the extractor"""
        logger.info("Initializing MQTT extractor")
        self.extract_env = extract_env
        self.client_id = f"python-mqtt-{random.randint(0, 1000)}"
        self.mqtt_client = self.create_mqtt_client(
            self.client_id,
            self.extract_env.mqtt_host,
            self.extract_env.mqtt_port,
            self.extract_env.mqtt_username,
            self.extract_env.mqtt_password,
        )

    @staticmethod
    def create_mqtt_client(
        client_id: str,
        mqtt_host: str = None,
        mqtt_port: int = None,
        mqtt_user: str = None,
        mqtt_password: str = None,
    ):
        logger.info("Creating MQTT client")
        mqtt_client = paho.Client(
            client_id=client_id, userdata=None, protocol=paho.MQTTv5
        )
        # Set callback function
        mqtt_client.on_connect = on_connect
        mqtt_client.on_message = on_message
        mqtt_client.on_subscribe = on_subscribe
        # enable TLS for secure connection
        mqtt_client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
        # set username and password
        mqtt_client.username_pw_set(mqtt_user, mqtt_password)
        # connect to HiveMQ Cloud on port 8883 (default for MQTT)
        mqtt_client.connect(mqtt_host, mqtt_port, keepalive=60)

        return mqtt_client

    def extract_data(self):
        logger.info("Extracting MQTT message")
        data = self.mqtt_client.subscribe(self.extract_env.mqtt_topic, qos=0)
        logger.debug(f'Subscribed: {data}')
        return data
