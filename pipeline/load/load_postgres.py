import psycopg2
from loguru import logger
from ..utils.load_env import LoadEnvironment


class LoadOperatorPostgres:
    """This is class handle to load data to a table in postgres
    """

    def __init__(self, load_env: LoadEnvironment):
        """
        :param conn: connection to postgres
        :param table_name: name of table
        :param schema_name: name of schema
        :param columns: list of columns
        :param data: list of data
        """
        logger.info("Initializing LoadOperatorPostgres")
        self.load_env = load_env
        self.conn = self.create_connection(
            self.load_env.db_host,
            self.load_env.db_port,
            self.load_env.db_user,
            self.load_env.db_password,
            self.load_env.db_name,
        )

    @staticmethod
    def create_connection(host, port, user, password, database):
        """Create connection to postgres"""
        conn = None
        try:
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, database=database
            )
        except psycopg2.Error as e:
            logger.error(e)
            # raise e
        return conn

    def load_data(self, data):
        """
        :return:
        """
        try:
            logger.debug(
                f"INSERT INTO {self.load_env.schema_name}.{self.load_env.table_name} ({','.join(self.load_env.columns)}) VALUES ({','.join(['%s' for _ in range(len(self.load_env.columns))])})"
            )
            # cur = self.conn.cursor()
            # cur.execute(
            #     f"INSERT INTO {self.load_env.schema_name}.{self.load_env.table_name} ({','.join(self.load_env.columns)}) VALUES ({','.join(['%s' for _ in range(len(self.load_env.columns))])})",
            #     data,
            # )
            # self.conn.commit()
            logger.info(f"Loaded {len(data)} rows to {self.load_env.table_name}")
        except Exception as e:
            logger.error(f"Error loading data to {self.load_env.table_name}")
            logger.error(e)
            self.conn.rollback()
            raise e
