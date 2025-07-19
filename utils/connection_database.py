import psycopg2
import yaml

class UseDatabase:

    def __init__(self, path_to_config):

        with open(path_to_config, 'r') as stream:
            self.config_params = yaml.safe_load(stream)

    def __enter__(self) -> psycopg2.extensions.cursor:
        self.conn = psycopg2.connect(**self.config_params)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, exc_trace) -> None:
        self.conn.commit()
        self.cursor.close()  
        self.conn.close()