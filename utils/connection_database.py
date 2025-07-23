import psycopg2
import yaml

from psycopg2.extras import execute_values

class DatabaseContext:

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


class DatabaseProxy:

    def __init__(self, path_to_config):
        self.__context_object__ = DatabaseContext(path_to_config=path_to_config)

    def query_insert_many(self, columns, table, values):
        column_list_to_str = '(' + ', '.join(columns) + ')'
        with self.__context_object__ as cursor:
            query_insert = f'INSERT INTO {table} {column_list_to_str} VALUES %s'
            execute_values(cursor, query_insert, values) 

    def query_insert_one(self):
        pass

    def custom_query(self, query):
        pass