import yaml

from utils.connection_database import UseDatabase

YOUR_PATH = "credentials/postgresql.yaml"



def insert_values_many(values):
    sql_query = "INSERT INTO reviews (title, review, summary, score, helpfulness) VALUES (%s, %s, %s, %s, %s)"
    with UseDatabase(YOUR_PATH) as cursor:
        cursor.executemany(sql_query, values)

def insert_value(value):
    sql_query = "INSERT INTO reviews (title, review, summary, score, helpfulness) VALUES (%s, %s, %s, %s, %s)"
    with UseDatabase(YOUR_PATH) as cursor:
        cursor.execute(sql_query, value)

