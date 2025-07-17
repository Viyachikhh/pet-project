import gzip
import json
import polars as pl
import psycopg2
import sys

def parse(filename):
    with gzip.open(filename, 'r') as f:
        entry = {}
        for string_entry in f:
            string_entry = str(string_entry.strip(), encoding='utf-8')
            kv_separator = string_entry.find(':')
            if kv_separator == -1:
                yield entry
                entry = {}
                continue
            feature_name = string_entry[:kv_separator]
            feature_value = string_entry[kv_separator+2:]
            entry[feature_name] = feature_value
        yield entry
        

def extract_features(filename, rows_count = 50000, features=['product/title','review/text']):
    entities = {feature : [] for feature in features}
    counter = 0
    for dict_entity in parse(filename):

        if len(dict_entity['review/text']) >= 700 or len(dict_entity['product/title']) > 70:
            continue

        for feature in features:
            entities[feature].append(dict_entity[feature])
        counter += 1
        if counter == rows_count:
            break

    return pl.from_dict(entities)


def load_to_database(conn_params, values):
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    cursor.executemany("INSERT INTO reviews (title, text) VALUES (%s, %s)", values)
    conn.commit()  
    conn.close()
    


        

