import gzip
import json
import polars as pl

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
    for idx, dict_entity in enumerate(parse(filename)):
        for feature in features:
            entities[feature].append(dict_entity[feature])
        if idx == rows_count - 1:
            break
    return pl.from_dict(entities)



        

