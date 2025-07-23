import gzip
import json
import polars as pl
import psycopg2
import sys
import tqdm

from math import ceil
from multiprocessing import Pool
from time import time

def parse_line(filename, start_row_number=None, rows_count=50000):
    counter = 0
    last_row_number = start_row_number if start_row_number is not None else 0

    with gzip.open(filename, 'r') as f:
        entry = {}
        for row_number, string_entry in enumerate(f):
            if row_number < last_row_number:
                continue

            string_entry = str(string_entry.strip(), encoding='utf-8')
            kv_separator = string_entry.find(':')

            if kv_separator == -1:

                counter += 1
                if counter == rows_count:
                    break

                yield row_number, entry

                entry = {}
                last_row_number += 1
                continue

            feature_name = string_entry[:kv_separator]
            feature_value = string_entry[kv_separator+2:]
            entry[feature_name] = feature_value
            last_row_number += 1

        yield last_row_number, entry

def parse_dicts(path):
    
    with gzip.open(path, 'r') as g:
        for i, l in enumerate(g):
            yield eval(l)
            if i == 10:
                break

def create_field(features, dict_entity):
        """
        create one row of dataset
        """
        field = {}
        for feature in features:

            feature_without_backslash = feature.split('/')[-1]
            value = dict_entity[feature]

            match feature_without_backslash:
                case 'helpfulness':
                    value = calculate_helpfullness(value)
                case 'price':
                    try:
                        value = float(value)
                    except:
                        value = None
                case 'score':
                    value = float(value)
                case _:
                    pass
            field[feature] = value
        return field
        

def extract_features(filename:str, features=None, rows_count=50000, start_row_number=None):
    """
    extracting infromation from archive
    filename: path to archive;
    constraints: dict with constraint for every feature
    """

    if isinstance(features, str):
        features = [features]
    elif features is None:
        features = next(parse_line(filename)).keys()


    full_res = []
    last_row_number = 0
    for row_number, dict_entity in parse_line(filename=filename, 
                                        rows_count=rows_count, 
                                        start_row_number=start_row_number):
        entity = create_field(features, dict_entity)
        full_res.append(entity)
        last_row_number = row_number
    
    df = pl.from_dicts(full_res)

    return last_row_number + 1, df.rename({feature: feature.split('/')[-1] for feature in features})

def calculate_helpfullness(string_repr):
    """
    Examples:
    0/0 -> 0
    6/6 -> 1
    3/4 -> 0.75
    6/4 -> 1
    """
    scores = list(map(int, string_repr.split('/')))
    return min(scores[0] / scores[1], 1) if scores[1] != 0 else 0

    


        

