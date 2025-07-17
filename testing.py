from src.load import extract_features 
import json

data = extract_features('all.txt.gz', rows_count=5000)

print(data[4, 1])
    