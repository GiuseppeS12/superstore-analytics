import os
import pandas as pd
from glob import glob

DATA_PATH = "/your_directory/superstore-analytics/data"
REQUIRED_FILES = os.listdir(DATA_PATH)

def validate_data():
    for file in REQUIRED_FILES:
        file_path = os.path.join(DATA_PATH, file)
        if os.path.isdir(file_path):
            print(f"{file} is a directory")
            continue
        if os.path.getsize(file_path) == 0:
            raise ValueError(f"{file} is empty")
        df = pd.read_csv(file_path,encoding='latin-1')
        if df.shape[0] == 0:
            raise ValueError(f"{file} has no rows")
    print("Validation successful")
