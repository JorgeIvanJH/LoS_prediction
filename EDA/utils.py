SAVE_DIR = r"../data/gcc/"
SCHEMA_NAME = "ML_DataModel"
TABLE_NAMES = ["Appointment", "Episode","Observation", "Diagnosis", "Order", "Patient", "Procedures"]
TABLE_NAMES = [SCHEMA_NAME+"."+NAME for NAME in TABLE_NAMES]




import pyodbc
import pandas as pd
from dotenv import load_dotenv
import os
from pathlib import Path

SAVE_DIR = Path(SAVE_DIR)

load_dotenv()
SERVER = os.getenv("IRIS_SERVER")
DATABASE = os.getenv("IRIS_DATABASE")
USERNAME = os.getenv("IRIS_USERNAME")
PASSWORD = os.getenv("IRIS_PASSWORD")


connection_string = (
    'DRIVER={InterSystems IRIS ODBC35};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'UID={USERNAME};'
    f'PWD={PASSWORD}'
)

def save_IRIS_tables(connection_string, table_names: list):
    """
    Fetch specified tables and saves to a local file
    """
    cnn = pyodbc.connect(connection_string)
    for table_name in table_names:
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, cnn)
        df.to_pickle(SAVE_DIR+f"{table_name}.pkl")

    return "Ok"

def GetSavedTables(SAVE_DIR=SAVE_DIR):
    dfs = {}
    for file_path in SAVE_DIR.iterdir():
        if file_path.suffix == ".pkl":
            print(f"Reading {file_path}")
            dfs[file_path.stem] = pd.read_pickle(file_path)
    return dfs