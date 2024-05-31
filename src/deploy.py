import json
import pyodbc
import sys
from codegen import generate_sql, entity_order, generate_sql_drop_if_exists

def deploy():
    
    config_path = "../data/"
    try:
        with open(f"{config_path}config.json") as f:
            config = json.load(f)
    except OSError:
        print(f"ERROR: Could not open config.json from {config_path}. Please verify that the file exists and is accessible.", file=sys.stderr)
        return
    
    model_path = "../data/"
    try:
        with open(f"{model_path}{config['ModelName']}.json") as f:
            model = json.load(f)
            cnxn = pyodbc.connect(f"Driver={config['DBMS']};Server={config['Server']};Database={config['DB']};Trusted_Connection=yes;", autocommit=True)
            cursor = cnxn.cursor()
            if config['DropIfExists'] == "True":
                for entity_type in reversed(entity_order()):
                    for entity in (e for e in model if e['EntityType'] == entity_type):
                        cursor.execute(generate_sql_drop_if_exists(entity))
            for entity_type in entity_order():
                for entity in (e for e in model if e['EntityType'] == entity_type):
                    cursor.execute(generate_sql(entity))
            cursor.close()
            cnxn.close()
    except OSError:
        print(f"ERROR: Could not open model file {config['ModelName']}.json from {model_path}. Please verify that the file exists and is accessible.", file=sys.stderr)