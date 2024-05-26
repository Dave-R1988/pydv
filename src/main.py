import json
import pyodbc
from codegen import generate_sql

def main():
    with open("C:/Users/Rahel Gasser/Desktop/D/Python/pydv/data/DataVault.json") as f:
        for entity in json.load(f):
            print(entity)
            print(generate_sql(entity))
            cnxn = pyodbc.connect(r'Driver=SQL Server;Server=RAHELDESKTOP\SQLEXPRESS;Database=DataVault;Trusted_Connection=yes;', autocommit=True)
            cursor = cnxn.cursor()
            cursor.execute(generate_sql(entity))
            cursor.close()
            cnxn.close()

if __name__ == "__main__":
    main()