import pandas as pd
from sqlalchemy import create_engine

class CsvToPostgresDb:
    
# Reading the list of given CSV Files
    def read_csv(self):
        dfs = []
        for path in self.csv_paths:
            DataFrame = pd.read_csv(path)
            dfs.append(DataFrame)
        return dfs

# Connecting to the Postgres database, reads the CSV files and writes them to the target database

    def write_postgresDb(self):
        print(self.user)
        print(self.password)
        print(self.host)
        print(self.port)    
        print(self.database)
        engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}')
        #engine = create_engine('postgresql://akshay:@localhost:5432/akshaydb')
        connection = engine.connect()
        for DataFrame in self.read_csv():
            table_name = DataFrame.columns[3]
            DataFrame.to_sql(table_name, con=connection, schema=self.schema, if_exists='replace', index=False)
        connection.close()

# Defining the CSV Path, Username, Password, DbName, Hostname, Port, Schema

    def __init__(self, csv_paths, user, password, host, port, database,  schema='public'):
        self.csv_paths = csv_paths
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.schema = schema

# Creating a list of input files

csv_file1 = '/Users/akshay/Downloads/Test/input_data/ecg1.csv' 
csv_file2 = '/Users/akshay/Downloads/Test/input_data/ecg2.csv'
csv_paths = [csv_file1, csv_file2]
database = 'akshaydb'
user = 'akshay'
password = 'password'
host = 'localhost'
port = 5432
schema = 'public'

# Calling the class which reads the csv files from the disk and writes into the target postgres database

Csv_Postgres = CsvToPostgresDb(csv_paths, user, password, host, port, database, schema)
Csv_Postgres.write_postgresDb()