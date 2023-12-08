# importing libraries
from sqlalchemy import create_engine
import yaml
import pandas as pd


# loading yaml file
def extracting_credentials():
    with open('credentials.yaml', 'r') as file:
        return yaml.safe_load(file)
    
credentials_dict = extracting_credentials()


class RDSDatabaseConnector:

    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict

    def initialise_SQLAlchemy(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{self.credentials_dict['RDS_USER']}:{self.credentials_dict['RDS_PASSWORD']}@{self.credentials_dict['RDS_HOST']}:{self.credentials_dict['RDS_PORT']}/{self.credentials_dict['RDS_DATABASE']}")

# engine.connect()
    def get_loan_data_df(self):
        self.engine.connect()
        self.loan_payments_df = pd.read_sql_table('loan_payments', self.engine)
        return self.loan_payments_df
        
    def create_csv_file(loan_data_df):
        with open('csv_files/loan_payments.csv', 'w') as file:
            loan_data_df.to_csv(file, encoding= 'utf-8', index= False)

