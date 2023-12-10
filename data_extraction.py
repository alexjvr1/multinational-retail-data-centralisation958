#Python script to extract data from different data sources: 
#CSV, API, and S3 bucket

#import required modules
from database_utils.py import init_db_engine
from sqlalchemy import inspect
import pandas as pd
class DataExtractor:
        def __init__(self)
#function to list all the tables in the database
	def list_db_tables():
		engine = init_db_creds()
		#create an inspector
		inspector = inspect(engine)
		table_names = []
		#Add table names to a list and print
		table_names += inspector.get_table_names()
		return table_names
#Function to extract a db table to a pandas df
	def read_rds_table(DatabaseConnector, table_name):
		table_name = table_name
		table_df = pd.read_sql_table(table_name, engine)
		table_df.head()
		return table_df

